import asyncio
import json
import logging
import os
from abc import ABC, abstractmethod
from collections import Counter, deque
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

import docker
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, field_validator

LOG_PATH = os.environ.get("SANDBOX_LOG_PATH", "sandbox.log")
REAPER_INTERVAL_SECONDS = int(os.environ.get("REAPER_INTERVAL_SECONDS", "5"))
DEFAULT_TTL_SECONDS = int(os.environ.get("DEFAULT_TTL_SECONDS", "15"))
IDLE_TIMEOUT_SECONDS = int(os.environ.get("IDLE_TIMEOUT_SECONDS", "15"))

_log = logging.getLogger("sandbox")
_log.setLevel(logging.INFO)
_handler = logging.FileHandler(LOG_PATH)
_handler.setFormatter(logging.Formatter("%(message)s"))
_log.addHandler(_handler)


def emit(event: str, **fields) -> None:
    record = {"ts": datetime.now(timezone.utc).isoformat(), "event": event, **fields}
    _log.info(json.dumps(record))


@dataclass
class ContainerInfo:
    container_id: str
    url: str


class SandboxBuilder(ABC):
    """Each subclass encapsulates the image, port mapping, and any per-type
    setup needed to bring a sandbox online. build() runs in a worker thread."""

    image: str

    @abstractmethod
    def build(self, job: "Job", client: docker.DockerClient) -> ContainerInfo: ...


class HttpSandbox(SandboxBuilder):
    image = "nginx:alpine"
    container_port = "80/tcp"

    def build(self, job: "Job", client: docker.DockerClient) -> ContainerInfo:
        container = client.containers.run(
            self.image,
            detach=True,
            name=job.jobId,
            ports={self.container_port: None},
            labels={"sandbox.jobId": job.jobId, "sandbox.type": job.type},
        )
        container.reload()
        host_port = container.attrs["NetworkSettings"]["Ports"][self.container_port][0]["HostPort"]
        return ContainerInfo(
            container_id=container.id,
            url=f"http://localhost:{host_port}",
        )


class BrowserSandbox(SandboxBuilder):
    image = "browserless/chrome:latest"
    container_port = "3000/tcp"  # CDP-over-WebSocket endpoint

    def build(self, job: "Job", client: docker.DockerClient) -> ContainerInfo:
        container = client.containers.run(
            self.image,
            detach=True,
            name=job.jobId,
            ports={self.container_port: None},
            labels={"sandbox.jobId": job.jobId, "sandbox.type": job.type},
        )
        container.reload()
        host_port = container.attrs["NetworkSettings"]["Ports"][self.container_port][0]["HostPort"]
        return ContainerInfo(
            container_id=container.id,
            url=f"ws://localhost:{host_port}",
        )


BUILDERS: dict[str, SandboxBuilder] = {
    "http": HttpSandbox(),
    "browser": BrowserSandbox(),
}


class Job(BaseModel):
    jobId: str = Field(..., min_length=1)
    type: str = Field(..., min_length=1)
    ttlSeconds: int = Field(
        default_factory=lambda: DEFAULT_TTL_SECONDS, ge=1, le=86400
    )

    @field_validator("type")
    @classmethod
    def known_type(cls, v: str) -> str:
        if v not in BUILDERS:
            raise ValueError(
                f"unknown sandbox type {v!r}; known: {sorted(BUILDERS)}"
            )
        return v


STATUS_QUEUED = "queued"
STATUS_STARTING = "starting"
STATUS_READY = "ready"
STATUS_FAILED = "failed"
STATUS_TERMINATED = "terminated"
ALL_STATUSES = (
    STATUS_QUEUED,
    STATUS_STARTING,
    STATUS_READY,
    STATUS_FAILED,
    STATUS_TERMINATED,
)
# Terminal statuses can be overwritten by a new submission with the same jobId.
REUSABLE_STATUSES = {STATUS_FAILED, STATUS_TERMINATED}


@dataclass
class SandboxRecord:
    jobId: str
    type: str
    status: str
    enqueuedAt: str
    ttlSeconds: int
    containerId: str | None = None
    url: str | None = None
    readyAt: str | None = None
    error: str | None = None
    lastActiveAt: str | None = None
    lastNetworkBytes: int = 0
    terminatedAt: str | None = None
    terminationReason: str | None = None


job_queue: deque[Job] = deque()
sandboxes: dict[str, SandboxRecord] = {}
docker_client: docker.DockerClient | None = None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def worker_loop() -> None:
    emit("worker_started")
    while True:
        if not job_queue:
            await asyncio.sleep(0.1)
            continue
        job = job_queue.popleft()
        record = sandboxes[job.jobId]
        # The DELETE endpoint may have terminated this jobId while it was
        # still queued; in that case the worker should skip it.
        if record.status == STATUS_TERMINATED:
            continue
        record.status = STATUS_STARTING
        emit("sandbox_starting", jobId=job.jobId, type=job.type)
        builder = BUILDERS[job.type]
        try:
            info = await asyncio.to_thread(builder.build, job, docker_client)
            now = _now()
            record.status = STATUS_READY
            record.containerId = info.container_id
            record.url = info.url
            record.readyAt = now
            record.lastActiveAt = now
            record.lastNetworkBytes = 0
            emit(
                "sandbox_ready",
                jobId=job.jobId,
                type=job.type,
                containerId=info.container_id,
                url=info.url,
            )
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            record.status = STATUS_FAILED
            record.error = err
            emit(
                "sandbox_failed",
                jobId=job.jobId,
                type=job.type,
                error=err,
            )


def _sum_network_bytes(stats: dict) -> int:
    networks = stats.get("networks") or {}
    return sum(
        (iface.get("rx_bytes") or 0) + (iface.get("tx_bytes") or 0)
        for iface in networks.values()
    )


def _terminate_container(record: SandboxRecord, reason: str) -> None:
    """Stop and remove the container, mark the record, log the event."""
    if record.containerId is not None:
        try:
            container = docker_client.containers.get(record.containerId)
            container.remove(force=True)
        except docker.errors.NotFound:
            pass
    record.status = STATUS_TERMINATED
    record.terminatedAt = _now()
    record.terminationReason = reason
    emit(
        "sandbox_terminated",
        jobId=record.jobId,
        type=record.type,
        containerId=record.containerId,
        reason=reason,
    )


def _run_reaper_pass(now: datetime | None = None) -> None:
    """Single sweep: terminate ready sandboxes that exceed TTL or idle window.

    Exposed for direct invocation by tests so they don't have to wait on the
    REAPER_INTERVAL_SECONDS sleep in reaper_loop.
    """
    now = now or datetime.now(timezone.utc)
    for record in list(sandboxes.values()):
        if record.status != STATUS_READY:
            continue

        ready_at = datetime.fromisoformat(record.readyAt)
        if (now - ready_at).total_seconds() >= record.ttlSeconds:
            _terminate_container(record, "ttl_expired")
            continue

        try:
            container = docker_client.containers.get(record.containerId)
            stats = container.stats(stream=False)
        except docker.errors.NotFound:
            # Container disappeared out from under us; record it.
            record.status = STATUS_TERMINATED
            record.terminatedAt = now.isoformat()
            record.terminationReason = "container_missing"
            emit(
                "sandbox_terminated",
                jobId=record.jobId,
                type=record.type,
                containerId=record.containerId,
                reason="container_missing",
            )
            continue

        current_bytes = _sum_network_bytes(stats)
        if current_bytes > record.lastNetworkBytes:
            record.lastNetworkBytes = current_bytes
            record.lastActiveAt = now.isoformat()
            continue

        last_active = datetime.fromisoformat(record.lastActiveAt)
        if (now - last_active).total_seconds() >= IDLE_TIMEOUT_SECONDS:
            _terminate_container(record, "idle_timeout")


async def reaper_loop() -> None:
    emit("reaper_started")
    while True:
        await asyncio.sleep(REAPER_INTERVAL_SECONDS)
        try:
            await asyncio.to_thread(_run_reaper_pass)
        except Exception as e:
            emit("reaper_error", error=f"{type(e).__name__}: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global docker_client
    docker_client = docker.from_env()
    docker_client.ping()
    tasks = [
        asyncio.create_task(worker_loop()),
        asyncio.create_task(reaper_loop()),
    ]
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass
        docker_client.close()


app = FastAPI(lifespan=lifespan)


@app.post("/jobs", status_code=202)
async def submit_job(job: Job) -> dict:
    existing = sandboxes.get(job.jobId)
    if existing is not None and existing.status not in REUSABLE_STATUSES:
        raise HTTPException(
            status_code=409,
            detail=f"jobId {job.jobId!r} already exists with status {existing.status!r}",
        )
    sandboxes[job.jobId] = SandboxRecord(
        jobId=job.jobId,
        type=job.type,
        status=STATUS_QUEUED,
        enqueuedAt=_now(),
        ttlSeconds=job.ttlSeconds,
    )
    job_queue.append(job)
    emit("job_enqueued", jobId=job.jobId, type=job.type)
    return {"jobId": job.jobId, "queued": True, "queueDepth": len(job_queue)}


@app.delete("/jobs/{jobId}")
async def kill_job(jobId: str) -> dict:
    record = sandboxes.get(jobId)
    if record is None:
        raise HTTPException(status_code=404, detail=f"jobId {jobId!r} not found")
    if record.status == STATUS_QUEUED:
        # Drop it from the queue before the worker can pick it up.
        for j in list(job_queue):
            if j.jobId == jobId:
                job_queue.remove(j)
                break
        record.status = STATUS_TERMINATED
        record.terminatedAt = _now()
        record.terminationReason = "manual"
        emit(
            "sandbox_terminated",
            jobId=jobId,
            type=record.type,
            containerId=None,
            reason="manual",
        )
    elif record.status == STATUS_READY:
        await asyncio.to_thread(_terminate_container, record, "manual")
    else:
        raise HTTPException(
            status_code=409,
            detail=f"jobId {jobId!r} cannot be killed in status {record.status!r}",
        )
    return {
        "jobId": jobId,
        "status": record.status,
        "reason": record.terminationReason,
    }


@app.get("/sandboxes")
async def list_sandboxes() -> dict:
    records = list(sandboxes.values())
    status_counts = Counter(r.status for r in records)
    return {
        "counts": {
            "running": dict(
                Counter(r.type for r in records if r.status == STATUS_READY)
            ),
            "byStatus": {s: status_counts.get(s, 0) for s in ALL_STATUSES},
            "total": len(records),
        },
        "sandboxes": [asdict(r) for r in records],
    }


@app.get("/healthz")
async def healthz() -> dict:
    return {"queueDepth": len(job_queue), "sandboxTypes": sorted(BUILDERS.keys())}
