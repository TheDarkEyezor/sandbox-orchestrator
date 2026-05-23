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
ALL_STATUSES = (STATUS_QUEUED, STATUS_STARTING, STATUS_READY, STATUS_FAILED)


@dataclass
class SandboxRecord:
    jobId: str
    type: str
    status: str
    enqueuedAt: str
    containerId: str | None = None
    url: str | None = None
    readyAt: str | None = None
    error: str | None = None


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
        record.status = STATUS_STARTING
        emit("sandbox_starting", jobId=job.jobId, type=job.type)
        builder = BUILDERS[job.type]
        try:
            info = await asyncio.to_thread(builder.build, job, docker_client)
            record.status = STATUS_READY
            record.containerId = info.container_id
            record.url = info.url
            record.readyAt = _now()
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    global docker_client
    docker_client = docker.from_env()
    docker_client.ping()
    task = asyncio.create_task(worker_loop())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        docker_client.close()


app = FastAPI(lifespan=lifespan)


@app.post("/jobs", status_code=202)
async def submit_job(job: Job) -> dict:
    existing = sandboxes.get(job.jobId)
    if existing is not None:
        raise HTTPException(
            status_code=409,
            detail=f"jobId {job.jobId!r} already exists with status {existing.status!r}",
        )
    sandboxes[job.jobId] = SandboxRecord(
        jobId=job.jobId,
        type=job.type,
        status=STATUS_QUEUED,
        enqueuedAt=_now(),
    )
    job_queue.append(job)
    emit("job_enqueued", jobId=job.jobId, type=job.type)
    return {"jobId": job.jobId, "queued": True, "queueDepth": len(job_queue)}


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
