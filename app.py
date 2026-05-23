import asyncio
import json
import logging
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Awaitable, Callable

import docker
from fastapi import FastAPI
from pydantic import BaseModel, Field

LOG_PATH = "sandbox.log"

_log = logging.getLogger("sandbox")
_log.setLevel(logging.INFO)
_handler = logging.FileHandler(LOG_PATH)
_handler.setFormatter(logging.Formatter("%(message)s"))
_log.addHandler(_handler)


def emit(event: str, **fields) -> None:
    record = {"ts": datetime.now(timezone.utc).isoformat(), "event": event, **fields}
    _log.info(json.dumps(record))


class Job(BaseModel):
    jobId: str = Field(..., min_length=1)
    type: str = Field(..., min_length=1)


job_queue: deque[Job] = deque()
docker_client: docker.DockerClient | None = None


async def spawn_http(job: Job) -> None:
    def _run():
        container = docker_client.containers.run(
            "nginx:alpine",
            detach=True,
            ports={"80/tcp": None},
            labels={"sandbox.jobId": job.jobId, "sandbox.type": job.type},
        )
        container.reload()
        host_port = container.attrs["NetworkSettings"]["Ports"]["80/tcp"][0]["HostPort"]
        return container.id, host_port

    container_id, host_port = await asyncio.to_thread(_run)
    url = f"http://localhost:{host_port}"
    emit(
        "sandbox_ready",
        jobId=job.jobId,
        type=job.type,
        containerId=container_id,
        url=url,
    )


HANDLERS: dict[str, Callable[[Job], Awaitable[None]]] = {
    "http": spawn_http,
}


async def worker_loop() -> None:
    emit("worker_started")
    while True:
        if not job_queue:
            await asyncio.sleep(0.1)
            continue
        job = job_queue.popleft()
        emit("sandbox_starting", jobId=job.jobId, type=job.type)
        handler = HANDLERS.get(job.type)
        if handler is None:
            emit(
                "sandbox_failed",
                jobId=job.jobId,
                type=job.type,
                error=f"unknown job type: {job.type}",
            )
            continue
        try:
            await handler(job)
        except Exception as e:
            emit(
                "sandbox_failed",
                jobId=job.jobId,
                type=job.type,
                error=f"{type(e).__name__}: {e}",
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
    job_queue.append(job)
    emit("job_enqueued", jobId=job.jobId, type=job.type)
    return {"jobId": job.jobId, "queued": True, "queueDepth": len(job_queue)}


@app.get("/healthz")
async def healthz() -> dict:
    return {"queueDepth": len(job_queue), "handlers": list(HANDLERS.keys())}
