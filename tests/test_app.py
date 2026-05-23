"""Tests for the sandbox orchestration service.

Covers:
- Successful job submission spins up a container named after the jobId.
- Multiple jobs produce containers in submission order.
- Producer-level rejections (missing/empty fields, unknown type) return 422.
- Worker survives builder exceptions and keeps draining the queue.
- The log file contains the expected event sequence per job.
"""
import time

import pytest

from tests.conftest import read_log_events


def wait_for_event(event: str, jobId: str | None = None, timeout: float = 3.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        for record in read_log_events():
            if record.get("event") == event and (
                jobId is None or record.get("jobId") == jobId
            ):
                return record
        time.sleep(0.05)
    raise TimeoutError(f"timeout waiting for event={event} jobId={jobId}")


class TestJobCreation:
    def test_submit_job_creates_container_named_after_jobId(self, client, mock_docker):
        r = client.post("/jobs", json={"jobId": "abc123", "type": "http"})
        assert r.status_code == 202
        assert r.json() == {"jobId": "abc123", "queued": True, "queueDepth": 1}

        ready = wait_for_event("sandbox_ready", jobId="abc123")
        assert ready["containerId"] == "cid-abc123"
        assert ready["url"].startswith("http://localhost:")

        assert mock_docker.containers.run.call_count == 1
        call = mock_docker.containers.run.call_args
        assert call.args == ("nginx:alpine",)
        assert call.kwargs["name"] == "abc123"
        assert call.kwargs["labels"]["sandbox.jobId"] == "abc123"

    def test_containers_created_in_submission_order(self, client, mock_docker):
        job_ids = ["job-a", "job-b", "job-c", "job-d"]
        for jid in job_ids:
            assert (
                client.post("/jobs", json={"jobId": jid, "type": "http"}).status_code
                == 202
            )

        for jid in job_ids:
            wait_for_event("sandbox_ready", jobId=jid)

        names = [c.kwargs["name"] for c in mock_docker.containers.run.call_args_list]
        assert names == job_ids

        ready_order = [
            e["jobId"]
            for e in read_log_events()
            if e.get("event") == "sandbox_ready" and e.get("jobId") in job_ids
        ]
        assert ready_order == job_ids


class TestRejections:
    def test_missing_jobId_returns_422(self, client, mock_docker):
        r = client.post("/jobs", json={"type": "http"})
        assert r.status_code == 422
        assert mock_docker.containers.run.call_count == 0
        # Producer-level rejection: no per-job events should be logged.
        assert [e for e in read_log_events() if "jobId" in e] == []

    def test_missing_type_returns_422(self, client, mock_docker):
        r = client.post("/jobs", json={"jobId": "x1"})
        assert r.status_code == 422
        assert mock_docker.containers.run.call_count == 0

    def test_empty_jobId_returns_422(self, client, mock_docker):
        r = client.post("/jobs", json={"jobId": "", "type": "http"})
        assert r.status_code == 422
        assert mock_docker.containers.run.call_count == 0

    def test_empty_type_returns_422(self, client, mock_docker):
        r = client.post("/jobs", json={"jobId": "x1", "type": ""})
        assert r.status_code == 422
        assert mock_docker.containers.run.call_count == 0

    def test_unknown_type_returns_422_at_producer(self, client, mock_docker):
        r = client.post("/jobs", json={"jobId": "ghost", "type": "spaceship"})
        assert r.status_code == 422
        # Dynamic validator names the rejected type in the error.
        assert "spaceship" in r.text
        assert mock_docker.containers.run.call_count == 0
        assert [e for e in read_log_events() if "jobId" in e] == []


class TestWorkerResilience:
    def test_worker_survives_builder_exception(self, client, mock_docker):
        # Force the first containers.run call to raise; subsequent calls succeed.
        real_run = mock_docker.containers.run.side_effect
        call_count = {"n": 0}

        def maybe_fail(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("simulated docker failure")
            return real_run(*args, **kwargs)

        mock_docker.containers.run.side_effect = maybe_fail

        client.post("/jobs", json={"jobId": "fails", "type": "http"})
        client.post("/jobs", json={"jobId": "recovers", "type": "http"})

        failed = wait_for_event("sandbox_failed", jobId="fails")
        assert "simulated docker failure" in failed["error"]

        ready = wait_for_event("sandbox_ready", jobId="recovers")
        assert ready["containerId"] == "cid-recovers"


class TestLogFile:
    def test_event_sequence_for_successful_job(self, client, mock_docker):
        client.post("/jobs", json={"jobId": "seq", "type": "http"})
        wait_for_event("sandbox_ready", jobId="seq")

        events = [e["event"] for e in read_log_events() if e.get("jobId") == "seq"]
        assert events == ["job_enqueued", "sandbox_starting", "sandbox_ready"]

    def test_every_event_has_timestamp(self, client, mock_docker):
        client.post("/jobs", json={"jobId": "ts", "type": "http"})
        wait_for_event("sandbox_ready", jobId="ts")

        for record in read_log_events():
            assert "ts" in record
            assert "event" in record

    def test_sandbox_ready_carries_url_and_containerId(self, client, mock_docker):
        client.post("/jobs", json={"jobId": "ready", "type": "http"})
        ready = wait_for_event("sandbox_ready", jobId="ready")

        assert ready["jobId"] == "ready"
        assert ready["type"] == "http"
        assert ready["containerId"] == "cid-ready"
        assert ready["url"].startswith("http://localhost:")


class TestHealthz:
    def test_healthz_reports_registered_sandbox_types(self, client):
        r = client.get("/healthz")
        assert r.status_code == 200
        body = r.json()
        assert "http" in body["sandboxTypes"]
        assert body["queueDepth"] == 0
