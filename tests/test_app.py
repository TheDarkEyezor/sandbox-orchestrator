"""Tests for the sandbox orchestration service.

Covers:
- Successful job submission spins up a container named after the jobId.
- Multiple jobs produce containers in submission order.
- Producer-level rejections (missing/empty fields, unknown type) return 422.
- Worker survives builder exceptions and keeps draining the queue.
- The log file contains the expected event sequence per job.
- Lifecycle: TTL expiry, idle-via-network-stats, manual DELETE, jobId reuse.
"""
import time
from datetime import datetime, timedelta, timezone

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


class TestBrowserSandbox:
    def test_browser_job_uses_browserless_image_and_ws_url(self, client, mock_docker):
        r = client.post("/jobs", json={"jobId": "brws", "type": "browser"})
        assert r.status_code == 202

        ready = wait_for_event("sandbox_ready", jobId="brws")
        assert ready["type"] == "browser"
        assert ready["url"].startswith("ws://localhost:")

        call = mock_docker.containers.run.call_args
        assert call.args[0].startswith("browserless/chrome")
        assert "3000/tcp" in call.kwargs["ports"]
        assert call.kwargs["name"] == "brws"
        assert call.kwargs["labels"]["sandbox.type"] == "browser"

    def test_browser_and_http_can_coexist(self, client, mock_docker):
        client.post("/jobs", json={"jobId": "h1", "type": "http"})
        client.post("/jobs", json={"jobId": "b1", "type": "browser"})

        wait_for_event("sandbox_ready", jobId="h1")
        wait_for_event("sandbox_ready", jobId="b1")

        by_name = {
            c.kwargs["name"]: c.args[0]
            for c in mock_docker.containers.run.call_args_list
        }
        assert by_name["h1"] == "nginx:alpine"
        assert by_name["b1"].startswith("browserless/chrome")


class TestDuplicateJobId:
    def test_duplicate_jobId_returns_409(self, client, mock_docker):
        r1 = client.post("/jobs", json={"jobId": "dup", "type": "http"})
        assert r1.status_code == 202
        wait_for_event("sandbox_ready", jobId="dup")

        r2 = client.post("/jobs", json={"jobId": "dup", "type": "http"})
        assert r2.status_code == 409
        assert "dup" in r2.text

    def test_duplicate_jobId_does_not_create_second_container(
        self, client, mock_docker
    ):
        client.post("/jobs", json={"jobId": "once", "type": "http"})
        wait_for_event("sandbox_ready", jobId="once")
        first_count = mock_docker.containers.run.call_count

        client.post("/jobs", json={"jobId": "once", "type": "http"})
        # Give the worker time to (incorrectly) act if dedup were broken.
        time.sleep(0.3)

        assert mock_docker.containers.run.call_count == first_count

    def test_duplicate_jobId_caught_before_record_overwrite(
        self, client, mock_docker
    ):
        # First request succeeds and writes containerId/url to the record.
        client.post("/jobs", json={"jobId": "keep", "type": "http"})
        wait_for_event("sandbox_ready", jobId="keep")

        first = next(
            r for r in client.get("/sandboxes").json()["sandboxes"]
            if r["jobId"] == "keep"
        )
        assert first["containerId"] == "cid-keep"

        # Duplicate is rejected; the original record is untouched.
        client.post("/jobs", json={"jobId": "keep", "type": "http"})
        after = next(
            r for r in client.get("/sandboxes").json()["sandboxes"]
            if r["jobId"] == "keep"
        )
        assert after == first


class TestSandboxView:
    def test_get_sandboxes_empty_initially(self, client):
        r = client.get("/sandboxes")
        assert r.status_code == 200
        body = r.json()
        assert body["counts"]["total"] == 0
        assert body["counts"]["running"] == {}
        assert body["sandboxes"] == []
        # All status buckets present even when zero, so callers don't have to defend.
        assert set(body["counts"]["byStatus"]) == {
            "queued",
            "starting",
            "ready",
            "failed",
            "terminated",
        }

    def test_get_sandboxes_lists_running_with_per_type_counts(
        self, client, mock_docker
    ):
        for jid, jt in [("h1", "http"), ("h2", "http"), ("b1", "browser")]:
            client.post("/jobs", json={"jobId": jid, "type": jt})
        for jid in ("h1", "h2", "b1"):
            wait_for_event("sandbox_ready", jobId=jid)

        body = client.get("/sandboxes").json()
        assert body["counts"]["total"] == 3
        assert body["counts"]["running"] == {"http": 2, "browser": 1}
        assert body["counts"]["byStatus"]["ready"] == 3

        records = {r["jobId"]: r for r in body["sandboxes"]}
        assert records["h1"]["status"] == "ready"
        assert records["h1"]["containerId"] == "cid-h1"
        assert records["h1"]["url"].startswith("http://")
        assert records["b1"]["url"].startswith("ws://")
        # Lifecycle timestamps populated on success.
        assert records["h1"]["enqueuedAt"] is not None
        assert records["h1"]["readyAt"] is not None

    def test_failed_sandbox_appears_with_failed_status_not_running(
        self, client, mock_docker
    ):
        mock_docker.containers.run.side_effect = RuntimeError("boom")
        client.post("/jobs", json={"jobId": "doom", "type": "http"})
        wait_for_event("sandbox_failed", jobId="doom")

        body = client.get("/sandboxes").json()
        doom = next(r for r in body["sandboxes"] if r["jobId"] == "doom")
        assert doom["status"] == "failed"
        assert "boom" in doom["error"]
        assert body["counts"]["byStatus"]["failed"] == 1
        # 'failed' must not count as running.
        assert body["counts"]["running"].get("http", 0) == 0


class TestLifecycleTTL:
    def test_ttl_expired_terminates_sandbox(self, client, mock_docker):
        client.post("/jobs", json={"jobId": "ephemeral", "type": "http", "ttlSeconds": 10})
        ready = wait_for_event("sandbox_ready", jobId="ephemeral")

        import app

        ready_at = datetime.fromisoformat(ready["ts"])
        app._run_reaper_pass(now=ready_at + timedelta(seconds=11))

        term = wait_for_event("sandbox_terminated", jobId="ephemeral")
        assert term["reason"] == "ttl_expired"
        rec = app.sandboxes["ephemeral"]
        assert rec.status == "terminated"
        assert rec.terminationReason == "ttl_expired"

    def test_ttl_not_yet_expired_leaves_sandbox_running(self, client, mock_docker):
        client.post("/jobs", json={"jobId": "young", "type": "http", "ttlSeconds": 60})
        ready = wait_for_event("sandbox_ready", jobId="young")

        import app

        ready_at = datetime.fromisoformat(ready["ts"])
        app._run_reaper_pass(now=ready_at + timedelta(seconds=5))

        assert app.sandboxes["young"].status == "ready"

    def test_default_ttl_used_when_not_specified(self, client, mock_docker):
        client.post("/jobs", json={"jobId": "default", "type": "http"})
        wait_for_event("sandbox_ready", jobId="default")

        import app

        assert app.sandboxes["default"].ttlSeconds == app.DEFAULT_TTL_SECONDS


class TestLifecycleIdle:
    def test_idle_sandbox_terminated_when_no_network_traffic(
        self, client, mock_docker
    ):
        client.post("/jobs", json={"jobId": "lonely", "type": "http", "ttlSeconds": 3600})
        wait_for_event("sandbox_ready", jobId="lonely")

        import app

        # stats() defaults to 0 bytes; jump past idle threshold.
        future = datetime.now(timezone.utc) + timedelta(
            seconds=app.IDLE_TIMEOUT_SECONDS + 1
        )
        app._run_reaper_pass(now=future)

        term = wait_for_event("sandbox_terminated", jobId="lonely")
        assert term["reason"] == "idle_timeout"

    def test_network_traffic_resets_idle_timer(self, client, mock_docker):
        client.post("/jobs", json={"jobId": "busy", "type": "http", "ttlSeconds": 3600})
        wait_for_event("sandbox_ready", jobId="busy")

        import app

        # Simulate traffic: bump stats before the reaper pass.
        container = mock_docker.containers.get("cid-busy")
        container.stats.return_value = {
            "networks": {"eth0": {"rx_bytes": 5000, "tx_bytes": 5000}}
        }
        # Even past the idle window, observing growth should *not* terminate.
        future = datetime.now(timezone.utc) + timedelta(
            seconds=app.IDLE_TIMEOUT_SECONDS + 1
        )
        app._run_reaper_pass(now=future)

        assert app.sandboxes["busy"].status == "ready"
        assert app.sandboxes["busy"].lastNetworkBytes == 10000

    def test_idle_pass_records_external_container_disappearance(
        self, client, mock_docker
    ):
        client.post("/jobs", json={"jobId": "ghost", "type": "http", "ttlSeconds": 3600})
        wait_for_event("sandbox_ready", jobId="ghost")

        import app
        from docker.errors import NotFound

        # Container vanishes externally (e.g., killed via docker CLI).
        mock_docker.containers.get.side_effect = NotFound("gone")
        app._run_reaper_pass()

        term = wait_for_event("sandbox_terminated", jobId="ghost")
        assert term["reason"] == "container_missing"


class TestKillEndpoint:
    def test_delete_running_sandbox_terminates_it(self, client, mock_docker):
        client.post("/jobs", json={"jobId": "byebye", "type": "http"})
        wait_for_event("sandbox_ready", jobId="byebye")

        r = client.delete("/jobs/byebye")
        assert r.status_code == 200
        assert r.json()["reason"] == "manual"

        # container.remove was called with force=True.
        container = mock_docker.containers.get("cid-byebye")
        container.remove.assert_called_with(force=True)

        term = wait_for_event("sandbox_terminated", jobId="byebye")
        assert term["reason"] == "manual"

    def test_delete_queued_job_removes_it_before_worker_picks_up(
        self, client, mock_docker
    ):
        import app
        import threading

        # Block the first containers.run() so the worker is mid-build on
        # 'first' while we delete 'second' from the queue.
        gate = threading.Event()
        real_run = mock_docker.containers.run.side_effect

        def blocking_run(*args, **kwargs):
            gate.wait(timeout=3)
            return real_run(*args, **kwargs)

        mock_docker.containers.run.side_effect = blocking_run

        client.post("/jobs", json={"jobId": "first", "type": "http"})
        client.post("/jobs", json={"jobId": "second", "type": "http"})
        # Either both are queued or 'first' is starting and 'second' is
        # queued — either way 'second' should not yet be ready.
        assert app.sandboxes["second"].status in {"queued", "starting"}

        r = client.delete("/jobs/second")
        assert r.status_code == 200
        assert app.sandboxes["second"].status == "terminated"

        gate.set()
        wait_for_event("sandbox_ready", jobId="first")
        # 'second' was removed from the queue before the worker reached it.
        names = [c.kwargs["name"] for c in mock_docker.containers.run.call_args_list]
        assert "second" not in names

    def test_delete_unknown_jobId_returns_404(self, client):
        r = client.delete("/jobs/nonexistent")
        assert r.status_code == 404

    def test_delete_already_terminated_returns_409(self, client, mock_docker):
        client.post("/jobs", json={"jobId": "twice", "type": "http"})
        wait_for_event("sandbox_ready", jobId="twice")
        assert client.delete("/jobs/twice").status_code == 200
        # Second delete should fail.
        r2 = client.delete("/jobs/twice")
        assert r2.status_code == 409

    def test_terminated_jobId_can_be_reused(self, client, mock_docker):
        import app

        client.post("/jobs", json={"jobId": "recycle", "type": "http"})
        wait_for_event("sandbox_ready", jobId="recycle")
        client.delete("/jobs/recycle")
        assert app.sandboxes["recycle"].status == "terminated"

        # Submitting the same jobId again now succeeds.
        r = client.post("/jobs", json={"jobId": "recycle", "type": "http"})
        assert r.status_code == 202

        # Poll the record itself (the log already has a sandbox_ready event
        # from the first sandbox, so wait_for_event would match prematurely).
        deadline = time.time() + 3.0
        while time.time() < deadline:
            if app.sandboxes["recycle"].status == "ready":
                break
            time.sleep(0.05)
        assert app.sandboxes["recycle"].status == "ready"

        names = [c.kwargs["name"] for c in mock_docker.containers.run.call_args_list]
        assert names.count("recycle") == 2


class TestHealthz:
    def test_healthz_reports_registered_sandbox_types(self, client):
        r = client.get("/healthz")
        assert r.status_code == 200
        body = r.json()
        assert set(body["sandboxTypes"]) == {"http", "browser"}
        assert body["queueDepth"] == 0
