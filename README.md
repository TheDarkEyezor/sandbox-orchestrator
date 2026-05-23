# Sandbox Orchestrator

Lightweight orchestration platform for short-lived sandbox containers, intended for spinning up isolated environments for AI agents on demand.

A FastAPI producer accepts job submissions, an in-memory queue buffers them, an async worker spins up a Docker container per job, and a reaper task tears them down on TTL expiry, idle timeout, or explicit `DELETE`.

The original task brief lives in [Reference.md](Reference.md).

## Quickstart

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# requires a running Docker daemon
uvicorn app:app --port 8080
```

Then:

```bash
# submit an http sandbox
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"jobId": "abc123", "type": "http"}'

# watch the live state
curl http://localhost:8080/sandboxes

# tear it down
curl -X DELETE http://localhost:8080/jobs/abc123
```

The default TTL/idle windows are short (15s) so the system is easy to exercise interactively. Override via env vars for real use.

## Architecture

```
                +--------------------+
   POST /jobs ->|  FastAPI producer  |-- pydantic-validate --+
                +--------------------+                       |
                          |                                  |
                          v                                  |
                 +------------------+    +-----------------+ |
                 |  collections.    |<---|  SandboxRecord  |<+
                 |  deque (queue)   |    |  registry       |
                 +------------------+    |  dict[str, R]   |
                          |              +-----------------+
                          v                       ^
                +--------------------+            |
                |  Async worker_loop |-- update --+
                |  (background task) |            |
                +--------------------+            |
                          |                       |
                  asyncio.to_thread               |
                          v                       |
                +--------------------+            |
                |  SandboxBuilder    |            |
                |    HttpSandbox     |            |
                |    BrowserSandbox  |            |
                +--------------------+            |
                          |                       |
                          v                       |
                +--------------------+            |
                |  Docker SDK        |            |
                +--------------------+            |
                          ^                       |
                          |                       |
                +--------------------+            |
                |  Async reaper_loop |-- update --+
                |  (TTL / idle)      |
                +--------------------+
```

All components live in `app.py`. A JSON-lines event log is written to `sandbox.log` for forensic replay.

## Endpoints

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/jobs` | Submit a job (`jobId`, `type`, optional `ttlSeconds`). Returns 202 with the queued state, 409 if `jobId` is already active, 422 on validation failure. |
| `DELETE` | `/jobs/{jobId}` | Terminate a queued or ready sandbox. 404 if unknown, 409 if already terminal. |
| `GET` | `/sandboxes` | Live state: by-status counts, per-type running counts, and the full record list. |
| `GET` | `/healthz` | Queue depth and registered sandbox types. |

## Sandbox types

| Type | Image | Container port | URL scheme |
| --- | --- | --- | --- |
| `http` | `nginx:alpine` | `80/tcp` | `http://localhost:<port>` |
| `browser` | `browserless/chrome:latest` | `3000/tcp` (CDP-over-WebSocket) | `ws://localhost:<port>` |

Adding a new type: subclass `SandboxBuilder`, implement `build()`, register the instance in `BUILDERS`. The pydantic validator picks it up automatically and `GET /healthz` reflects it without further changes.

## Architectural decisions

### Single in-memory source of truth

`sandboxes: dict[str, SandboxRecord]` is the only structure tracking current state. Per-type counts and the `GET /sandboxes` view are derived from filters over this dict — no parallel structures to drift out of sync.

### Log file as audit trail

Every state transition (`job_enqueued`, `sandbox_starting`, `sandbox_ready`, `sandbox_failed`, `sandbox_terminated`, `reaper_error`) is appended to `sandbox.log` as a JSON line. The in-memory dict holds *current* state; the log file holds *history* — including records reused for new submissions.

### Builder classes per sandbox type

`SandboxBuilder` is an abstract base; each subclass owns its image, port mapping, and URL scheme. New types are one subclass plus one `BUILDERS` entry — no conditional logic to extend. The class shape leaves room for per-type lifecycle hooks (readiness probes, custom env, etc.) without forcing a refactor.

### Dynamic type validation

The pydantic validator on `Job.type` checks against `BUILDERS.keys()` rather than a hard-coded enum. Adding a builder automatically extends the set of valid request types — no two-place update.

### Single FastAPI process, async tasks

Producer (HTTP), worker, and reaper all run as asyncio tasks in one process. The Docker SDK is blocking, so container ops are wrapped in `asyncio.to_thread` to keep the event loop responsive. One process means everything shares the same in-memory state without IPC.

### Idle detection via Docker network stats

Tracking "time since last request" without a reverse proxy is hard — we never see traffic to the sandbox. The reaper polls `container.stats()` and treats the total of `rx_bytes + tx_bytes` across all interfaces as an activity signal. Bytes grew → reset the idle timer; bytes flat for `IDLE_TIMEOUT_SECONDS` → terminate. This approximates true idleness but a long-lived download with periodic bursts will look "active" indefinitely. The reaper is conservative by design.

### Producer-side jobId dedup

A duplicate `POST /jobs` returns 409 immediately, leaving the original record untouched. Without this, the duplicate would be enqueued and fail later at the Docker name-conflict layer as `sandbox_failed` — late and noisy. Terminal records (failed/terminated) *can* be overwritten by a new submission with the same `jobId`, so jobIds are reusable once a sandbox is gone.

### Reaper pass extracted from the loop

`_run_reaper_pass(now=...)` is callable directly with a synthetic timestamp. This lets the test suite drive TTL and idle scenarios deterministically without sleeping past the real `REAPER_INTERVAL_SECONDS`.

## Configuration

| Env var | Default | Description |
| --- | --- | --- |
| `SANDBOX_LOG_PATH` | `sandbox.log` | JSON-lines event log destination |
| `DEFAULT_TTL_SECONDS` | `15` | Per-job TTL when not set by the caller (1..86400) |
| `IDLE_TIMEOUT_SECONDS` | `15` | Quiet window before idle termination |
| `REAPER_INTERVAL_SECONDS` | `5` | How often the reaper sweeps |

Shipped defaults are short to keep manual testing snappy. For real use, raise them — e.g. `DEFAULT_TTL_SECONDS=1800`, `IDLE_TIMEOUT_SECONDS=300`.

## Testing

```bash
pip install -r requirements-dev.txt
pytest
```

The 31-test suite mocks the Docker SDK, so it runs in under 4 seconds without a daemon. It asserts on the SDK call args directly (image, name, ports, labels), reads back the event log, and drives `_run_reaper_pass` with synthetic timestamps to cover TTL and idle paths without real sleep.

## Project layout

```
app.py                  FastAPI app, builders, worker, reaper
requirements.txt        runtime deps
requirements-dev.txt    runtime + test deps
tests/                  pytest suite (mocked Docker)
sandbox.log             runtime event log (gitignored)
Reference.md            original task brief
```
