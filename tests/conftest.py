"""Test fixtures.

Mocks docker.from_env so tests run without a Docker daemon: fast,
deterministic, and able to assert on container args and call order.
"""
import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Point the app's file logger at a tmp file BEFORE app is imported anywhere,
# since the FileHandler binds to whatever LOG_PATH resolves to at import time.
_LOG_PATH = Path(tempfile.gettempdir()) / "sandbox_test.log"
os.environ["SANDBOX_LOG_PATH"] = str(_LOG_PATH)


@pytest.fixture
def log_path() -> Path:
    return _LOG_PATH


@pytest.fixture
def mock_docker(monkeypatch):
    """Replace docker.from_env with a MagicMock recording all run() calls."""
    import docker

    client = MagicMock(name="docker_client")
    client.ping.return_value = True

    port_counter = {"n": 49999}

    def fake_run(image, *, detach, name, ports, labels, **_):
        port_counter["n"] += 1
        # The builder asks for a specific container port (e.g., "80/tcp",
        # "3000/tcp"); echo that back in the attrs so each builder's port
        # discovery works regardless of type.
        container_port = next(iter(ports))
        container = MagicMock(name=f"container_{name}")
        container.id = f"cid-{name}"
        container.attrs = {
            "NetworkSettings": {
                "Ports": {
                    container_port: [
                        {"HostIp": "0.0.0.0", "HostPort": str(port_counter["n"])}
                    ]
                }
            }
        }
        return container

    client.containers.run.side_effect = fake_run
    monkeypatch.setattr(docker, "from_env", lambda: client)
    return client


@pytest.fixture(autouse=True)
def reset_state():
    """Clear the in-memory deque + sandbox registry and truncate the log."""
    import app

    app.job_queue.clear()
    app.sandboxes.clear()
    _LOG_PATH.write_text("")
    yield


@pytest.fixture
def client(mock_docker):
    """FastAPI TestClient with lifespan active (worker task running)."""
    from fastapi.testclient import TestClient
    import app

    with TestClient(app.app) as c:
        yield c


def read_log_events() -> list[dict]:
    if not _LOG_PATH.exists():
        return []
    return [
        json.loads(line)
        for line in _LOG_PATH.read_text().splitlines()
        if line.strip()
    ]
