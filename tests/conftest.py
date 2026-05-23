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
    """Replace docker.from_env with a MagicMock recording all run() calls.

    Containers created via run() are also retrievable via get() and have a
    no-op stats() returning zero network bytes by default. Tests can mutate
    a container's stats.return_value to simulate traffic.
    """
    import docker
    from docker.errors import NotFound

    client = MagicMock(name="docker_client")
    client.ping.return_value = True

    port_counter = {"n": 49999}
    containers_by_id: dict[str, MagicMock] = {}

    def fake_run(image, *, detach, name, ports, labels, **_):
        port_counter["n"] += 1
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
        container.stats.return_value = {
            "networks": {"eth0": {"rx_bytes": 0, "tx_bytes": 0}}
        }
        containers_by_id[container.id] = container
        return container

    def fake_get(container_id):
        if container_id in containers_by_id:
            return containers_by_id[container_id]
        raise NotFound(f"no such container: {container_id}")

    client.containers.run.side_effect = fake_run
    client.containers.get.side_effect = fake_get
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
