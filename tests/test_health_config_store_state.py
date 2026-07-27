"""/health surfaces ConfigStore reload health.

Before this, a ConfigStore reload failure (exception swallowed) or a
silent cache wipe (storage query succeeds with fewer/zero rows) had no
checkable signal anywhere — not even a log line for the wipe case. This
covers the new ``config_store.reload_ok`` / ``last_reload_at`` fields in
the ``/health`` payload, and that a failed reload degrades overall status.
"""

from __future__ import annotations

import queue
import threading
from unittest.mock import MagicMock

import pytest


@pytest.fixture()
def make_client():
    from starlette.testclient import TestClient

    from turnstone.server import create_app

    clients = []

    def _make(config_store=None):
        mock_mgr = MagicMock()
        mock_mgr.list_all.return_value = []
        mock_mgr.max_active = 10
        app = create_app(
            workstreams=mock_mgr,
            global_queue=queue.Queue(),
            global_listeners=[],
            global_listeners_lock=threading.Lock(),
            skip_permissions=False,
            jwt_secret="test-jwt-secret-minimum-32-chars!",
            config_store=config_store,
        )
        client = TestClient(app, raise_server_exceptions=False)
        clients.append(client)
        return client

    yield _make
    for c in clients:
        c.close()


def test_health_no_config_store_key_when_unwired(make_client):
    """Payload shape unchanged when no ConfigStore is wired (matches
    create_app's default of config_store=None, as existing tests rely on)."""
    resp = make_client().get("/health")
    assert resp.status_code == 200
    assert "config_store" not in resp.json()
    assert resp.json()["status"] == "ok"


def test_health_reports_healthy_reload(make_client):
    cs = MagicMock()
    cs.last_reload_ok = True
    cs.last_reload_at = "2026-07-27T12:00:00+00:00"
    resp = make_client(config_store=cs).get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["config_store"] == {
        "reload_ok": True,
        "last_reload_at": "2026-07-27T12:00:00+00:00",
    }
    assert body["status"] == "ok"


def test_health_surfaces_failed_reload_as_degraded(make_client):
    """The silent-failure case must be observable: a failed ConfigStore
    reload flips overall /health status to degraded, not just a buried key."""
    cs = MagicMock()
    cs.last_reload_ok = False
    cs.last_reload_at = "2026-07-27T12:00:00+00:00"
    resp = make_client(config_store=cs).get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["config_store"]["reload_ok"] is False
    assert body["status"] == "degraded"
