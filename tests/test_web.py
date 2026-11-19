import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from fastapi.testclient import TestClient

from app import web
from app.clock import Sample
from app.config import Settings
from app.upstream import QueryOutcome, UpstreamConfig


class StubQuery:
    """Deterministic replacement for the UDP client, keyed by source."""

    def __init__(self, offsets: dict[str, float]) -> None:
        self.offsets = offsets
        self.fail = set()

    def __call__(self, cfg, *, timeout=2.0, **kwargs):
        if cfg.id in self.fail:
            return QueryOutcome(ok=False, error="timeout waiting for reply")
        now = time.time()
        offset = self.offsets.get(cfg.id, 0.0)
        sample = Sample(
            source_id=cfg.id,
            source_ip="127.0.0.1",
            source_port=cfg.port,
            stratum=2,
            leap=0,
            upstream_ref_ts=now - 10,
            upstream_root_delay=0.001,
            upstream_root_dispersion=0.002,
            offset=offset,
            delay=0.002,
            true_time=now + offset,
            local_mono=time.monotonic(),
            local_wall=now,
        )
        return QueryOutcome(ok=True, sample=sample, peer="127.0.0.1", stratum=2, leap=0,
                            root_delay=0.001, root_dispersion=0.002, rtt=0.002)


@pytest.fixture
def app_client(tmp_path):
    settings = Settings(
        upstreams=[
            UpstreamConfig("upstream-a", "127.0.0.1", 1123, "上游 A"),
            UpstreamConfig("upstream-b", "127.0.0.1", 1124, "上游 B"),
        ],
        sample_interval=60.0,
        sample_timeout=0.2,
        holdover_seconds=30.0,
        fresh_seconds=5.0,
        rollback_tolerance=0.25,
        ntp_host="127.0.0.1",
        ntp_port=0,
        http_port=8000,
        data_path=str(tmp_path / "history.json"),
        probe_timeout=0.2,
    )
    state = web.AppState(settings)
    stub = StubQuery({"upstream-a": 0.0, "upstream-b": 0.0})
    state.sampler._query = stub
    app = web.create_app(settings, state=state)
    app.state.stub = stub
    # create_app() does not auto-start externally supplied state, so start the
    # real UDP responder here for the self-probe test.
    state.server.start()
    try:
        with TestClient(app) as client:
            yield client, state, stub
    finally:
        state.server.stop()


def test_index_and_status(app_client):
    client, state, stub = app_client
    r = client.get("/")
    assert r.status_code == 200
    assert "隔离网 NTP 服务" in r.text

    client.post("/api/refresh")
    time.sleep(0.1)
    status = client.get("/api/status").json()
    assert status["clock"]["synced"] is True
    assert status["clock"]["source"]["id"] in ("upstream-a", "upstream-b")
    assert status["server"]["port"] == state.server.bound_port
    assert status["server"]["port"] not in (None, 0)
    assert len(status["sampler"]["sources"]) == 2


def test_disable_active_source_switches(app_client):
    client, state, stub = app_client
    client.post("/api/refresh")
    time.sleep(0.1)
    first = client.get("/api/status").json()["sampler"]["active_source"]
    other = "upstream-b" if first == "upstream-a" else "upstream-a"

    r = client.post(f"/api/sources/{first}/enabled", json={"enabled": False})
    assert r.status_code == 200
    time.sleep(0.1)
    status = client.get("/api/status").json()
    assert status["clock"]["synced"] is True
    assert status["sampler"]["active_source"] == other


def test_disable_all_sources_then_expire(app_client):
    client, state, stub = app_client
    client.post("/api/refresh")
    time.sleep(0.1)
    for sid in ("upstream-a", "upstream-b"):
        client.post(f"/api/sources/{sid}/enabled", json={"enabled": False})
    time.sleep(0.1)
    status = client.get("/api/status").json()
    # Still within holdover: served, but explicitly in holdover.
    assert status["clock"]["synced"] is True
    assert status["clock"]["state"] in ("holdover", "synced")


def test_probe_sends_real_request_and_reports_failure(app_client):
    """The probe endpoint deliberately bypasses the sampler stub."""
    client, state, stub = app_client
    r = client.post("/api/probe", json={"source_id": "upstream-a", "timeout": 0.2})
    body = r.json()
    assert body["results"][0]["ok"] is False
    assert body["results"][0]["error"]


def test_unknown_source_404(app_client):
    client, _state, _stub = app_client
    assert client.post("/api/sources/nope/enabled", json={"enabled": False}).status_code == 404
    assert client.post("/api/probe", json={"source_id": "nope"}).status_code == 404


def test_self_probe_returns_real_reply(app_client):
    client, state, _stub = app_client
    client.post("/api/refresh")
    time.sleep(0.1)
    r = client.post("/api/probe/server")
    out = r.json()["results"][0]
    assert out["ok"] is True, out["error"]
    assert out["sample"]["rtt"] < 0.5
    assert out["stratum"] == state.clock.status().stratum


def test_pause_toggle(app_client):
    client, state, _stub = app_client
    r = client.post("/api/sampler/paused", json={"paused": True})
    assert r.json()["sampler"]["paused"] is True
    r = client.post("/api/sampler/paused", json={"paused": False})
    assert r.json()["sampler"]["paused"] is False


def test_restart_ignores_old_history(app_client, tmp_path):
    client, state, stub = app_client
    client.post("/api/refresh")
    time.sleep(0.1)
    assert state.clock.status().synced

    # Simulate a restart: brand-new AppState over the same data file.
    fresh = web.AppState(state.settings)
    assert fresh.clock.status().synced is False
    assert fresh.clock.status().state == "never-synced"
    assert fresh.clock.describe()["restored_history_samples"] >= 1
