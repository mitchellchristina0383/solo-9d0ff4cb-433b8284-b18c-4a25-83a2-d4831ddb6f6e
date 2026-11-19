import datetime as dt
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from fastapi.testclient import TestClient

from app import ntp_proto, web
from app.clock import Sample
from app.config import Settings
from app.upstream import QueryOutcome, UpstreamConfig
from tests.fakes import FakeUpstream


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


# ---------------------------------------------------------------------------
# Expected-UTC handling / 2036 rollover in the web probe
# ---------------------------------------------------------------------------
WRAP = ntp_proto.NTP_WRAP_UNIX


@pytest.mark.parametrize(
    "text,expected",
    [
        ("2036-02-08", dt.datetime(2036, 2, 8, tzinfo=dt.timezone.utc).timestamp()),
        ("2036-02-08T00:00:00Z", dt.datetime(2036, 2, 8, tzinfo=dt.timezone.utc).timestamp()),
        ("2036-02-08 00:00:00", dt.datetime(2036, 2, 8, tzinfo=dt.timezone.utc).timestamp()),
        ("2036-02-08T08:00:00+08:00", dt.datetime(2036, 2, 8, tzinfo=dt.timezone.utc).timestamp()),
        ("", None),
        ("   ", None),
        (None, None),
    ],
)
def test_parse_expected_utc(text, expected):
    got = web._parse_expected_utc(text)
    if expected is None:
        assert got is None
    else:
        assert got == pytest.approx(expected)


def test_parse_expected_utc_rejects_garbage():
    with pytest.raises(ValueError):
        web._parse_expected_utc("yesterday")
    with pytest.raises(ValueError):
        web._parse_expected_utc("2036-13-40")


def test_probe_rejects_bad_expected_utc(app_client):
    client, _state, _stub = app_client
    r = client.post("/api/probe", json={"expected_utc": "not-a-date"})
    assert r.status_code == 400
    assert "预期 UTC" in r.json()["detail"]


def _make_app_at(now_fn, tmp_path):
    up = FakeUpstream(now_fn=now_fn)
    settings = Settings(
        upstreams=[UpstreamConfig("wrap", "127.0.0.1", up.addr[1], "回卷源")],
        sample_interval=60.0, sample_timeout=0.2, holdover_seconds=30.0,
        fresh_seconds=5.0, rollback_tolerance=0.25, ntp_host="127.0.0.1",
        ntp_port=0, http_port=8000, data_path=str(tmp_path / "history.json"),
        probe_timeout=0.2,
    )
    state = web.AppState(settings)
    return up, web.create_app(settings, state=state)


@pytest.mark.parametrize(
    "instant",
    [WRAP - 3600, WRAP + 3600],
    ids=["before-wrap", "after-wrap"],
)
def test_web_probe_handles_rollover(tmp_path, instant):
    """Valid replies on either side of the wrap are shown with real UTC."""
    up, app = _make_app_at(lambda: instant, tmp_path)
    iso = dt.datetime.fromtimestamp(instant, tz=dt.timezone.utc).date().isoformat()
    expectation = dt.datetime.fromtimestamp(instant, tz=dt.timezone.utc).isoformat()
    try:
        with TestClient(app) as client:
            r = client.post("/api/probe", json={"source_id": "wrap", "expected_utc": expectation})
            body = r.json()
        assert r.status_code == 200
        out = body["results"][0]
        assert out["ok"] is True, out["error"]
        assert out["timestamps"]["receive"].startswith(iso)
        assert out["timestamps"]["transmit"].startswith(iso)
        assert out["reference"]["era"] == ntp_proto.era_for(instant)
        upstream_ts = dt.datetime.fromisoformat(out["sample"]["upstream_iso"]).timestamp()
        assert upstream_ts == pytest.approx(instant, abs=1.0)
        # Relative delta is upstream time minus our receive wall clock, i.e.
        # the same offset the probe reports (within loopback RTT/2).
        assert out["sample"]["delta_vs_our_time"] == pytest.approx(
            instant - out["sample"]["our_time"], abs=2e-3
        )
        assert 0 <= out["sample"]["rtt"] < 0.5
    finally:
        up.close()


def test_web_probe_straddling_wrap_shows_ordered_timestamps(tmp_path):
    """Receive before the rollover, transmit after: both decode, order holds."""
    recv = WRAP - 0.00005
    up, app = _make_app_at(lambda: recv, tmp_path)
    try:
        with TestClient(app) as client:
            r = client.post(
                "/api/probe",
                json={"source_id": "wrap", "expected_utc": "2036-02-07T06:28:16Z"},
            )
        out = r.json()["results"][0]
        assert out["ok"] is True, out["error"]
        t2 = dt.datetime.fromisoformat(out["timestamps"]["receive"]).timestamp()
        t3 = dt.datetime.fromisoformat(out["timestamps"]["transmit"]).timestamp()
        assert t2 == pytest.approx(recv, abs=2e-3)
        assert t3 == pytest.approx(recv + 0.0001, abs=2e-3)
        assert t3 > t2
        assert out["sample"]["rtt"] < 0.5
    finally:
        up.close()


def test_web_probe_blank_expected_uses_local_date(tmp_path):
    """Blank expectation falls back to the host clock at send time."""
    before = time.time()
    up, app = _make_app_at(lambda: time.time(), tmp_path)
    try:
        with TestClient(app) as client:
            out = client.post("/api/probe", json={"source_id": "wrap"}).json()["results"][0]
        after = time.time()
        assert out["ok"] is True, out["error"]
        assert out["reference"]["era"] == ntp_proto.era_for(time.time())
        ref = dt.datetime.fromisoformat(out["reference"]["expected_utc"]).timestamp()
        assert before - 1 <= ref <= after + 1
    finally:
        up.close()


def test_web_probe_missing_timestamp_is_still_an_error(tmp_path):
    up, app = _make_app_at(lambda: WRAP + 3600, tmp_path)
    up.mode = "zero_recv"
    try:
        with TestClient(app) as client:
            out = client.post(
                "/api/probe",
                json={"source_id": "wrap", "expected_utc": "2036-02-08"},
            ).json()["results"][0]
        assert out["ok"] is False
        assert "missing receive/transmit" in out["error"]
        # Diagnostic timestamps still show what did arrive.
        assert out["timestamps"]["receive"] is None
        assert out["timestamps"]["transmit"] is not None
    finally:
        up.close()
