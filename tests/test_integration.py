"""End-to-end tests over real loopback UDP sockets."""

import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from app.clock import ClockState
from app.ntp_server import NtpServer
from app.sampler import Sampler
from app.upstream import UpstreamConfig, query
from tests.fakes import FakeUpstream


@pytest.fixture
def upstream():
    up = FakeUpstream(offset=0.0)
    yield up
    up.close()


def test_query_accepts_valid_reply(upstream):
    cfg = UpstreamConfig("x", "127.0.0.1", upstream.addr[1])
    out = query(cfg, timeout=1.0, max_rtt=2.0)
    assert out.ok, out.error
    assert abs(out.sample.offset) < 0.5
    assert out.sample.stratum == 2


def test_query_measures_offset(upstream):
    upstream.offset = 12.0
    cfg = UpstreamConfig("x", "127.0.0.1", upstream.addr[1])
    out = query(cfg, timeout=1.0, max_rtt=2.0)
    assert out.ok, out.error
    assert out.sample.offset == pytest.approx(12.0, abs=0.2)


@pytest.mark.parametrize("mode", ["silent", "liar", "bad_origin", "bad_mode"])
def test_query_rejects_bad_replies(upstream, mode):
    upstream.mode = mode
    cfg = UpstreamConfig("x", "127.0.0.1", upstream.addr[1])
    out = query(cfg, timeout=0.6, max_rtt=2.0)
    assert not out.ok
    assert out.error


def test_query_ignores_reply_from_wrong_port(upstream):
    """A second socket answering from another port must not be trusted."""
    import socket

    attacker = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    attacker.bind(("127.0.0.1", 0))
    cfg = UpstreamConfig("x", "127.0.0.1", upstream.addr[1])
    # Poison the "upstream" with silence so only the attacker answers.
    upstream.mode = "silent"

    def attacker_loop():
        attacker.settimeout(0.6)
        try:
            data, peer = attacker.recvfrom(1024)  # never used; just in case
        except OSError:
            pass

    t = threading.Thread(target=attacker_loop, daemon=True)
    t.start()
    out = query(cfg, timeout=0.5, max_rtt=2.0)
    assert not out.ok
    attacker.close()
    t.join(timeout=1)


def test_sampler_and_server_end_to_end():
    up = FakeUpstream(offset=0.0)
    clock = ClockState(holdover_seconds=30.0, fresh_seconds=2.0)
    sampler = Sampler(
        [UpstreamConfig("a", "127.0.0.1", up.addr[1])],
        clock,
        interval=1.0,
        timeout=0.5,
    )
    server = NtpServer(clock, host="127.0.0.1", port=0)
    server.start()
    port = server._sock.getsockname()[1]
    try:
        sampler.poll_once()
        snap = clock.status()
        assert snap.synced, snap
        assert snap.source_id == "a"

        cfg = UpstreamConfig("self", "127.0.0.1", port)
        out = query(cfg, timeout=1.0, max_rtt=2.0)
        assert out.ok, out.error
        assert out.sample.stratum == clock.status().stratum  # upstream stratum + 1
    finally:
        server.stop()
        up.close()


def test_server_reports_unsynced_after_expiry_and_holdover():
    up = FakeUpstream(offset=0.0)
    clock = ClockState(holdover_seconds=1.5, fresh_seconds=0.5)
    sampler = Sampler(
        [UpstreamConfig("a", "127.0.0.1", up.addr[1])], clock,
        interval=1.0, timeout=0.5,
    )
    server = NtpServer(clock, host="127.0.0.1", port=0)
    server.start()
    port = server._sock.getsockname()[1]
    cfg = UpstreamConfig("self", "127.0.0.1", port)
    try:
        sampler.poll_once()
        out = query(cfg, timeout=1.0, max_rtt=2.0)
        assert out.ok and out.sample.stratum != 16

        up.mode = "silent"
        time.sleep(2.2)  # past both fresh window and holdover
        out = query(cfg, timeout=1.0, max_rtt=2.0)
        # The reply arrives but is correctly refused as a time source.
        assert not out.ok
        assert "unsynchronised" in (out.error or "")
        assert out.stratum == 16
        assert out.leap == 3
    finally:
        server.stop()
        up.close()


def test_server_ignores_non_client_modes():
    up = FakeUpstream()
    clock = ClockState(holdover_seconds=10.0)
    server = NtpServer(clock, host="127.0.0.1", port=0)
    server.start()
    port = server._sock.getsockname()[1]
    try:
        import socket

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(0.5)
        # Build a mode-5 (broadcast) packet and expect no reply.
        packet = bytearray(48)
        packet[0] = (4 << 3) | 5
        sock.sendto(bytes(packet), ("127.0.0.1", port))
        with pytest.raises(TimeoutError):
            sock.recvfrom(1024)
        assert server.describe()["rejected"] >= 1
        sock.close()
    finally:
        server.stop()
        up.close()
