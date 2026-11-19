#!/usr/bin/env python3
"""Operator-style self-test: rollback, source loss, expiry, restart.

Runs entirely in one process with real loopback UDP sockets and fake time is
NOT used, so it exercises the production threading and socket paths.  Time
constants are shortened so the whole run takes a few seconds.

    python tests/selftest_scenarios.py

Exit code 0 means every scenario behaved as required.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import ntp_proto
from app.clock import ClockState, HistoryStore
from app.ntp_server import NtpServer
from app.sampler import Sampler
from app.upstream import UpstreamConfig, query
from tests.fakes import FakeUpstream

HOLDOVER = 3.0
FRESH = 1.0

results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, ok, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def ntp_time(port: int) -> tuple[float, int, int]:
    """Ask our own service for the time and return (tx, stratum, leap)."""
    import socket

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(2.0)
    nonce = int.from_bytes(os.urandom(8), "big")
    try:
        sock.sendto(ntp_proto.client_request_nonce(nonce), ("127.0.0.1", port))
        data, _ = sock.recvfrom(1024)
    finally:
        sock.close()
    assert ntp_proto.origin_bits(data) == nonce
    pk = ntp_proto.decode(data)
    return pk.transmit_ts, pk.stratum, pk.leap


def main() -> int:
    a = FakeUpstream()
    b = FakeUpstream()
    data_dir = tempfile.mkdtemp()
    history_path = os.path.join(data_dir, "history.json")

    clock = ClockState(holdover_seconds=HOLDOVER, fresh_seconds=FRESH, history=HistoryStore(history_path))
    sampler = Sampler(
        [UpstreamConfig("a", "127.0.0.1", a.addr[1]), UpstreamConfig("b", "127.0.0.1", b.addr[1])],
        clock,
        interval=0.5,
        timeout=0.4,
    )
    server = NtpServer(clock, host="127.0.0.1", port=0)
    server.start()
    port = server._sock.getsockname()[1]

    try:
        sampler.poll_once()
        tx0, stratum0, leap0 = ntp_time(port)
        check("同步后对外应答正常", stratum0 not in (0, 16) and leap0 == 0,
              f"stratum={stratum0} leap={leap0}")

        # ---- 1. upstream rollback -------------------------------------
        a.offset = -3600.0
        sampler.poll_once()
        tx1, stratum1, leap1 = ntp_time(port)
        check("上游回拨不导致对外时间倒退", tx1 >= tx0,
              f"{tx0:.3f} -> {tx1:.3f}")

        # Source B still healthy at the correct time; it must be picked.
        check("回拨源被拒绝后仍有可信来源", clock.status().synced,
              f"state={clock.status().state} source={clock.status().source_id}")
        a.offset = 0.0

        # ---- 2. both sources lost -------------------------------------
        a.mode = "silent"
        b.mode = "silent"
        start = time.monotonic()
        while time.monotonic() - start < HOLDOVER * 0.4:
            sampler.poll_once()
            time.sleep(0.1)
        tx2, stratum2, leap2 = ntp_time(port)
        check("失联但保鲜期内仍按已确认时间推进",
              stratum2 not in (0, 16) and tx2 > tx1,
              f"stratum={stratum2} leap={leap2} {tx1:.3f} -> {tx2:.3f}")

        # ---- 3. holdover expiry ---------------------------------------
        time.sleep(HOLDOVER + 0.6)
        sampler.poll_once()
        tx3, stratum3, leap3 = ntp_time(port)
        check("保鲜期过后报告未同步", stratum3 == 16 and leap3 == 3,
              f"stratum={stratum3} leap={leap3}")
        check("过期后时间冻结不冒充正常", tx3 >= tx2,
              f"{tx2:.3f} -> {tx3:.3f}")

        # ---- 4. restart cannot revive expired samples -----------------
        old_history = HistoryStore(history_path)
        loaded = old_history.load()
        fresh_clock = ClockState(
            holdover_seconds=HOLDOVER, fresh_seconds=FRESH,
            history=HistoryStore(history_path),
        )
        snap = fresh_clock.status()
        check("重启后旧样本不重新生效",
              loaded > 0 and not snap.synced and snap.state == "never-synced",
              f"loaded={loaded} state={snap.state}")

        # ---- 5. recovery after expiry --------------------------------
        a.mode = "ok"
        b.mode = "ok"
        time.sleep(0.2)
        sampler.poll_once()
        tx4, stratum4, leap4 = ntp_time(port)
        check("恢复上游后重新同步且不倒退",
              stratum4 not in (0, 16) and tx4 >= tx3,
              f"stratum={stratum4} {tx3:.3f} -> {tx4:.3f}")
    finally:
        server.stop()
        a.close()
        b.close()

    # ---- 6. 2036 seconds-field rollover (diagnostic probe path) ------
    wrap = ntp_proto.NTP_WRAP_UNIX
    scenarios = [
        ("回卷前应答按参照解码", wrap - 3600, wrap - 3600),
        ("回卷后应答按参照解码", wrap + 3600, wrap + 3600),
        ("接收/发送分处回卷点两侧", wrap - 0.00005, wrap - 0.00005),
    ]
    for label, source_now, expected in scenarios:
        up = FakeUpstream(now_fn=lambda t=source_now: t)
        try:
            out = query(UpstreamConfig("w", "127.0.0.1", up.addr[1]),
                        timeout=1.0, max_rtt=2.0, reference_time=wrap)
            if out.ok:
                got = out.sample.true_time
                detail = (f"recv={out.recv_ts - wrap:+.6f}s tx={out.transmit_ts - wrap:+.6f}s "
                          f"rtt={out.sample.delay * 1000:.3f}ms")
            else:
                got, detail = None, out.error or "no sample"
            check(label,
                  out.ok and got is not None and abs(got - expected) < 1.0,
                  detail)
        finally:
            up.close()

    up = FakeUpstream(now_fn=lambda: wrap + 3600)
    up.mode = "zero_recv"
    try:
        out = query(UpstreamConfig("w", "127.0.0.1", up.addr[1]),
                    timeout=1.0, max_rtt=2.0, reference_time=wrap + 3600)
        check("缺失时间戳在回卷后仍报错",
              not out.ok and "missing receive/transmit" in (out.error or ""),
              out.error or "accepted?!")
    finally:
        up.close()

    up = FakeUpstream(now_fn=lambda: wrap + 3600)
    try:
        out = query(UpstreamConfig("w", "127.0.0.1", up.addr[1]), timeout=1.0)
        check("采样路径不按探测参照解释回卷后应答",
              not out.ok and out.reference_time is None,
              out.error or "accepted?!")
    finally:
        up.close()

    with open(os.path.join(data_dir, "selftest-report.json"), "w", encoding="utf-8") as fh:
        json.dump([{"name": n, "ok": ok, "detail": d} for n, ok, d in results], fh, indent=2, ensure_ascii=False)

    failed = [r for r in results if not r[1]]
    print(f"\n{len(results) - len(failed)}/{len(results)} scenarios passed")
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
