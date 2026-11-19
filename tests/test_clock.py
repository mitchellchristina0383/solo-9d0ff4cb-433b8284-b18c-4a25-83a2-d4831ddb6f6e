"""Deterministic tests for the clock state machine using fake time."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from app.clock import ClockState, HistoryStore, Sample


class FakeTime:
    def __init__(self, wall: float = 1_700_000_000.0, mono: float = 100.0) -> None:
        self.wall = wall
        self.mono = mono

    def time(self) -> float:
        return self.wall

    def monotonic(self) -> float:
        return self.mono

    def advance(self, seconds: float) -> None:
        self.mono += seconds
        self.wall += seconds

    def set_wall(self, value: float) -> None:
        """Simulate the host being set backwards without moving monotonic."""
        self.wall = value


def make_sample(ft: FakeTime, *, true_time=None, source="a", stratum=2, delay=0.01):
    true_time = ft.wall if true_time is None else true_time
    return Sample(
        source_id=source,
        source_ip="10.0.0.1",
        source_port=123,
        stratum=stratum,
        leap=0,
        upstream_ref_ts=true_time - 5,
        upstream_root_delay=0.001,
        upstream_root_dispersion=0.002,
        offset=true_time - ft.wall,
        delay=delay,
        true_time=true_time,
        local_mono=ft.mono,
        local_wall=ft.wall,
    )


def make_clock(ft: FakeTime, holdover: float = 60.0, **kw) -> ClockState:
    return ClockState(
        holdover_seconds=holdover,
        fresh_seconds=kw.pop("fresh_seconds", 10.0),
        wall_clock=ft.time,
        monotonic=ft.monotonic,
        **kw,
    )


def test_never_synced_reports_unsynchronised():
    ft = FakeTime()
    clock = make_clock(ft)
    snap = clock.reply()
    assert snap.synced is False
    assert snap.state == "never-synced"
    assert snap.stratum == 16
    assert snap.leap == 3
    assert snap.unix_time == pytest.approx(ft.wall)


def test_sync_then_holdover_then_expire():
    ft = FakeTime()
    clock = make_clock(ft, holdover=30.0, fresh_seconds=5.0)
    clock.record(make_sample(ft))
    snap = clock.status()
    assert snap.synced and snap.state == "synced"
    assert snap.stratum == 3  # upstream stratum + 1
    assert snap.source_id == "a"

    ft.advance(10)
    snap = clock.status()
    assert snap.synced and snap.state == "holdover"
    assert snap.holdover is True
    assert snap.stratum == 3  # still a usable stratum during holdover

    ft.advance(25)  # total 35 > 30 holdover
    snap = clock.status()
    assert not snap.synced
    assert snap.state == "holdover-expired"
    assert snap.stratum == 16 and snap.leap == 3
    # time is frozen at the end of the confirmed window, not advancing
    frozen = snap.unix_time
    ft.advance(50)
    assert clock.status().unix_time == pytest.approx(frozen)


def test_resync_after_expiry_does_not_go_backwards():
    ft = FakeTime()
    clock = make_clock(ft, holdover=10.0, fresh_seconds=5.0)
    clock.record(make_sample(ft))
    ft.advance(40)  # expired, frozen
    frozen = clock.status().unix_time
    # A returning upstream offers the *correct* (later) time: accept it.
    clock.record(make_sample(ft))
    snap = clock.status()
    assert snap.synced
    assert snap.unix_time >= frozen


def test_rollback_is_rejected():
    ft = FakeTime()
    clock = make_clock(ft, holdover=300.0)
    clock.record(make_sample(ft))
    ft.advance(5)
    before = clock.status().unix_time
    # upstream jumps one hour back
    res = clock.record(make_sample(ft, true_time=ft.wall - 3600))
    assert res.accepted is False
    assert res.reason == "rollback"
    snap = clock.status()
    assert snap.unix_time >= before
    assert snap.synced  # still serving confirmed time


def test_small_backwards_jitter_is_clamped_not_applied():
    ft = FakeTime()
    clock = make_clock(ft, holdover=300.0)
    clock.record(make_sample(ft))
    ft.advance(1.0)
    before = clock.status().unix_time
    # 100 ms behind the projected clock: inside tolerance, so hold, don't step
    res = clock.record(make_sample(ft, true_time=ft.wall - 0.1))
    assert res.accepted
    assert res.clamped
    snap = clock.status()
    assert snap.unix_time >= before - 1e-9


def test_source_switch_with_jitter_holds_time():
    ft = FakeTime()
    clock = make_clock(ft, holdover=300.0)
    clock.record(make_sample(ft, source="a"))
    ft.advance(10)
    before = clock.status().unix_time
    # Source B is 100 ms behind A: within jitter tolerance, hold not step.
    res = clock.record(make_sample(ft, source="b", true_time=ft.wall - 0.1))
    assert res.accepted and res.clamped
    assert res.source_changed
    snap = clock.status()
    assert snap.unix_time >= before
    assert snap.source_id == "b"
    assert snap.synced


def test_source_switch_to_rolled_back_source_rejected():
    ft = FakeTime()
    clock = make_clock(ft, holdover=300.0)
    clock.record(make_sample(ft, source="a"))
    ft.advance(1)
    before = clock.status().unix_time
    res = clock.record(make_sample(ft, source="b", true_time=ft.wall - 600))
    assert not res.accepted and res.reason == "rollback"
    snap = clock.status()
    assert snap.source_id == "a"
    assert snap.unix_time >= before


def test_forward_step_is_accepted_and_logged():
    ft = FakeTime()
    clock = make_clock(ft, holdover=300.0)
    clock.record(make_sample(ft))
    ft.advance(1)
    res = clock.record(make_sample(ft, true_time=ft.wall + 30))
    assert res.accepted and res.stepped
    assert clock.status().unix_time == pytest.approx(ft.wall + 30)


def test_host_wall_clock_rollback_is_irrelevant_once_confirmed():
    ft = FakeTime()
    clock = make_clock(ft, holdover=300.0)
    clock.record(make_sample(ft))
    served = clock.status().unix_time
    ft.set_wall(ft.wall - 7200)  # host clock set back two hours
    snap = clock.status()
    assert snap.synced
    assert snap.unix_time >= served  # driven by monotonic, not wall


def test_reply_timestamp_never_moves_backwards_even_before_sync():
    ft = FakeTime()
    clock = make_clock(ft)
    first = clock.reply().unix_time
    ft.set_wall(ft.wall - 100)
    second = clock.reply().unix_time
    assert second >= first


def test_snapshot_time_and_sync_flag_are_consistent():
    """Interleave samples and replies; flags must describe the same state."""
    ft = FakeTime()
    clock = make_clock(ft, holdover=30.0, fresh_seconds=5.0)

    # Never synced -> unsynchronised, and time from host clock.
    s0 = clock.reply()
    assert (s0.synced, s0.stratum, s0.leap) == (False, 16, 3)

    clock.record(make_sample(ft))
    s1 = clock.reply()
    assert (s1.synced, s1.stratum, s1.leap) == (True, 3, 0)

    # Expire, then take a fresh snapshot: unsynchronised again.
    ft.advance(100)
    s2 = clock.reply()
    assert (s2.synced, s2.stratum, s2.leap) == (False, 16, 3)
    assert s2.refid == b"STAL"


def test_restart_does_not_reuse_expired_history(tmp_path):
    ft = FakeTime()
    path = str(tmp_path / "history.json")
    history = HistoryStore(path)
    clock = make_clock(ft, holdover=30.0, history=history)
    clock.record(make_sample(ft))
    assert os.path.exists(path)
    del clock

    # New process: same file, fresh clock.  Must start unsynced.
    ft2 = FakeTime(wall=ft.wall, mono=0.0)
    clock2 = ClockState(
        holdover_seconds=30.0,
        wall_clock=ft2.time,
        monotonic=ft2.monotonic,
        history=HistoryStore(path),
    )
    snap = clock2.status()
    assert not snap.synced
    assert snap.state == "never-synced"
    assert snap.stratum == 16
    assert snap.refid == b"INIT"
    assert clock2.describe()["restored_history_samples"] == 1
    # and there is no confirmed time to project
    assert clock2.current_time() is None


def test_holdover_remaining_counts_down():
    ft = FakeTime()
    clock = make_clock(ft, holdover=40.0, fresh_seconds=2.0)
    clock.record(make_sample(ft))
    assert clock.status().holdover_remaining == pytest.approx(40.0, abs=0.01)
    ft.advance(15)
    assert clock.status().holdover_remaining == pytest.approx(25.0, abs=0.01)


def test_concurrent_samples_and_replies_are_sane():
    import threading

    ft = FakeTime()
    clock = make_clock(ft, holdover=300.0)
    errors = []

    def sampler():
        for _ in range(200):
            ft.advance(0.001)
            clock.record(make_sample(ft, source="a"))

    def replayer():
        last = 0.0
        for _ in range(400):
            snap = clock.reply()
            if snap.unix_time < last - 1e-9:
                errors.append("backwards")
            last = snap.unix_time
            if snap.synced and snap.stratum == 16:
                errors.append("synced but stratum 16")
            if not snap.synced and snap.stratum != 16:
                errors.append("unsynced but stratum != 16")

    threads = [threading.Thread(target=sampler), threading.Thread(target=replayer)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors[:5]
