"""Authoritative clock state for the NTP service.

Design goals, in order:

* The time handed to *clients* never moves backwards inside one process
  lifetime, regardless of source switches, upstream rollbacks, or the host
  wall clock being set backwards.
* The sync flag and the time value handed out are always computed from the
  same locked snapshot, so concurrent sampling cannot produce a packet whose
  timestamp belongs to one state and whose LI/stratum belongs to another.
* When upstream contact is lost, the clock free-runs only for a bounded
  ``holdover`` interval measured on the monotonic clock.  After that it
  reports unsynchronised (LI=3, stratum 16) and holds the time still instead
  of pretending to be a healthy server.
* Nothing read back from disk is ever used to serve time.  A fresh upstream
  confirmation is required after every restart.
"""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass
from typing import Callable

STRATUM_UNSYNC = 16
LI_UNSYNC = 3
LI_NONE = 0

# Accept samples this far behind the projected clock as normal jitter.  Larger
# backwards movement is a rollback and is rejected outright.
DEFAULT_ROLLBACK_TOLERANCE = 0.25
# Forward jumps larger than this are logged as steps (still accepted).
STEP_LOG_THRESHOLD = 1.0
# NTP dispersion growth (RFC 5905 PHI).
PHI = 15e-6


@dataclass(frozen=True)
class Sample:
    """A validated upstream measurement."""

    source_id: str
    source_ip: str
    source_port: int
    stratum: int
    leap: int
    upstream_ref_ts: float | None
    upstream_root_delay: float
    upstream_root_dispersion: float
    offset: float          # upstream clock minus local wall clock
    delay: float           # round trip delay
    true_time: float       # estimated upstream time at local_mono
    local_mono: float
    local_wall: float
    version: int = 4

    @property
    def root_distance(self) -> float:
        return self.delay / 2.0 + self.upstream_root_dispersion

    @property
    def source_refid(self) -> bytes:
        try:
            return b"".join(bytes([int(part)]) for part in self.source_ip.split(".")).ljust(4, b"\x00")[:4]
        except ValueError:
            return self.source_id.encode("ascii", "replace")[:4].ljust(4, b"\x00")


@dataclass(frozen=True)
class ClockSnapshot:
    """One consistent view of the clock, used for every NTP reply."""

    unix_time: float
    synced: bool
    holdover: bool              # still serving confirmed time, but contact lost
    holdover_remaining: float
    stratum: int
    leap: int
    refid: bytes
    ref_ts: float | None
    root_delay: float
    root_dispersion: float
    source_id: str | None
    source_ip: str | None
    source_port: int | None
    state: str                  # "synced" | "holdover-expired" | "never-synced"
    last_sample_age: float | None
    time_clamped: bool
    version: int
    mono: float = 0.0


@dataclass
class RecordResult:
    accepted: bool
    reason: str                 # "accepted" | "rollback"
    clamped: bool = False
    stepped: bool = False
    source_changed: bool = False


class EventLog:
    """Bounded, thread-safe event ring for the web UI."""

    def __init__(self, maxlen: int = 200) -> None:
        self._events: list[dict] = []
        self._maxlen = maxlen
        self._lock = threading.Lock()

    def add(self, kind: str, message: str, **extra) -> None:
        event = {"wall": time.time(), "kind": kind, "message": message, **extra}
        with self._lock:
            self._events.append(event)
            if len(self._events) > self._maxlen:
                del self._events[: len(self._events) - self._maxlen]

    def recent(self, limit: int = 50) -> list[dict]:
        with self._lock:
            return list(self._events[-limit:])[::-1]


class HistoryStore:
    """Append-only-ish JSON persistence of accepted samples for auditing.

    IMPORTANT: contents are *never* replayed into the live clock.  They exist
    so an operator can see what the service last confirmed before a restart.
    """

    def __init__(self, path: str | None) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._samples: list[dict] = []
        self._loaded = 0

    def load(self) -> int:
        if not self.path or not os.path.exists(self.path):
            return 0
        try:
            with open(self.path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            samples = data.get("samples", [])
            if not isinstance(samples, list):
                return 0
            self._samples = [s for s in samples if isinstance(s, dict)][-50:]
            self._loaded = len(self._samples)
        except (OSError, ValueError):
            self._samples = []
            self._loaded = 0
        return self._loaded

    def append(self, sample: Sample) -> None:
        entry = {
            "saved_wall": time.time(),
            "source_id": sample.source_id,
            "source": f"{sample.source_ip}:{sample.source_port}",
            "stratum": sample.stratum,
            "true_time": sample.true_time,
            "offset": sample.offset,
            "delay": sample.delay,
        }
        with self._lock:
            self._samples.append(entry)
            del self._samples[:-50]
            self._write_locked()

    def _write_locked(self) -> None:
        if not self.path:
            return
        payload = {
            "note": "audit history only; never replayed into the live clock on restart",
            "samples": self._samples,
        }
        tmp = f"{self.path}.tmp"
        try:
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.path)
        except OSError:
            pass  # persistence is best-effort and must never break serving

    def describe(self) -> dict:
        with self._lock:
            return {
                "path": self.path,
                "loaded_on_start": self._loaded,
                "samples": list(self._samples[-10:]),
                "replayed_into_clock": False,
            }


class ClockState:
    def __init__(
        self,
        *,
        holdover_seconds: float,
        fresh_seconds: float | None = None,
        rollback_tolerance: float = DEFAULT_ROLLBACK_TOLERANCE,
        wall_clock: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
        events: EventLog | None = None,
        history: HistoryStore | None = None,
    ) -> None:
        if holdover_seconds <= 0:
            raise ValueError("holdover_seconds must be positive")
        self.holdover_seconds = float(holdover_seconds)
        # A sample younger than this is considered "live contact".  Between
        # this and holdover_seconds the service is in explicit holdover.
        self.fresh_seconds = float(fresh_seconds) if fresh_seconds else max(
            3.0, min(self.holdover_seconds, 10.0)
        )
        self.rollback_tolerance = float(rollback_tolerance)
        self._wall = wall_clock
        self._mono = monotonic
        self.events = events or EventLog()
        self.history = history
        self._lock = threading.RLock()

        self._base_real: float | None = None   # confirmed time at _base_mono
        self._base_mono: float | None = None
        self._base_dispersion = 0.0
        self._base_root_delay = 0.0
        self._ref_ts: float | None = None
        self._source_id: str | None = None
        self._source_ip: str | None = None
        self._source_port: int | None = None
        self._stratum = STRATUM_UNSYNC
        self._leap = LI_UNSYNC
        self._refid = b"INIT"
        self._last_accept_mono: float | None = None
        self._last_contact_mono: float | None = None
        self._clamped = False
        self._version = 0
        self._last_emit: float | None = None

        self._restored = self.history.load() if self.history else 0
        if self._restored:
            self.events.add(
                "restart",
                f"loaded {self._restored} historical sample(s) for display only; "
                "clock starts unsynchronised",
            )

    # ------------------------------------------------------------------
    # helpers
    def _project_locked(self, mono_now: float) -> float | None:
        if self._base_real is None or self._base_mono is None:
            return None
        elapsed = mono_now - self._base_mono
        if elapsed < 0:
            elapsed = 0.0
        if elapsed > self.holdover_seconds:
            elapsed = self.holdover_seconds
        return self._base_real + elapsed

    def _age_locked(self, mono_now: float) -> float | None:
        if self._last_accept_mono is None:
            return None
        return max(0.0, mono_now - self._last_accept_mono)

    # ------------------------------------------------------------------
    # sampling
    def record(self, sample: Sample) -> RecordResult:
        """Fold one validated sample into the served timeline.

        Policy, in words:

        * The served timeline is ``base_real + (mono - base_mono)``, i.e. it
          advances on the monotonic clock and is immune to host wall-clock
          changes.
        * If the candidate is behind by more than ``rollback_tolerance``
          (plus RTT/2) it is rejected outright and logged as a rollback.  This
          applies to the active source rolling back *and* to a newly selected
          source that disagrees with the served timeline by more than network
          jitter.  When both upstreams are wrong in the same direction the
          service therefore freezes and eventually reports unsynchronised
          rather than following them backwards.
        * If the candidate is behind but within tolerance it is accepted, yet
          the served time is *held* (clamped), absorbing jitter without ever
          stepping backwards.
        * If the candidate is ahead, the timeline jumps forward (a step).
        """
        with self._lock:
            mono_now = sample.local_mono
            current = self._project_locked(mono_now)
            clamped = False
            stepped = False
            source_changed = self._source_id is not None and self._source_id != sample.source_id

            if current is not None:
                tolerance = max(self.rollback_tolerance, sample.delay / 2.0 + 0.01)
                behind = current - sample.true_time
                if behind > tolerance:
                    self._version += 1
                    rejected = RecordResult(
                        accepted=False,
                        reason="rollback",
                        source_changed=source_changed,
                    )
                    self.events.add(
                        "rollback",
                        f"rejected time from {sample.source_id}: candidate "
                        f"{sample.true_time:.3f} is {behind:.3f}s behind served time "
                        f"{current:.3f} (limit {tolerance:.3f}s)",
                        source=sample.source_id,
                        rejected_time=sample.true_time,
                        served_time=current,
                    )
                    return rejected
                if sample.true_time > current:
                    stepped = sample.true_time - current > STEP_LOG_THRESHOLD
                    self._base_real = sample.true_time
                else:
                    clamped = behind > 1e-6
                    self._base_real = current
            else:
                self._base_real = sample.true_time

            self._base_mono = mono_now
            self._base_dispersion = sample.upstream_root_dispersion
            self._base_root_delay = sample.upstream_root_delay + sample.delay
            self._ref_ts = sample.upstream_ref_ts or sample.true_time
            self._stratum = min(15, sample.stratum + 1) if sample.stratum < 15 else STRATUM_UNSYNC
            self._leap = sample.leap
            self._refid = sample.source_refid
            self._last_accept_mono = mono_now
            self._last_contact_mono = mono_now
            self._clamped = clamped
            self._version += 1

            if source_changed:
                self.events.add(
                    "source-switch",
                    f"serving time now sourced from {sample.source_id}",
                    source=sample.source_id,
                )
            self._source_id = sample.source_id
            self._source_ip = sample.source_ip
            self._source_port = sample.source_port

            if stepped:
                self.events.add(
                    "step-forward",
                    f"forward step to {self._base_real:.3f} from {sample.source_id}",
                    source=sample.source_id,
                )
            if clamped:
                self.events.add(
                    "clamp",
                    f"served time held at {current:.3f}; {sample.source_id} "
                    f"offered {sample.true_time:.3f}",
                    source=sample.source_id,
                )
            self.events.add(
                "sample",
                f"accepted {sample.source_id} offset={sample.offset:+.6f}s "
                f"delay={sample.delay * 1000:.3f}ms",
                source=sample.source_id,
            )
            if self.history is not None:
                self.history.append(sample)
            return RecordResult(
                accepted=True,
                reason="accepted",
                clamped=clamped,
                stepped=stepped,
                source_changed=source_changed,
            )

    # ------------------------------------------------------------------
    # reading
    def status(self) -> ClockSnapshot:
        return self._snapshot(record_emit=False)

    def reply(self) -> ClockSnapshot:
        """Snapshot for an outgoing NTP reply; guarantees non-decreasing tx."""
        return self._snapshot(record_emit=True)

    def _snapshot(self, *, record_emit: bool) -> ClockSnapshot:
        with self._lock:
            mono_now = self._mono()
            projected = self._project_locked(mono_now)
            age = self._age_locked(mono_now)

            if projected is None:
                # Never had a confirmed sample in this process.  There is no
                # confirmed time to advance, so report the host clock but mark
                # the reply unsynchronised.  The emit floor below still keeps
                # the transmitted timestamp from going backwards.
                state = "never-synced"
                synced = False
                holdover = False
                remaining = 0.0
                stratum, leap = STRATUM_UNSYNC, LI_UNSYNC
                refid = b"INIT"
                ref_ts = None
                disp, rd = 0.0, 0.0
                projected = self._wall()
            elif age is not None and age <= self.holdover_seconds:
                remaining = max(0.0, self.holdover_seconds - age)
                if age <= self.fresh_seconds:
                    state, holdover = "synced", False
                else:
                    state, holdover = "holdover", True
                synced = True
                stratum, leap = self._stratum, self._leap
                refid = self._refid
                ref_ts = self._ref_ts
                disp = self._base_dispersion + age * PHI + 0.0005
                rd = self._base_root_delay
            else:
                # Holdover window exhausted: the last confirmed time is frozen
                # at base + holdover_seconds and reported as unsynchronised.
                state = "holdover-expired"
                synced = False
                holdover = False
                remaining = 0.0
                stratum, leap = STRATUM_UNSYNC, LI_UNSYNC
                refid = b"STAL"
                ref_ts = None
                disp = self.holdover_seconds * PHI + 1.0
                rd = 0.0

            # Emit floor: the transmitted time never decreases within this
            # process.  The UI uses the same floor (without advancing it) so
            # what the operator sees matches what clients receive.  This only
            # ever raises an unsynchronised reading (e.g. host clock set
            # backwards before first sync); the LI/stratum flags still come
            # from the same snapshot, so the packet remains self-consistent.
            if self._last_emit is not None and projected < self._last_emit:
                projected = self._last_emit
            if record_emit:
                self._last_emit = projected
            return ClockSnapshot(
                unix_time=projected,
                synced=synced,
                holdover=holdover,
                holdover_remaining=remaining,
                stratum=stratum,
                leap=leap,
                refid=refid,
                ref_ts=ref_ts,
                root_delay=rd,
                root_dispersion=disp,
                source_id=self._source_id if synced else None,
                source_ip=self._source_ip if synced else None,
                source_port=self._source_port if synced else None,
                state=state,
                last_sample_age=age,
                time_clamped=self._clamped,
                version=self._version,
                mono=mono_now,
            )

    def current_time(self) -> float | None:
        with self._lock:
            return self._project_locked(self._mono())

    # ------------------------------------------------------------------
    # introspection for the web layer / tests
    def describe(self) -> dict:
        snap = self.status()
        return {
            "state": snap.state,
            "synced": snap.synced,
            "holdover": snap.holdover,
            "holdover_seconds": self.holdover_seconds,
            "fresh_seconds": self.fresh_seconds,
            "holdover_remaining": snap.holdover_remaining,
            "last_sample_age": snap.last_sample_age,
            "unix_time": snap.unix_time,
            "iso_time": _iso(snap.unix_time),
            "stratum": snap.stratum,
            "leap": snap.leap,
            "refid": _refid_text(snap.refid, snap.stratum),
            "ref_ts": snap.ref_ts,
            "root_delay": snap.root_delay,
            "root_dispersion": snap.root_dispersion,
            "source": (
                {"id": snap.source_id, "ip": snap.source_ip, "port": snap.source_port}
                if snap.source_id
                else None
            ),
            "time_clamped": snap.time_clamped,
            "state_version": snap.version,
            "restored_history_samples": self._restored,
            "history": self.history.describe() if self.history else None,
        }


def _iso(value: float | None) -> str | None:
    if value is None:
        return None
    import datetime as _dt

    return _dt.datetime.fromtimestamp(value, tz=_dt.timezone.utc).isoformat(timespec="milliseconds")


def _refid_text(refid: bytes, stratum: int) -> str:
    if stratum == STRATUM_UNSYNC:
        return refid.decode("ascii", "replace")
    if len(refid) == 4 and all(0 <= b < 256 for b in refid):
        try:
            return ".".join(str(b) for b in refid)
        except ValueError:
            pass
    return refid.hex()
