"""Periodic sampling, source selection and failover orchestration."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass

from . import upstream
from .clock import ClockState, EventLog, Sample
from .upstream import QueryOutcome, UpstreamConfig


@dataclass
class SourceHealth:
    id: str
    label: str
    host: str
    port: int
    enabled: bool = True
    reachable: bool = False
    last_error: str | None = None
    last_attempt_mono: float | None = None
    last_success_mono: float | None = None
    last_success_wall: float | None = None
    stratum: int | None = None
    root_delay: float | None = None
    root_dispersion: float | None = None
    rtt: float | None = None
    consecutive_failures: int = 0
    trusted: bool = False
    last_rejection: str | None = None

    def to_dict(self, mono_now: float) -> dict:
        age = None if self.last_success_mono is None else max(0.0, mono_now - self.last_success_mono)
        return {
            "id": self.id,
            "label": self.label,
            "address": f"{self.host}:{self.port}",
            "enabled": self.enabled,
            "reachable": self.reachable,
            "trusted": self.trusted,
            "last_error": self.last_error,
            "last_rejection": self.last_rejection,
            "stratum": self.stratum,
            "root_delay": self.root_delay,
            "root_dispersion": self.root_dispersion,
            "rtt": self.rtt,
            "consecutive_failures": self.consecutive_failures,
            "last_success_age": age,
            "last_success_wall": self.last_success_wall,
        }


class Sampler:
    """Poll all upstreams; prefer the best (lowest stratum, then RTT).

    The active source is only *changed* after the new source has produced a
    validated sample.  A source that starts failing is no longer a candidate,
    but the previous confirmed time keeps being served from the monotonic
    clock until the holdover deadline, so switches never move time backwards.
    """

    def __init__(
        self,
        sources: list[UpstreamConfig],
        clock: ClockState,
        *,
        interval: float = 4.0,
        timeout: float = 2.0,
        events: EventLog | None = None,
        query_fn=None,
    ) -> None:
        self.sources = sources
        self.clock = clock
        self.interval = interval
        self.timeout = timeout
        self.events = events or clock.events
        self._query = query_fn or upstream.query
        self._lock = threading.RLock()
        self._health: dict[str, SourceHealth] = {
            s.id: SourceHealth(id=s.id, label=s.label or s.id, host=s.host, port=s.port)
            for s in sources
        }
        self._active: str | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._paused = False
        self._last_cycle: float | None = None
        self._poll_lock = threading.Lock()

    # ----------------------------------------------------------- lifecycle
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="ntp-sampler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=self.timeout + 1)

    def _run(self) -> None:
        # First cycle immediately so a fresh start syncs promptly.
        while not self._stop.is_set():
            if not self._paused:
                try:
                    self.poll_once()
                except Exception as exc:  # never let the sampler die
                    self.events.add("sampler-error", f"sampling cycle failed: {exc}")
            self._stop.wait(self.interval)

    # ------------------------------------------------------------- control
    def set_enabled(self, source_id: str, enabled: bool) -> bool:
        with self._lock:
            health = self._health.get(source_id)
            if health is None:
                return False
            health.enabled = enabled
            if not enabled:
                health.reachable = False
                self.events.add("source-disable", f"{source_id} disabled via web UI")
                if self._active == source_id:
                    self.events.add(
                        "source-drop",
                        f"active source {source_id} disabled; serving confirmed "
                        "time until another source is validated",
                    )
            else:
                self.events.add("source-enable", f"{source_id} enabled via web UI")
        self.poll_once()
        return True

    def set_paused(self, paused: bool) -> None:
        with self._lock:
            self._paused = paused
            self.events.add(
                "sampler-paused" if paused else "sampler-resumed",
                "automatic sampling paused" if paused else "automatic sampling resumed",
            )

    @property
    def paused(self) -> bool:
        return self._paused

    # ------------------------------------------------------------- polling
    def poll_once(self) -> None:
        # The scheduler and a manual "refresh" must not overlap: overlapping
        # cycles would query upstreams twice and can confuse health counters.
        if not self._poll_lock.acquire(blocking=False):
            return
        try:
            self._poll_once_locked()
        finally:
            self._poll_lock.release()

    def _poll_once_locked(self) -> None:
        with self._lock:
            snapshot_sources = [s for s in self.sources]
            health_before = {k: v.enabled for k, v in self._health.items()}

        outcomes: dict[str, QueryOutcome] = {}
        for cfg in snapshot_sources:
            if not health_before.get(cfg.id, True):
                continue
            # Query outside the lock so web requests are never blocked.
            outcomes[cfg.id] = self._query(cfg, timeout=self.timeout)

        mono_now = time.monotonic()
        accepted: list[Sample] = []
        with self._lock:
            for cfg in snapshot_sources:
                health = self._health[cfg.id]
                if not health.enabled:
                    continue
                outcome = outcomes.get(cfg.id)
                health.last_attempt_mono = mono_now
                if outcome is None:
                    continue
                if outcome.ok and outcome.sample is not None:
                    health.reachable = True
                    health.last_error = None
                    health.last_success_mono = outcome.sample.local_mono
                    health.last_success_wall = outcome.roundtrip_checked_at
                    health.stratum = outcome.stratum
                    health.root_delay = outcome.root_delay
                    health.root_dispersion = outcome.root_dispersion
                    health.rtt = outcome.rtt
                    health.consecutive_failures = 0
                    health.trusted = True
                    accepted.append(outcome.sample)
                else:
                    health.reachable = False
                    health.consecutive_failures += 1
                    health.last_error = outcome.error
                    health.trusted = False
                    if outcome.error and "mismatch" in outcome.error:
                        health.last_rejection = outcome.error

            # Sort: best stratum first, then lowest RTT.
            accepted.sort(key=lambda s: (s.stratum, s.delay))
            for sample in accepted:
                result = self.clock.record(sample)
                if result.accepted:
                    if self._active != sample.source_id:
                        self._active = sample.source_id
                        self.events.add(
                            "active-source",
                            f"active source is now {sample.source_id} "
                            f"(stratum {sample.stratum})",
                        )
                    break
                else:
                    health = self._health[sample.source_id]
                    health.last_rejection = result.reason
                    health.trusted = False

            self._last_cycle = mono_now

    # -------------------------------------------------------------- report
    def describe(self) -> dict:
        with self._lock:
            mono_now = time.monotonic()
            active_enabled = (
                self._health[self._active].enabled
                if self._active is not None and self._active in self._health
                else None
            )
            return {
                "active_source": self._active,
                "active_source_enabled": active_enabled,
                "paused": self._paused,
                "interval": self.interval,
                "timeout": self.timeout,
                "last_cycle_age": (
                    None if self._last_cycle is None else max(0.0, mono_now - self._last_cycle)
                ),
                "sources": [self._health[s.id].to_dict(mono_now) for s in self.sources],
            }
