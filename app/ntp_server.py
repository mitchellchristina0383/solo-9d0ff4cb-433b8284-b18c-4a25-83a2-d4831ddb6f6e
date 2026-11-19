"""UDP NTPv4 responder for the isolated network.

Each reply is built from exactly one :meth:`ClockState.reply` snapshot, so
the transmit timestamp, reference timestamps, stratum and leap indicator in a
packet are internally consistent even if a sample is being recorded at the
same instant.
"""

from __future__ import annotations

import socket
import threading

from . import ntp_proto
from .clock import ClockState, EventLog


class NtpServer:
    def __init__(
        self,
        clock: ClockState,
        *,
        host: str = "0.0.0.0",
        port: int = 123,
        events: EventLog | None = None,
        max_workers: int = 16,
    ) -> None:
        self.clock = clock
        self.host = host
        self.port = port
        self.events = events or clock.events
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._sock: socket.socket | None = None
        self.requests = 0
        self.replies = 0
        self.rejected = 0
        self._lock = threading.Lock()
        self._max_workers = max_workers
        self._sem = threading.BoundedSemaphore(max_workers)

    def start(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.host, self.port))
        sock.settimeout(0.5)
        self._sock = sock
        self._thread = threading.Thread(target=self._serve, name="ntp-udp", daemon=True)
        self._thread.start()
        self.events.add("ntp-listen", f"NTP responder listening on {self.host}:{self.port}/udp")

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass

    def _serve(self) -> None:
        assert self._sock is not None
        while not self._stop.is_set():
            try:
                data, addr = self._sock.recvfrom(2048)
            except socket.timeout:
                continue
            except OSError:
                break
            # Cheap amplification guard: a single worker thread handles each
            # datagram; excess concurrency simply drops.
            if not self._sem.acquire(blocking=False):
                with self._lock:
                    self.rejected += 1
                continue
            threading.Thread(
                target=self._handle_and_release, args=(data, addr), daemon=True
            ).start()

    def _handle_and_release(self, data: bytes, addr) -> None:
        try:
            self._handle(data, addr)
        finally:
            self._sem.release()

    def _handle(self, data: bytes, addr) -> None:
        with self._lock:
            self.requests += 1
        if len(data) < ntp_proto.NTP_PACKET_SIZE:
            with self._lock:
                self.rejected += 1
            return
        try:
            packet = ntp_proto.decode(data)
        except ntp_proto.NtpFormatError:
            with self._lock:
                self.rejected += 1
            return
        if packet.mode != ntp_proto.MODE_CLIENT:
            # Never answer server/broadcast packets; avoids reflection loops.
            with self._lock:
                self.rejected += 1
            return
        version = packet.version if packet.version in (3, 4) else 4

        # One snapshot for both the payload and the sync indicators.
        snap = self.clock.reply()

        root_delay = snap.root_delay if snap.synced else 0.0
        root_dispersion = snap.root_dispersion if snap.synced else max(snap.root_dispersion, 1.0)
        refid = snap.refid if snap.synced else b"STAL"
        reply = ntp_proto.encode(
            leap=snap.leap,
            version=version,
            mode=ntp_proto.MODE_SERVER,
            stratum=snap.stratum,
            poll=packet.poll,
            precision=-20,
            root_delay=root_delay,
            root_dispersion=root_dispersion,
            ref_id=refid,
            ref_ts=snap.ref_ts,
            origin_ts=None,
            # Echo the client's transmit field bit-for-bit; re-encoding a
            # decoded float would not round-trip exactly.
            origin_bits=ntp_proto.transmit_bits(data),
            recv_ts=snap.unix_time,
            transmit_ts=snap.unix_time,
        )
        try:
            assert self._sock is not None
            self._sock.sendto(reply, addr)
        except OSError:
            return
        with self._lock:
            self.replies += 1

    @property
    def bound_port(self) -> int | None:
        if self._sock is None:
            return None
        try:
            return self._sock.getsockname()[1]
        except OSError:
            return None

    def describe(self) -> dict:
        with self._lock:
            return {
                "host": self.host,
                "port": self.bound_port if self.bound_port is not None else self.port,
                "requests": self.requests,
                "replies": self.replies,
                "rejected": self.rejected,
            }
