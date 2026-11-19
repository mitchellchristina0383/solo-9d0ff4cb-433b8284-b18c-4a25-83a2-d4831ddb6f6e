"""A real UDP NTP responder used by tests and the self-test scenario."""

from __future__ import annotations

import socket
import threading
import time

from app import ntp_proto


class FakeUpstream:
    """A UDP responder with a settable clock offset and failure modes.

    modes:
      ok          answer honestly
      silent      receive but never answer (simulates dead uplink)
      liar        answer with LI=3 / stratum 16 (unsynchronised)
      bad_origin  echo the wrong origin timestamp (spoof/wrong-reply)
      bad_mode    answer with mode 3 (client) instead of server
    """

    def __init__(self, port: int = 0, offset: float = 0.0, host: str = "127.0.0.1") -> None:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((host, port))
        self.addr = self.sock.getsockname()
        self.offset = offset
        self.mode = "ok"
        self.queries = 0
        self.lock = threading.Lock()
        self._stop = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def now(self) -> float:
        return time.time() + self.offset

    def _run(self) -> None:
        self.sock.settimeout(0.1)
        while not self._stop.is_set():
            try:
                data, peer = self.sock.recvfrom(1024)
            except (TimeoutError, OSError):
                continue
            with self.lock:
                self.queries += 1
                mode, offset = self.mode, self.offset
            if mode == "silent" or len(data) < 48:
                continue
            try:
                req = ntp_proto.decode(data)
            except ntp_proto.NtpFormatError:
                continue
            reply_mode = ntp_proto.MODE_CLIENT if mode == "bad_mode" else ntp_proto.MODE_SERVER
            now = time.time() + offset
            origin = ntp_proto.transmit_bits(data)
            if mode == "bad_origin":
                origin ^= 1
            stratum = 16 if mode == "liar" else 2
            leap = 3 if mode == "liar" else 0
            reply = ntp_proto.encode(
                leap=leap, version=4, mode=reply_mode, stratum=stratum,
                poll=req.poll, precision=-20, root_delay=0.001,
                root_dispersion=0.002, ref_id=b"TEST",
                ref_ts=now - 10, origin_ts=None, origin_bits=origin,
                recv_ts=now, transmit_ts=now + 0.0001,
            )
            try:
                self.sock.sendto(reply, peer)
            except OSError:
                pass

    def close(self) -> None:
        self._stop.set()
        self.thread.join(timeout=1)
        self.sock.close()
