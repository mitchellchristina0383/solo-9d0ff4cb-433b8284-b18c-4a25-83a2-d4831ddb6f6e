"""Outbound NTP polling with strict reply ownership validation.

A reply is only accepted when *all* of the following hold:

* it arrives on the socket that is connected to the configured upstream, so
  the kernel already discarded datagrams from other addresses;
* the transport address equals the upstream we asked;
* the origin timestamp echoes the unpredictable 64-bit nonce we sent
  (byte-for-byte, so no re-encoding rounding games);
* it is a version 3/4 **server** packet with a sane stratum, leap
  indicator and root delay/dispersion;
* its timestamps are non-zero and the measured round-trip delay is not
  absurdly large;
* its timestamps are interpreted in the NTP era nearest the reference this
  probe asked for.  Without that, a source legitimately running past the
  2036 seconds-field rollover would decode as a negative/absent timestamp.

Only then is an offset computed and offered to the clock.
"""

from __future__ import annotations

import os
import socket
import time
from dataclasses import dataclass

from . import ntp_proto
from .clock import Sample


class UpstreamError(Exception):
    pass


@dataclass
class QueryOutcome:
    ok: bool
    sample: Sample | None = None
    error: str | None = None
    peer: str | None = None
    stratum: int | None = None
    leap: int | None = None
    root_delay: float | None = None
    root_dispersion: float | None = None
    kiss_code: str | None = None
    rtt: float | None = None
    roundtrip_checked_at: float | None = None
    reference_time: float | None = None
    ref_ts: float | None = None
    recv_ts: float | None = None
    transmit_ts: float | None = None


@dataclass(frozen=True)
class UpstreamConfig:
    id: str
    host: str
    port: int = 123
    label: str = ""


def _validate_peer(addr, expected_ip: str, expected_port: int) -> None:
    if addr is None:
        raise UpstreamError("reply has no source address")
    ip, port = addr[0], addr[1]
    if port != expected_port:
        raise UpstreamError(f"reply came from port {port}, expected {expected_port}")
    if ip != expected_ip:
        # Kernel connect() normally filters this; check anyway.
        raise UpstreamError(f"reply came from {ip}, expected {expected_ip}")


def query(
    cfg: UpstreamConfig,
    *,
    timeout: float = 2.0,
    version: int = 4,
    max_rtt: float = 1.5,
    reference_time: float | None = None,
    mono= time.monotonic,
    wall= time.time,
) -> QueryOutcome:
    """Send one NTP request and return a validated outcome.

    ``reference_time`` is the POSIX instant the response timestamps should
    be interpreted against.  It is supplied by the *diagnostic probe* (the
    operator's expected date, or the local send-time clock when left blank)
    and is resolved once for the whole exchange, so a reply whose receive
    and transmit straddle the 2036 seconds-field rollover still decodes both
    sides correctly.

    When it is ``None`` — the periodic sampler's path — timestamps are
    decoded with the historical single-era rule.  The probe's era guessing
    is a display aid; it must not silently turn a demo source set to 2036
    into a sample the clock would step forward by ten years.
    """
    reference = reference_time
    try:
        infos = socket.getaddrinfo(cfg.host, cfg.port, type=socket.SOCK_DGRAM)
    except OSError as exc:
        return QueryOutcome(ok=False, error=f"resolve failed: {exc}",
                            reference_time=reference)
    if not infos:
        return QueryOutcome(ok=False, error="resolve returned no addresses",
                            reference_time=reference)
    family, socktype, proto, _canon, sockaddr = infos[0]
    expected_ip, expected_port = sockaddr[0], sockaddr[1]

    nonce = int.from_bytes(os.urandom(8), "big")
    request = ntp_proto.client_request_nonce(nonce, version=version)
    sock = socket.socket(family, socktype, proto)
    sock.settimeout(timeout)
    try:
        # connect() makes the kernel drop datagrams from any other peer.
        sock.connect(sockaddr)

        t1_wall = wall()
        t1_mono = mono()
        sock.send(request)

        deadline = t1_mono + timeout
        mismatched = 0
        last_mismatch = ""
        while True:
            remaining = deadline - mono()
            if remaining <= 0:
                return QueryOutcome(ok=False, error="timeout waiting for reply",
                                    reference_time=reference)
            sock.settimeout(remaining)
            try:
                data, _addr = sock.recvfrom(1024)
            except socket.timeout:
                return QueryOutcome(ok=False, error="timeout waiting for reply",
                                    reference_time=reference)
            t4_mono = mono()
            t4_wall = wall()

            # A connected UDP socket only yields datagrams from the peer, but
            # verify the address we were told about anyway.
            try:
                peer_addr = sock.getpeername()
            except OSError:
                peer_addr = None
            if peer_addr is not None:
                _validate_peer(peer_addr, expected_ip, expected_port)

            if len(data) < ntp_proto.NTP_PACKET_SIZE:
                # Ignore stray runt datagrams too and keep waiting.
                mismatched += 1
                last_mismatch = f"short datagram ({len(data)} bytes)"
                if mono() >= deadline:
                    return QueryOutcome(ok=False, peer=expected_ip, error=last_mismatch,
                                        reference_time=reference)
                continue
            try:
                origin = ntp_proto.origin_bits(data)
            except ntp_proto.NtpFormatError as exc:
                return QueryOutcome(ok=False, peer=expected_ip, error=str(exc),
                                    reference_time=reference)

            if origin != nonce:
                # Some other packet (or a spoofed/duplicate reply).  Ignore it
                # and keep waiting for the matching one within the deadline.
                mismatched += 1
                last_mismatch = (
                    "origin timestamp mismatch "
                    f"(got {origin:016x}, expected {nonce:016x})"
                )
                if mono() >= deadline:
                    return QueryOutcome(ok=False, peer=expected_ip, error=last_mismatch,
                                        reference_time=reference)
                continue

            break

        try:
            packet = ntp_proto.decode(data, reference=reference)
        except ntp_proto.NtpFormatError as exc:
            return QueryOutcome(ok=False, peer=expected_ip, error=str(exc),
                                reference_time=reference)

        usable, problems = packet.is_usable()
        kod = ntp_proto.kiss_code(packet)
        stamps = {
            "reference_time": reference,
            "ref_ts": packet.ref_ts,
            "recv_ts": packet.recv_ts,
            "transmit_ts": packet.transmit_ts,
        }
        if kod:
            return QueryOutcome(
                ok=False,
                peer=expected_ip,
                stratum=packet.stratum,
                leap=packet.leap,
                root_delay=packet.root_delay,
                root_dispersion=packet.root_dispersion,
                error=f"kiss-o'-death {kod}",
                kiss_code=kod,
                **stamps,
            )
        if not usable:
            return QueryOutcome(ok=False, peer=expected_ip,
                                error="unusable reply: " + "; ".join(problems),
                                stratum=packet.stratum, leap=packet.leap,
                                root_delay=packet.root_delay,
                                root_dispersion=packet.root_dispersion,
                                **stamps)
        if packet.recv_ts is None or packet.transmit_ts <= 0 or packet.ref_ts is None:
            return QueryOutcome(ok=False, peer=expected_ip,
                                error="reply is missing receive/transmit timestamps",
                                **stamps)

        t2 = packet.recv_ts
        t3 = packet.transmit_ts
        delay = (t4_mono - t1_mono) - (t3 - t2)
        if delay < -0.05:
            return QueryOutcome(ok=False, peer=expected_ip,
                                error=f"negative round-trip delay {delay*1000:.3f}ms", **stamps)
        delay = max(0.0, delay)
        if delay > max_rtt:
            return QueryOutcome(ok=False, peer=expected_ip,
                                error=f"round-trip delay {delay:.3f}s exceeds {max_rtt}s", **stamps)
        # Standard NTP offset: ((T2 - T1) + (T3 - T4)) / 2.  T1/T4 come from
        # the host wall clock and cancel each other; only relative run across
        # the few-ms exchange matters, so a wrong host clock does not poison
        # the measurement.  Upstream time at the t4 instant is t4 + offset.
        offset = ((t2 - t1_wall) + (t3 - t4_wall)) / 2.0
        true_time = t4_wall + offset
        sample = Sample(
            source_id=cfg.id,
            source_ip=expected_ip,
            source_port=expected_port,
            stratum=packet.stratum,
            leap=packet.leap,
            upstream_ref_ts=packet.ref_ts,
            upstream_root_delay=packet.root_delay,
            upstream_root_dispersion=packet.root_dispersion,
            offset=offset,
            delay=delay,
            true_time=true_time,
            local_mono=t4_mono,
            local_wall=t4_wall,
            version=packet.version,
        )
        return QueryOutcome(
            ok=True,
            sample=sample,
            peer=expected_ip,
            stratum=packet.stratum,
            leap=packet.leap,
            root_delay=packet.root_delay,
            root_dispersion=packet.root_dispersion,
            rtt=delay,
            roundtrip_checked_at=wall(),
            **stamps,
        )
    except UpstreamError as exc:
        return QueryOutcome(ok=False, error=str(exc), peer=expected_ip,
                            reference_time=reference)
    except OSError as exc:
        return QueryOutcome(ok=False, error=f"socket error: {exc}", peer=expected_ip,
                            reference_time=reference)
    finally:
        sock.close()
