"""Minimal, strictly validated NTPv4 client/server packet codec.

Everything here is deliberately dependency free so it can be unit tested
without any I/O.  The parser is strict: a response is only accepted when it
is a *server* packet (mode 4), has a matching origin timestamp, a sane
stratum, and valid timestamps.  A bare UDP reply from the right address is
never trusted on its own.
"""

from __future__ import annotations

import math
import struct
from dataclasses import dataclass

NTP_EPOCH_OFFSET = 2_208_988_800  # seconds between 1900-01-01 and 1970-01-01
NTP_PACKET_SIZE = 48
# The 32-bit NTP seconds field wraps every 2**32 seconds (~136 years); the
# current era ends at 2036-02-07 06:28:16 UTC.
NTP_ERA_SECONDS = 1 << 32
NTP_WRAP_UNIX = NTP_ERA_SECONDS - NTP_EPOCH_OFFSET  # 2036-02-07 06:28:16 UTC

# mode values
MODE_CLIENT = 3
MODE_SERVER = 4

# stratum values of interest
STRATUM_UNSYNC = 0      # kiss-o'-death / unspecified
STRATUM_PRIMARY = 1
STRATUM_MAX = 15
STRATUM_UNSYNCED = 16   # per RFC 5905, "unsynchronised"


class NtpFormatError(ValueError):
    """Raised when a datagram is not a well-formed NTP packet."""


@dataclass(frozen=True)
class NtpPacket:
    leap: int
    version: int
    mode: int
    stratum: int
    poll: int
    precision: int
    root_delay: float
    root_dispersion: float
    ref_id: bytes
    ref_ts: float | None
    origin_ts: float | None
    recv_ts: float | None
    transmit_ts: float
    raw: bytes
    # Raw wire fields, before any era/epoch interpretation.
    ref_bits: int = 0
    origin_bits: int = 0
    recv_bits: int = 0
    transmit_bits: int = 0

    @property
    def root_delay_s(self) -> float:
        return self.root_delay

    @property
    def root_dispersion_s(self) -> float:
        return self.root_dispersion

    def sanity_problems(self) -> list[str]:
        """Return a list of reasons this packet must not be trusted."""
        problems: list[str] = []
        if self.mode != MODE_SERVER:
            problems.append(f"mode {self.mode} is not server (4)")
        if self.version not in (3, 4):
            problems.append(f"unsupported version {self.version}")
        if self.stratum == STRATUM_UNSYNC or self.stratum >= STRATUM_UNSYNCED:
            problems.append(f"stratum {self.stratum} indicates unsynchronised")
        elif self.stratum > STRATUM_MAX:
            problems.append(f"stratum {self.stratum} out of range")
        if self.transmit_ts <= 0:
            problems.append("zero transmit timestamp")
        if self.recv_ts is not None and self.recv_ts <= 0:
            problems.append("zero receive timestamp")
        if self.ref_ts is not None and self.ref_ts <= 0:
            problems.append("zero reference timestamp")
        if self.leap == 3:
            problems.append("leap indicator 3 (unsynchronised)")
        if not (0.0 <= self.root_delay < 16.0):
            problems.append(f"root delay {self.root_delay} out of range")
        if not (0.0 <= self.root_dispersion < 16.0):
            problems.append(f"root dispersion {self.root_dispersion} out of range")
        return problems

    def is_usable(self) -> tuple[bool, list[str]]:
        problems = self.sanity_problems()
        return (not problems, problems)


def _to_ntp(ts: float) -> int:
    """Convert a POSIX timestamp to a 64-bit NTP timestamp."""
    seconds = int(ts) + NTP_EPOCH_OFFSET
    fraction = int(round((ts - int(ts)) * (1 << 32)))
    if fraction >= 1 << 32:
        seconds += 1
        fraction -= 1 << 32
    return ((seconds & 0xFFFFFFFF) << 32) | (fraction & 0xFFFFFFFF)


def _from_ntp(value: int) -> float | None:
    if value == 0:
        return None
    seconds = (value >> 32) & 0xFFFFFFFF
    fraction = value & 0xFFFFFFFF
    return seconds - NTP_EPOCH_OFFSET + fraction / (1 << 32)


def _from_ntp_near(value: int, reference: float) -> float | None:
    """Decode a timestamp near ``reference``, resolving era wrap.

    The NTP seconds field is only 32 bits, so every raw value recurs once
    per 136-year era.  We choose the era whose instant is nearest to the
    reference (the local send time of this probe).  That keeps the three
    server timestamps in one consistent era *and* still decodes correctly
    when, say, receive is just before the rollover and transmit just after.
    """
    if value == 0:
        return None
    raw_seconds = (value >> 32) & 0xFFFFFFFF
    fraction = value & 0xFFFFFFFF
    # Reference unix time -> NTP seconds; pick the near-integer era shift.
    era = int(math.floor(((reference + NTP_EPOCH_OFFSET) - raw_seconds) / NTP_ERA_SECONDS + 0.5))
    return raw_seconds - NTP_EPOCH_OFFSET + era * NTP_ERA_SECONDS + fraction / (1 << 32)


def era_for(ts: float) -> int:
    """Number of 2**32-second wraps in a POSIX timestamp (1900-based)."""
    return int(math.floor((ts + NTP_EPOCH_OFFSET) / NTP_ERA_SECONDS))


def _short_float(value: int) -> float:
    return (value >> 16) + (value & 0xFFFF) / 65536.0


def _short_float_bits(seconds: float) -> int:
    if seconds < 0:
        seconds = 0.0
    fixed = int(round(seconds * 65536.0))
    if fixed > 0xFFFFFFFF:
        fixed = 0xFFFFFFFF
    return fixed


def decode(data: bytes, *, reference: float | None = None) -> NtpPacket:
    """Parse a datagram.

    ``reference`` is the POSIX time this packet is interpreted around (the
    local wall clock at send time for an outbound probe).  When given, the
    server timestamps are resolved to the era nearest that instant, which is
    what makes replies still correct across the 2036 seconds-field rollover.
    Without it the legacy era-0 interpretation is kept (used for requests,
    whose only meaningful field is the echoed origin bits).
    """
    if len(data) < NTP_PACKET_SIZE:
        raise NtpFormatError(f"short packet ({len(data)} bytes)")
    (first,) = struct.unpack_from("!B", data, 0)
    leap = (first >> 6) & 0x3
    version = (first >> 3) & 0x7
    mode = first & 0x7
    stratum, poll, precision = struct.unpack_from("!BBb", data, 1)  # poll read unsigned, precision signed
    root_delay, root_dispersion, ref_id = struct.unpack_from("!II4s", data, 4)
    ref_bits, origin_bits, recv_bits, transmit_bits = struct.unpack_from("!QQQQ", data, 16)
    if reference is None:
        ref_ts = _from_ntp(ref_bits)
        origin_ts = _from_ntp(origin_bits)
        recv_ts = _from_ntp(recv_bits)
        transmit_ts = _from_ntp(transmit_bits) or 0.0
    else:
        ref_ts = _from_ntp_near(ref_bits, reference)
        origin_ts = _from_ntp_near(origin_bits, reference)
        recv_ts = _from_ntp_near(recv_bits, reference)
        transmit_ts = _from_ntp_near(transmit_bits, reference) or 0.0
    return NtpPacket(
        leap=leap,
        version=version,
        mode=mode,
        stratum=stratum,
        poll=poll,
        precision=precision,
        root_delay=_short_float(root_delay),
        root_dispersion=_short_float(root_dispersion),
        ref_id=ref_id,
        ref_ts=ref_ts,
        origin_ts=origin_ts,
        recv_ts=recv_ts,
        transmit_ts=transmit_ts,
        raw=data[:NTP_PACKET_SIZE],
        ref_bits=ref_bits,
        origin_bits=origin_bits,
        recv_bits=recv_bits,
        transmit_bits=transmit_bits,
    )


def encode(
    *,
    leap: int = 0,
    version: int = 4,
    mode: int = MODE_SERVER,
    stratum: int,
    poll: int,
    precision: int,
    root_delay: float,
    root_dispersion: float,
    ref_id: bytes,
    ref_ts: float | None,
    origin_ts: float | None,
    recv_ts: float | None,
    transmit_ts: float,
    origin_bits: int | None = None,
) -> bytes:
    first = ((leap & 0x3) << 6) | ((version & 0x7) << 3) | (mode & 0x7)
    header = struct.pack(
        "!BBbbII4s",
        first,
        stratum & 0xFF,
        poll & 0xFF,
        int(precision),
        _short_float_bits(root_delay),
        _short_float_bits(root_dispersion),
        ref_id[:4].ljust(4, b"\x00"),
    )
    if origin_bits is None:
        origin_bits = _to_ntp(origin_ts) if origin_ts else 0
    stamps = struct.pack(
        "!QQQQ",
        _to_ntp(ref_ts) if ref_ts else 0,
        origin_bits & 0xFFFFFFFFFFFFFFFF,
        _to_ntp(recv_ts) if recv_ts else 0,
        _to_ntp(transmit_ts),
    )
    return header + stamps


def client_request(transmit_ts: float, version: int = 4) -> bytes:
    return _client_request_raw(_to_ntp(transmit_ts), version=version)


def client_request_nonce(nonce: int, version: int = 4) -> bytes:
    """Build a client request whose transmit field is a random nonce.

    RFC 5905 servers copy the client transmit timestamp into the origin
    field of the reply.  Using an unpredictable 64-bit nonce there and
    comparing the origin field byte-for-byte makes off-path spoofing
    substantially harder than matching a predictable timestamp.
    """
    return _client_request_raw(nonce & 0xFFFFFFFFFFFFFFFF, version=version)


def _client_request_raw(transmit_bits: int, version: int = 4) -> bytes:
    first = ((version & 0x7) << 3) | MODE_CLIENT
    header = struct.pack(
        "!BBbbII4s",
        first,
        0,
        3,
        0,
        0,
        0,
        b"\x00\x00\x00\x00",
    )
    return header + struct.pack("!QQQQ", 0, 0, 0, transmit_bits)


def kiss_code(packet: NtpPacket) -> str | None:
    """Return the kiss-o'-death code when the packet is a KoD."""
    if packet.stratum == 0 and packet.mode == MODE_SERVER:
        raw = packet.ref_id.rstrip(b"\x00")
        try:
            return raw.decode("ascii")
        except UnicodeDecodeError:
            return None
    return None


# Public aliases used by both the daemon and the local upstream.
to_ntp_bits = _to_ntp
short_float_bits = _short_float_bits
from_ntp_bits = _from_ntp


def origin_bits(data: bytes) -> int:
    """Raw 64-bit origin field of a datagram (no epoch conversion)."""
    return _field_bits(data, 24)


def transmit_bits(data: bytes) -> int:
    """Raw 64-bit transmit field of a datagram (no epoch conversion)."""
    return _field_bits(data, 40)


def _field_bits(data: bytes, offset: int) -> int:
    if len(data) < NTP_PACKET_SIZE:
        raise NtpFormatError("short packet")
    return struct.unpack_from("!Q", data, offset)[0]
