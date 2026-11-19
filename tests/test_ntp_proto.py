import struct
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from app import ntp_proto as p


def server_packet(**over):
    kwargs = dict(
        stratum=2, poll=3, precision=-20,
        root_delay=0.001, root_dispersion=0.002,
        ref_id=b"LOCL",
        ref_ts=1_700_000_000.0,
        origin_ts=1_700_000_001.0,
        recv_ts=1_700_000_002.0,
        transmit_ts=1_700_000_003.0,
    )
    kwargs.update(over)
    return p.encode(**kwargs)


def test_roundtrip_server_packet_is_usable():
    pk = p.decode(server_packet())
    assert pk.mode == p.MODE_SERVER
    assert pk.version == 4
    assert pk.stratum == 2
    ok, problems = pk.is_usable()
    assert ok, problems
    assert pk.transmit_ts == pytest.approx(1_700_000_003.0, abs=1e-6)


def test_client_request_nonce_lands_in_transmit_field():
    nonce = 0xDEADBEEF12345678
    req = p.client_request_nonce(nonce)
    assert len(req) == 48
    assert struct.unpack_from("!Q", req, 40)[0] == nonce
    pk = p.decode(req)
    assert pk.mode == p.MODE_CLIENT
    assert pk.version == 4


def test_origin_bits_is_raw_not_epoch_converted():
    req = p.client_request_nonce(1)
    assert p.origin_bits(req) == 0
    # origin field begins at byte 24
    assert struct.unpack_from("!Q", req, 24)[0] == 0


@pytest.mark.parametrize(
    "over,needle",
    [
        ({"stratum": 0}, "unsynchronised"),
        ({"stratum": 16}, "unsynchronised"),
        ({"leap": 3}, "leap indicator 3"),
        ({"transmit_ts": 0.0}, "zero transmit"),
    ],
)
def test_unusable_packets_are_rejected(over, needle):
    pk = p.decode(server_packet(**over))
    ok, problems = pk.is_usable()
    assert not ok
    assert any(needle in item for item in problems), problems


def test_non_server_mode_rejected():
    pk = p.decode(p.client_request_nonce(5))
    ok, problems = pk.is_usable()
    assert not ok
    assert any("not server" in item for item in problems)


def test_short_packet_raises():
    with pytest.raises(p.NtpFormatError):
        p.decode(b"\x00" * 20)


def test_kiss_code_extraction():
    raw = bytearray(server_packet())
    raw[1] = 0  # stratum 0 == KoD
    raw[12:16] = b"RATE"
    pk = p.decode(bytes(raw))
    assert p.kiss_code(pk) == "RATE"


WRAP = p.NTP_WRAP_UNIX  # 2036-02-07 06:28:16 UTC


def _packet_at(ref_t, *, recv, transmit, origin=0):
    """A valid server packet whose receive/transmit are explicit instants."""
    return p.encode(
        stratum=2, poll=3, precision=-20,
        root_delay=0.001, root_dispersion=0.002, ref_id=b"WRAP",
        ref_ts=ref_t, origin_ts=None, origin_bits=origin,
        recv_ts=recv, transmit_ts=transmit,
    )


def test_wrap_constant_is_2036_rollover():
    import datetime as dt

    assert dt.datetime.fromtimestamp(WRAP, tz=dt.timezone.utc).replace(tzinfo=None) == dt.datetime(
        2036, 2, 7, 6, 28, 16
    )


def test_pre_wrap_timestamps_decode_with_reference():
    t = WRAP - 7200
    pk = p.decode(_packet_at(t - 10, recv=t, transmit=t + 0.0001), reference=t)
    assert pk.recv_ts == pytest.approx(t, abs=1e-6)
    assert pk.transmit_ts == pytest.approx(t + 0.0001, abs=1e-6)
    assert pk.ref_ts == pytest.approx(t - 10, abs=1e-6)


def test_post_wrap_timestamps_decode_with_reference():
    """Seconds field is near zero after the rollover; must land in 2036."""
    t = WRAP + 7200
    pk = p.decode(_packet_at(t - 10, recv=t, transmit=t + 0.0001), reference=t)
    assert pk.recv_ts == pytest.approx(t, abs=1e-6)
    assert pk.transmit_ts == pytest.approx(t + 0.0001, abs=1e-6)
    # Without a reference the legacy era-0 reading is negative (pre-1970).
    legacy = p.decode(_packet_at(t - 10, recv=t, transmit=t + 0.0001))
    assert legacy.recv_ts < 0


def test_timestamps_straddling_the_wrap_decode_correctly():
    """Receive just before the rollover, transmit just after."""
    recv = WRAP - 0.00005
    transmit = WRAP + 0.00005
    data = _packet_at(WRAP - 10, recv=recv, transmit=transmit)
    # Sanity: the raw seconds fields really did wrap between the two fields.
    assert (p._field_bits(data, 32) >> 32) == 0xFFFFFFFF
    assert (p._field_bits(data, 40) >> 32) == 0
    pk = p.decode(data, reference=WRAP)
    assert pk.recv_ts == pytest.approx(recv, abs=1e-6)
    assert pk.transmit_ts == pytest.approx(transmit, abs=1e-6)
    assert pk.transmit_ts - pk.recv_ts == pytest.approx(0.0001, abs=1e-6)


def test_missing_timestamps_stay_missing_even_with_reference():
    raw = bytearray(_packet_at(WRAP, recv=WRAP, transmit=WRAP + 0.0001))
    raw[32:40] = b"\x00" * 8  # receive field cleared
    pk = p.decode(bytes(raw), reference=WRAP)
    assert pk.recv_ts is None
    # Era resolution must not invent a timestamp for a zero field.  The
    # probe client rejects this explicitly (see test_integration).
    assert pk.recv_bits == 0


def test_era_for():
    assert p.era_for(p.NTP_WRAP_UNIX - 1) == 0
    assert p.era_for(p.NTP_WRAP_UNIX) == 1
    assert p.era_for(p.NTP_WRAP_UNIX + 86400) == 1
