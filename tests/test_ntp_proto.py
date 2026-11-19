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
