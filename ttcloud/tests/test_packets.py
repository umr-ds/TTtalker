from typing import List

from ttcloud.packets import unmarshall

SAMPLE_PACKETS: List[bytes] = [
    bytes.fromhex("4a4a4a4a520103520502"),
    bytes.fromhex("52010352180103c241be52d84860"),
    bytes.fromhex(
        "180103c2520103524d020d010000328800008c88000071b5000013aa0000111dd4004a00eafc940f0000000000007787000074570000fcc5bd430100"
    ),
]


def test_marshalling() -> None:
    for packet in SAMPLE_PACKETS:
        unmarshalled = unmarshall(packet)
        marshalled = unmarshalled.marshall()

        assert marshalled == packet
