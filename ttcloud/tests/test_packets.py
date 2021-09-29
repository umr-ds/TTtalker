from typing import List, Tuple

from ttt.packets import *


def _zip_dicts() -> List[Tuple[bytes, TTPacket]]:
    zipped: List[Tuple[bytes, TTPacket]] = []
    for key in SAMPLE_PACKETS.keys():
        zipped.append((SAMPLE_RAW[key], SAMPLE_PACKETS[key]))

    return zipped


def test_unmarshall() -> None:
    for raw, packet in _zip_dicts():
        unmarshalled = unmarshall(raw)

        assert unmarshalled == packet


def test_marshall() -> None:
    for raw, packet in _zip_dicts():
        marshalled = packet.marshall()

        assert marshalled == raw


def test_marshall_unmarshall() -> None:
    for raw in SAMPLE_RAW.values():
        unmarshalled = unmarshall(raw)
        marshalled = unmarshalled.marshall()

        assert marshalled == raw


def test_unmarshall_marshall() -> None:
    for packet in SAMPLE_PACKETS.values():
        marshalled = packet.marshall()
        unmarshalled = unmarshall(marshalled)

        assert unmarshalled == packet
