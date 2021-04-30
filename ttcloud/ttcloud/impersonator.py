#! /usr/bin/env python3

from socket import socket
from ttcloud.packets import unmarshall


def listen_and_process(s: socket) -> None:
    packet: bytes
    while 1:
        packet, _ = s.recvfrom(4096)
        unmarshall(packet)


if __name__ == "__main__":
    test_packet: bytes = bytes.fromhex("52010352180103c241be52d84860")
    parsed = unmarshall(test_packet)
    print(parsed)
    marshalled = parsed.marshall()

    assert marshalled == test_packet
