#! /usr/bin/env python3

from socket import socket
from ttcloud.packets import unmarshall


def listen_and_process(s: socket) -> None:
    packet: bytes
    while 1:
        packet, _ = s.recvfrom(4096)
        unmarshall(packet)


if __name__ == "__main__":
    test_packet: bytes = bytes.fromhex(
        "180103c2520103524d020d010000328800008c88000071b5000013aa0000111dd4004a00eafc940f0000000000007787000074570000fcc5bd430100"
    )
    parsed = unmarshall(test_packet)
    print(parsed)
    marshalled = parsed.marshall()

    assert marshalled == test_packet
