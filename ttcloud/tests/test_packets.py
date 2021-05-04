from typing import List

from ttcloud.packets import *

SAMPLE_RAW: List[bytes] = [
    bytes.fromhex("4a4a4a4a520103520502"),
    bytes.fromhex("52010352180103c241be52d84860"),
    bytes.fromhex(
        "180103c2520103524d020d010000328800008c88000071b5000013aa0000111dd4004a00eafc940f0000000000007787000074570000fcc5bd430100"
    ),
]

SAMPLE_PACKETS: List[TTPacket] = [
    TTHeloPacket(
        receiver_address=TTAddress(1246382666),
        sender_address=TTAddress(1375798098),
        packet_number=2,
    ),
    TTCloudHeloPacket(
        receiver_address=TTAddress(1375798098),
        sender_address=TTAddress(402719682),
        command=190,
        time=1389906016,
    ),
    DataPacket(
        receiver_address=TTAddress(402719682),
        sender_address=TTAddress(1375798098),
        packet_number=2,
        time=218169344,
        temperature_reference=(679477256, 2020605959),
        temperature_heat=(3363831815, 1164967951),
        growth_sensor=458227713,
        adc_bandgap=983564289,
        number_of_bits=17,
        air_relative_humidity=221,
        air_temperature=16388,
        gravity_z_mean=-24562,
        gravity_z_derivation=-20535,
        gravity_y_mean=-16624,
        gravity_y_derivation=0,
        gravity_x_mean=0,
        gravity_x_derivation=7,
        StWC=64709,
        adc_volt_bat=3175284992,
    ),
]


def test_unmarshall() -> None:
    for raw, packet in zip(SAMPLE_RAW, SAMPLE_PACKETS):
        unmarshalled = unmarshall(raw)

        assert unmarshalled == packet
