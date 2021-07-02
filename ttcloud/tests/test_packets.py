from typing import List

from ttcloud.packets import *

SAMPLE_RAW: List[bytes] = [
    bytes.fromhex("4a4a4a4a520103520502"),
    bytes.fromhex("52010352180103c241be52d84860"),
    bytes.fromhex(
        "180103c2520103524d014038000077850000fa8500006cb8000041aa0000111ee2003900ddfc920f000000000000788500000256000086c545430100"
    ),
    bytes.fromhex("52010352180103c242188cd84860100e000058022d02"),
    bytes.fromhex("52010352180103c24a5289e148603203"),
]

SAMPLE_PACKETS: List[TTPacket] = [
    TTHeloPacket(
        receiver_address=TTAddress(1246382666),
        sender_address=TTAddress(1375928658),
        packet_number=2,
    ),
    TTCloudHeloPacket(
        receiver_address=TTAddress(1375928658),
        sender_address=TTAddress(3254976792),
        command=190,
        time=1615386706,
    ),
    DataPacket(
        receiver_address=TTAddress(3254976792),
        sender_address=TTAddress(1375928658),
        packet_number=1,
        time=14400,
        temperature_reference=(34167, 34168),
        temperature_heat=(34298, 22018),
        growth_sensor=47212,
        adc_bandgap=43585,
        number_of_bits=17,
        air_relative_humidity=30,
        air_temperature=226,
        gravity_z_mean=57,
        gravity_z_derivation=-803,
        gravity_y_mean=3986,
        gravity_y_derivation=0,
        gravity_x_mean=0,
        gravity_x_derivation=0,
        StWC=50566,
        adc_volt_bat=82757,
    ),
    TTCommand1(
        receiver_address=TTAddress(1375928658),
        sender_address=TTAddress(3254976792),
        command=24,
        timestamp=1615386764,
        sleep_intervall=3600,
        heating=600,
        unknown=(0, 45, 2),
    ),
    TTCommand2(
        receiver_address=TTAddress(1375928658),
        sender_address=TTAddress(3254976792),
        command=82,
        timestamp=1615389065,
        integration_time=50,
        gain=3,
    ),
]


def test_unmarshall() -> None:
    for raw, packet in zip(SAMPLE_RAW, SAMPLE_PACKETS):
        unmarshalled = unmarshall(raw)

        assert unmarshalled == packet


def test_marshall() -> None:
    for raw, packet in zip(SAMPLE_RAW, SAMPLE_PACKETS):
        marshalled = packet.marshall()

        assert marshalled == raw


def test_marshall_unmarshall() -> None:
    for raw in SAMPLE_RAW:
        unmarshalled = unmarshall(raw)
        marshalled = unmarshalled.marshall()

        assert marshalled == raw


def test_unmarshall_marshall() -> None:
    for packet in SAMPLE_PACKETS:
        marshalled = packet.marshall()
        unmarshalled = unmarshall(marshalled)

        assert unmarshalled == packet
