from dataclasses import dataclass
from io import BytesIO
from struct import unpack
from typing import Dict, Callable, Tuple


@dataclass
class TTAddress:
    address: bytes

    def __str__(self) -> str:
        return self.address.hex()


@dataclass
class TTPacket:
    sender_address: TTAddress
    receiver_address: TTAddress

    def packet_type(self) -> str:
        return ""


class HELOPacket(TTPacket):
    def __init__(
        self,
        receiver_address: TTAddress,
        sender_address: TTAddress,
        raw_stream: BytesIO,
    ):
        TTPacket.__init__(
            self=self, sender_address=sender_address, receiver_address=receiver_address
        )
        self.packet_number: int = int.from_bytes(raw_stream.read(1), byteorder="little")

    def packet_type(self) -> str:
        return "HELO"


class DATAPacket(TTPacket):
    def __init__(
        self,
        receiver_address: TTAddress,
        sender_address: TTAddress,
        raw_stream: BytesIO,
    ):
        TTPacket.__init__(
            self=self, sender_address=sender_address, receiver_address=receiver_address
        )

        fields = unpack("=BIIIIIBBhhhhhhhIIHI", raw_stream.read(51))

        self.packet_number: int = fields[0]
        self.time: int = fields[1]
        t_ref_0: int = fields[2]
        t_heat_0: int = fields[3]
        self.growth_sensor: int = fields[4]
        self.adc_bandgap: int = fields[5]
        self.number_of_bits: int = fields[6]
        self.air_relative_humidity: int = fields[7]
        self.air_temperature: int = fields[8]
        self.gravity_z_mean: int = fields[9]
        self.gravity_z_derivation: int = fields[10]
        self.gravity_y_mean: int = fields[11]
        self.gravity_y_derivation: int = fields[12]
        self.gravity_x_mean: int = fields[13]
        self.gravity_x_derivation: int = fields[14]
        t_ref_1: int = fields[15]
        t_heat_1: int = fields[16]
        self.StWC: int = fields[17]
        self.adc_volt_bat: int = fields[18]

        self.temperature_reference: Tuple[int, int] = (t_ref_0, t_ref_1)
        self.temperature_heat: Tuple[int, int] = (t_heat_0, t_heat_1)

    def packet_type(self) -> str:
        return "DATA"


PACKET_TYPES: Dict[int, Callable[[TTAddress, TTAddress, BytesIO], TTPacket]] = {
    5: HELOPacket,
    77: DATAPacket,
}


def unmarshall(raw: bytes) -> TTPacket:
    raw_stream = BytesIO(raw)
    receiver_address: TTAddress = TTAddress(raw_stream.read(4))
    sender_address: TTAddress = TTAddress(raw_stream.read(4))
    packet_type: int = unpack("B", raw_stream.read(1))[0]

    parsed_packet: TTPacket = PACKET_TYPES[packet_type](
        receiver_address, sender_address, raw_stream
    )
    raw_stream.close()
    return parsed_packet
