from dataclasses import dataclass
from io import BytesIO
from struct import unpack, pack
from typing import Dict, Callable, Tuple


@dataclass
class TTAddress:
    address: int

    def __str__(self) -> str:
        return hex(self.address)


@dataclass
class TTPacket:
    sender_address: TTAddress
    receiver_address: TTAddress

    def packet_type(self) -> str:
        raise NotImplemented

    def marshall(self) -> bytes:
        raise NotImplemented


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
        self.packet_number: int = unpack("!B", raw_stream.read(1))[0]

    def packet_type(self) -> str:
        return "HELO"

    def marshall(self) -> bytes:
        return pack(
            "!IIBB",
            self.receiver_address.address,
            self.sender_address.address,
            5,
            self.packet_number,
        )


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

        fields = unpack("!BIIIIIBBhhhhhhhIIHI", raw_stream.read(51))

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

    def marshall(self) -> bytes:
        return pack(
            "!IIBBIIIIIBBhhhhhhhIIHI",
            self.receiver_address.address,
            self.sender_address.address,
            77,
            self.packet_number,
            self.time,
            self.temperature_reference[0],
            self.temperature_heat[0],
            self.growth_sensor,
            self.adc_bandgap,
            self.number_of_bits,
            self.air_relative_humidity,
            self.air_temperature,
            self.gravity_z_mean,
            self.gravity_z_derivation,
            self.gravity_y_mean,
            self.gravity_y_derivation,
            self.gravity_x_mean,
            self.gravity_x_derivation,
            self.temperature_reference[1],
            self.temperature_heat[1],
            self.StWC,
            self.adc_volt_bat,
        )


PACKET_TYPES: Dict[int, Callable[[TTAddress, TTAddress, BytesIO], TTPacket]] = {
    5: HELOPacket,
    77: DATAPacket,
}


def unmarshall(raw: bytes) -> TTPacket:
    raw_stream = BytesIO(raw)
    receiver: int
    sender: int
    packet_type: int

    receiver, sender, packet_type = unpack("!IIB", raw_stream.read(9))

    receiver_address = TTAddress(receiver)
    sender_address: TTAddress = TTAddress(sender)

    parsed_packet: TTPacket = PACKET_TYPES[packet_type](
        receiver_address, sender_address, raw_stream
    )
    raw_stream.close()
    return parsed_packet
