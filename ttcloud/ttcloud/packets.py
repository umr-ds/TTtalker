from __future__ import annotations
from dataclasses import dataclass
from io import BytesIO
from struct import unpack, pack
from typing import Dict, Callable, Tuple


@dataclass
class TTAddress:
    address: int

    def __str__(self) -> str:
        return hex(self.address)

    def __eq__(self, other) -> bool:
        return isinstance(other, TTAddress) and (self.address == other.address)

    def __hash__(self) -> int:
        return self.address.__hash__()


@dataclass
class TTPacket:
    receiver_address: TTAddress
    sender_address: TTAddress

    def packet_type(self) -> str:
        raise NotImplemented

    @classmethod
    def unmarshall(
        cls, receiver_address: TTAddress, sender_address: TTAddress, raw_stream: BytesIO
    ) -> TTPacket:
        raise NotImplemented

    def marshall(self) -> bytes:
        raise NotImplemented


@dataclass
class TTHeloPacket(TTPacket):
    packet_number: int

    def __eq__(self, other) -> bool:
        return isinstance(other, TTHeloPacket) and self.__dict__ == other.__dict__

    def packet_type(self) -> str:
        return "HELO"

    @classmethod
    def unmarshall(
        cls, receiver_address: TTAddress, sender_address: TTAddress, raw_stream: BytesIO
    ) -> TTHeloPacket:
        packet_number: int = unpack("!B", raw_stream.read(1))[0]
        return cls(
            receiver_address=receiver_address,
            sender_address=sender_address,
            packet_number=packet_number,
        )

    def marshall(self) -> bytes:
        return pack(
            "!IIBB",
            self.receiver_address.address,
            self.sender_address.address,
            5,
            self.packet_number,
        )


@dataclass
class TTCloudHeloPacket(TTPacket):
    command: int
    time: int

    def __eq__(self, other) -> bool:
        return isinstance(other, TTCloudHeloPacket) and self.__dict__ == other.__dict__

    def packet_type(self) -> str:
        return "TTCloudHELO"

    @classmethod
    def unmarshall(
        cls, receiver_address: TTAddress, sender_address: TTAddress, raw_stream: BytesIO
    ) -> TTCloudHeloPacket:
        command: int
        cloud_time: int
        command, cloud_time = unpack("!BI", raw_stream.read(5))
        return cls(
            receiver_address=receiver_address,
            sender_address=sender_address,
            command=command,
            time=cloud_time,
        )

    def marshall(self) -> bytes:
        return pack(
            "!IIBBI",
            self.receiver_address.address,
            self.sender_address.address,
            65,
            self.command,
            self.time,
        )


@dataclass
class DataPacket(TTPacket):
    packet_number: int
    time: int
    growth_sensor: int
    adc_bandgap: int
    number_of_bits: int
    air_relative_humidity: int
    air_temperature: int
    gravity_z_mean: int
    gravity_z_derivation: int
    gravity_y_mean: int
    gravity_y_derivation: int
    gravity_x_mean: int
    gravity_x_derivation: int
    StWC: int
    adc_volt_bat: int
    temperature_reference: Tuple[int, int]
    temperature_heat: Tuple[int, int]

    def __eq__(self, other) -> bool:
        return isinstance(other, DataPacket) and self.__dict__ == other.__dict__

    def packet_type(self) -> str:
        return "DATA"

    @classmethod
    def unmarshall(
        cls, receiver_address: TTAddress, sender_address: TTAddress, raw_stream: BytesIO
    ) -> DataPacket:
        fields = unpack("!BIIIIIBBhhhhhhhIIHI", raw_stream.read(51))
        packet_number: int = fields[0]
        time: int = fields[1]
        t_ref_0: int = fields[2]
        t_heat_0: int = fields[3]
        growth_sensor: int = fields[4]
        adc_bandgap: int = fields[5]
        number_of_bits: int = fields[6]
        air_relative_humidity: int = fields[7]
        air_temperature: int = fields[8]
        gravity_z_mean: int = fields[9]
        gravity_z_derivation: int = fields[10]
        gravity_y_mean: int = fields[11]
        gravity_y_derivation: int = fields[12]
        gravity_x_mean: int = fields[13]
        gravity_x_derivation: int = fields[14]
        t_ref_1: int = fields[15]
        t_heat_1: int = fields[16]
        StWC: int = fields[17]
        adc_volt_bat: int = fields[18]

        temperature_reference: Tuple[int, int] = (t_ref_0, t_ref_1)
        temperature_heat: Tuple[int, int] = (t_heat_0, t_heat_1)
        return cls(
            receiver_address=receiver_address,
            sender_address=sender_address,
            packet_number=packet_number,
            time=time,
            growth_sensor=growth_sensor,
            adc_bandgap=adc_bandgap,
            number_of_bits=number_of_bits,
            air_relative_humidity=air_relative_humidity,
            air_temperature=air_temperature,
            gravity_z_mean=gravity_z_mean,
            gravity_z_derivation=gravity_z_derivation,
            gravity_y_mean=gravity_y_mean,
            gravity_y_derivation=gravity_y_derivation,
            gravity_x_mean=gravity_x_mean,
            gravity_x_derivation=gravity_x_derivation,
            StWC=StWC,
            adc_volt_bat=adc_volt_bat,
            temperature_reference=temperature_reference,
            temperature_heat=temperature_heat,
        )

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


@dataclass
class TTCommand1(TTPacket):
    command: int
    timestamp: int
    sleep_intervall: int
    unknown: Tuple[int, int, int]
    heating: int

    def __eq__(self, other) -> bool:
        return isinstance(other, TTCommand1) and self.__dict__ == other.__dict__

    def packet_type(self) -> str:
        return "COMMAND"

    @classmethod
    def unmarshall(
        cls, receiver_address: TTAddress, sender_address: TTAddress, raw_stream: BytesIO
    ) -> TTCommand1:
        fields = unpack("!BIHHHBB", raw_stream.read(13))
        return TTCommand1(
            receiver_address=receiver_address,
            sender_address=sender_address,
            command=fields[0],
            timestamp=fields[1],
            sleep_intervall=fields[2],
            heating=fields[4],
            unknown=(fields[3], fields[5], fields[6]),
        )

    def marshall(self) -> bytes:
        return pack(
            "!IIBBIHHHBB",
            self.receiver_address.address,
            self.sender_address.address,
            66,
            self.command,
            self.timestamp,
            self.sleep_intervall,
            self.unknown[0],
            self.heating,
            self.unknown[1],
            self.unknown[2],
        )


PACKET_TYPES: Dict[int, Callable[[TTAddress, TTAddress, BytesIO], TTPacket]] = {
    5: TTHeloPacket.unmarshall,
    65: TTCloudHeloPacket.unmarshall,
    66: TTCommand1.unmarshall,
    77: DataPacket.unmarshall,
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
