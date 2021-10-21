from __future__ import annotations
from dataclasses import dataclass
from io import BytesIO
from struct import unpack, pack
from typing import Dict, Callable, Tuple, List, Any

from ttt.util import (
    compute_battery_voltage_rev_3_2,
    compute_battery_voltage_rev_3_1,
    compute_temperature,
)
from ttt.address import TTAddress


@dataclass
class TTPacket:
    receiver_address: TTAddress
    sender_address: TTAddress

    @classmethod
    def unmarshall(
        cls, receiver_address: TTAddress, sender_address: TTAddress, raw_stream: BytesIO
    ) -> TTPacket:
        raise NotImplemented

    def marshall(self) -> bytes:
        raise NotImplemented

    def to_influx_json(self) -> List[Dict[str, Any]]:
        raise NotImplemented


@dataclass
class TTHeloPacket(TTPacket):
    packet_number: int
    packet_type: int = 5

    def __eq__(self, other) -> bool:
        return isinstance(other, TTHeloPacket) and self.__dict__ == other.__dict__

    @classmethod
    def unmarshall(
        cls, receiver_address: TTAddress, sender_address: TTAddress, raw_stream: BytesIO
    ) -> TTHeloPacket:
        packet_number: int = unpack("=B", raw_stream.read())[0]
        return cls(
            receiver_address=receiver_address,
            sender_address=sender_address,
            packet_number=packet_number,
        )

    def marshall(self) -> bytes:
        return pack(
            "=IIBB",
            self.receiver_address.address,
            self.sender_address.address,
            self.packet_type,
            self.packet_number,
        )


@dataclass
class TTCloudHeloPacket(TTPacket):
    command: int
    time: int
    packet_type: int = 65

    def __eq__(self, other) -> bool:
        return isinstance(other, TTCloudHeloPacket) and self.__dict__ == other.__dict__

    @classmethod
    def unmarshall(
        cls, receiver_address: TTAddress, sender_address: TTAddress, raw_stream: BytesIO
    ) -> TTCloudHeloPacket:
        command: int
        cloud_time: int
        command, cloud_time = unpack("=BI", raw_stream.read())
        return cls(
            receiver_address=receiver_address,
            sender_address=sender_address,
            command=command,
            time=cloud_time,
        )

    def marshall(self) -> bytes:
        return pack(
            "=IIBBI",
            self.receiver_address.address,
            self.sender_address.address,
            self.packet_type,
            self.command,
            self.time,
        )


@dataclass
class DataPacketRev32(TTPacket):
    packet_number: int
    timestamp: int
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
    packet_type: int = 77

    def __eq__(self, other) -> bool:
        return isinstance(other, DataPacketRev32) and self.__dict__ == other.__dict__

    @classmethod
    def unmarshall(
        cls, receiver_address: TTAddress, sender_address: TTAddress, raw_stream: BytesIO
    ) -> DataPacketRev32:
        fields = unpack("=BIIIIIBBhhhhhhhIIHI", raw_stream.read())
        packet_number: int = fields[0]
        timestamp: int = fields[1]
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
            timestamp=timestamp,
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
            "=IIBBIIIIIBBhhhhhhhIIHI",
            self.receiver_address.address,
            self.sender_address.address,
            self.packet_type,
            self.packet_number,
            self.timestamp,
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

    def to_influx_json(self) -> List[Dict[str, Any]]:
        return [
            {
                "measurement": "stem_temperature",
                "tags": {
                    "treetalker": self.sender_address.address,
                    "heating": True,
                },
                "fields": {
                    "reference_probe_cold": self.temperature_reference[0],
                    "reference_probe_hot": self.temperature_reference[1],
                    "heat_probe_cold": self.temperature_heat[0],
                    "heat_probe_hot": self.temperature_heat[1],
                    "ttt_reference_probe_cold": compute_temperature(
                        self.temperature_reference[0]
                    ),
                    "ttt_reference_probe_hot": compute_temperature(
                        self.temperature_reference[1]
                    ),
                    "ttt_heat_probe_cold": compute_temperature(
                        self.temperature_heat[0]
                    ),
                    "ttt_heat_probe_hot": compute_temperature(self.temperature_heat[1]),
                },
            },
            {
                "measurement": "growth",
                "tags": {
                    "treetalker": self.sender_address.address,
                },
                "fields": {
                    "distance": self.growth_sensor,
                },
            },
            {
                "measurement": "power",
                "tags": {
                    "treetalker": self.sender_address.address,
                },
                "fields": {
                    "bandgap": self.adc_bandgap,
                    "voltage": self.adc_volt_bat,
                    "ttt_voltage": compute_battery_voltage_rev_3_2(
                        adc_volt_bat=self.adc_volt_bat, adc_bandgap=self.adc_bandgap
                    ),
                },
            },
            {
                "measurement": "stem_water",
                "tags": {
                    "treetalker": self.sender_address.address,
                },
                "fields": {
                    "content": self.StWC,
                },
            },
            {
                "measurement": "air",
                "tags": {
                    "treetalker": self.sender_address.address,
                },
                "fields": {
                    "temperature": self.air_temperature,
                    "humidity": self.air_relative_humidity,
                },
            },
            {
                "measurement": "gravity",
                "tags": {
                    "treetalker": self.sender_address.address,
                },
                "fields": {
                    "x_mean": self.gravity_x_mean,
                    "x_derivation": self.gravity_x_derivation,
                    "y_mean": self.gravity_y_mean,
                    "y_derivation": self.gravity_y_derivation,
                    "z_mean": self.gravity_z_mean,
                    "z_derivation": self.gravity_z_derivation,
                },
            },
        ]


@dataclass
class DataPacketRev31(TTPacket):
    packet_number: int
    timestamp: int
    temperature_reference_cold: int
    temperature_heat_cold: int
    growth_sensor: int
    voltage: int
    number_of_bits: int
    air_relative_humidity: int
    air_temperature: int
    gravity_z_mean: int
    gravity_z_derivation: int
    gravity_y_mean: int
    gravity_y_derivation: int
    gravity_x_mean: int
    gravity_x_derivation: int
    temperature_reference_hot: int
    temperature_heat_hot: int
    moisture: int
    packet_type: int = 69

    def __eq__(self, other) -> bool:
        return isinstance(other, DataPacketRev31) and self.__dict__ == other.__dict__

    @classmethod
    def unmarshall(
        cls, receiver_address: TTAddress, sender_address: TTAddress, raw_stream: BytesIO
    ) -> DataPacketRev31:
        fields = unpack("=BIhhIIBBhhhhhhhhhh", raw_stream.read())
        packet_number: int = fields[0]
        timestamp: int = fields[1]
        temperature_reference_cold: int = fields[2]
        temperature_heat_cold: int = fields[3]
        growth_sensor: int = fields[4]
        voltage: int = fields[5]
        number_of_bits: int = fields[6]
        air_relative_humidity: int = fields[7]
        air_temperature: int = fields[8]
        gravity_z_mean: int = fields[9]
        gravity_z_derivation: int = fields[10]
        gravity_y_mean: int = fields[11]
        gravity_y_derivation: int = fields[12]
        gravity_x_mean: int = fields[13]
        gravity_x_derivation: int = fields[14]
        temperature_reference_hot: int = fields[15]
        temperature_heat_hot: int = fields[16]
        moisture: int = fields[17]

        return cls(
            receiver_address=receiver_address,
            sender_address=sender_address,
            packet_number=packet_number,
            timestamp=timestamp,
            temperature_reference_cold=temperature_reference_cold,
            temperature_heat_cold=temperature_heat_cold,
            growth_sensor=growth_sensor,
            voltage=voltage,
            number_of_bits=number_of_bits,
            air_relative_humidity=air_relative_humidity,
            air_temperature=air_temperature,
            gravity_z_mean=gravity_z_mean,
            gravity_z_derivation=gravity_z_derivation,
            gravity_y_mean=gravity_y_mean,
            gravity_y_derivation=gravity_y_derivation,
            gravity_x_mean=gravity_x_mean,
            gravity_x_derivation=gravity_x_derivation,
            temperature_reference_hot=temperature_reference_hot,
            temperature_heat_hot=temperature_heat_hot,
            moisture=moisture,
        )

    def marshall(self) -> bytes:
        return pack(
            "=IIBBIhhIIBBhhhhhhhhhh",
            self.receiver_address.address,
            self.sender_address.address,
            self.packet_type,
            self.packet_number,
            self.timestamp,
            self.temperature_reference_cold,
            self.temperature_heat_cold,
            self.growth_sensor,
            self.voltage,
            self.number_of_bits,
            self.air_relative_humidity,
            self.air_temperature,
            self.gravity_z_mean,
            self.gravity_z_derivation,
            self.gravity_y_mean,
            self.gravity_y_derivation,
            self.gravity_x_mean,
            self.gravity_x_derivation,
            self.temperature_reference_hot,
            self.temperature_heat_hot,
            self.moisture,
        )

    def to_influx_json(self) -> List[Dict[str, Any]]:
        return [
            {
                "measurement": "stem_temperature",
                "tags": {
                    "treetalker": self.sender_address.address,
                    "heating": False,
                },
                "fields": {
                    "reference_probe_cold": self.temperature_reference_cold,
                    "reference_probe_hot": self.temperature_reference_hot,
                    "heat_probe_cold": self.temperature_heat_cold,
                    "heat_probe_hot": self.temperature_heat_hot,
                    "ttt_reference_probe_cold": compute_temperature(
                        self.temperature_reference_cold
                    ),
                    "ttt_reference_probe_hot": compute_temperature(
                        self.temperature_reference_hot
                    ),
                    "ttt_heat_probe_cold": compute_temperature(
                        self.temperature_heat_cold
                    ),
                    "ttt_heat_probe_hot": compute_temperature(
                        self.temperature_heat_hot
                    ),
                },
            },
            {
                "measurement": "growth",
                "tags": {
                    "treetalker": self.sender_address.address,
                },
                "fields": {
                    "distance": self.growth_sensor,
                },
            },
            {
                "measurement": "power",
                "tags": {
                    "treetalker": self.sender_address.address,
                },
                "fields": {
                    "voltage": self.voltage,
                    "ttt_voltage": compute_battery_voltage_rev_3_1(
                        voltage=self.voltage
                    ),
                },
            },
            {
                "measurement": "stem_water",
                "tags": {
                    "treetalker": self.sender_address.address,
                },
                "fields": {
                    "xmc": self.moisture,
                },
            },
            {
                "measurement": "air",
                "tags": {
                    "treetalker": self.sender_address.address,
                },
                "fields": {
                    "temperature": self.air_temperature,
                    "humidity": self.air_relative_humidity,
                },
            },
            {
                "measurement": "gravity",
                "tags": {
                    "treetalker": self.sender_address.address,
                },
                "fields": {
                    "x_mean": self.gravity_x_mean,
                    "x_derivation": self.gravity_x_derivation,
                    "y_mean": self.gravity_y_mean,
                    "y_derivation": self.gravity_y_derivation,
                    "z_mean": self.gravity_z_mean,
                    "z_derivation": self.gravity_z_derivation,
                },
            },
        ]


@dataclass
class LightSensorPacket(TTPacket):
    packet_number: int
    timestamp: int
    AS7263: Dict[int, float]
    AS7262: Dict[int, float]
    integration_time: int
    gain: int
    packet_type: int = 73

    def __eq__(self, other) -> bool:
        return isinstance(other, LightSensorPacket) and self.__dict__ == other.__dict__

    @classmethod
    def unmarshall(
        cls, receiver_address: TTAddress, sender_address: TTAddress, raw_stream: BytesIO
    ) -> LightSensorPacket:
        fields = unpack("=BIffffffffffffBB", raw_stream.read())
        return LightSensorPacket(
            receiver_address=receiver_address,
            sender_address=sender_address,
            packet_number=fields[0],
            timestamp=fields[1],
            AS7263={
                610: fields[2],
                680: fields[3],
                730: fields[4],
                760: fields[5],
                810: fields[6],
                860: fields[7],
            },
            AS7262={
                450: fields[8],
                500: fields[9],
                550: fields[10],
                570: fields[11],
                600: fields[12],
                650: fields[13],
            },
            integration_time=fields[14],
            gain=fields[15],
        )

    def marshall(self) -> bytes:
        return pack(
            "=IIBBIffffffffffffBB",
            self.receiver_address.address,
            self.sender_address.address,
            self.packet_type,
            self.packet_number,
            self.timestamp,
            self.AS7263[610],
            self.AS7263[680],
            self.AS7263[730],
            self.AS7263[760],
            self.AS7263[810],
            self.AS7263[860],
            self.AS7262[450],
            self.AS7262[500],
            self.AS7262[550],
            self.AS7262[570],
            self.AS7262[600],
            self.AS7262[650],
            self.integration_time,
            self.gain,
        )

    def to_influx_json(self) -> List[Dict[str, Any]]:
        return [
            {
                "measurement": "AS7263",
                "tags": {
                    "treetalker": self.sender_address.address,
                    "gain": self.gain,
                    "integration_time": self.integration_time,
                },
                "fields": {
                    "610": self.AS7263[610],
                    "680": self.AS7263[680],
                    "730": self.AS7263[730],
                    "760": self.AS7263[760],
                    "810": self.AS7263[810],
                    "860": self.AS7263[860],
                },
            },
            {
                "measurement": "AS7262",
                "tags": {
                    "treetalker": self.sender_address.address,
                    "gain": self.gain,
                    "integration_time": self.integration_time,
                },
                "fields": {
                    "450": self.AS7262[450],
                    "500": self.AS7262[500],
                    "550": self.AS7262[550],
                    "570": self.AS7262[570],
                    "600": self.AS7262[600],
                    "650": self.AS7262[650],
                },
            },
        ]


@dataclass
class TTCommand1(TTPacket):
    command: int
    time: int
    sleep_interval: int
    unknown: int
    heating: int
    time_slot: int
    time_slot_length: int
    packet_type: int = 66

    def __eq__(self, other) -> bool:
        return isinstance(other, TTCommand1) and self.__dict__ == other.__dict__

    @classmethod
    def unmarshall(
        cls, receiver_address: TTAddress, sender_address: TTAddress, raw_stream: BytesIO
    ) -> TTCommand1:
        fields = unpack("=BIHHHBB", raw_stream.read())
        return TTCommand1(
            receiver_address=receiver_address,
            sender_address=sender_address,
            command=fields[0],
            time=fields[1],
            sleep_interval=fields[2],
            unknown=fields[3],
            heating=fields[4],
            time_slot_length=fields[5],
            time_slot=fields[6],
        )

    def marshall(self) -> bytes:
        return pack(
            "=IIBBIHHHBB",
            self.receiver_address.address,
            self.sender_address.address,
            self.packet_type,
            self.command,
            self.time,
            self.sleep_interval,
            self.unknown,
            self.heating,
            self.time_slot_length,
            self.time_slot,
        )


@dataclass
class TTCommand2(TTPacket):
    command: int
    time: int
    integration_time: int
    gain: int
    packet_type: int = 74

    def __eq__(self, other) -> bool:
        return isinstance(other, TTCommand2) and self.__dict__ == other.__dict__

    @classmethod
    def unmarshall(
        cls, receiver_address: TTAddress, sender_address: TTAddress, raw_stream: BytesIO
    ) -> TTCommand2:
        fields = unpack("=BIBB", raw_stream.read())
        return TTCommand2(
            receiver_address=receiver_address,
            sender_address=sender_address,
            command=fields[0],
            time=fields[1],
            integration_time=fields[2],
            gain=fields[3],
        )

    def marshall(self) -> bytes:
        return pack(
            "=IIBBIBB",
            self.receiver_address.address,
            self.sender_address.address,
            self.packet_type,
            self.command,
            self.time,
            self.integration_time,
            self.gain,
        )


PACKET_TYPES: Dict[int, Callable[[TTAddress, TTAddress, BytesIO], TTPacket]] = {
    5: TTHeloPacket.unmarshall,
    65: TTCloudHeloPacket.unmarshall,
    66: TTCommand1.unmarshall,
    69: DataPacketRev31.unmarshall,
    73: LightSensorPacket.unmarshall,
    74: TTCommand2.unmarshall,
    77: DataPacketRev32.unmarshall,
}


def unmarshall(raw: bytes) -> TTPacket:
    raw_stream = BytesIO(raw)
    receiver: int
    sender: int
    packet_type: int

    receiver, sender, packet_type = unpack("=IIB", raw_stream.read(9))

    receiver_address = TTAddress(receiver)
    sender_address: TTAddress = TTAddress(sender)

    parsed_packet: TTPacket = PACKET_TYPES[packet_type](
        receiver_address, sender_address, raw_stream
    )
    raw_stream.close()
    return parsed_packet


SAMPLE_RAW: Dict[str, bytes] = {
    "TTHeloPacket": bytes.fromhex("4a4a4a4a520103520502"),
    "TTCloudHeloPacket": bytes.fromhex("52010352180103c241be52d84860"),
    "DataPacket": bytes.fromhex(
        "180103c2520103524d014038000077850000fa8500006cb8000041aa0000111ee2003900ddfc920f000000000000788500000256000086c545430100"
    ),
    "LightSensorPacket": bytes.fromhex(
        "180103c252010352490240380000d10793414856da411448754256158f428151b34230d4b34245216742e5156842247e304244c42d42ea760f42d9e10b423203"
    ),
    "TTCommand1": bytes.fromhex("52010352180103c242188cd84860100e000058022d02"),
    "TTCommand2": bytes.fromhex("52010352180103c24a5289e148603203"),
}

SAMPLE_PACKETS: Dict[str, TTPacket] = {
    "TTHeloPacket": TTHeloPacket(
        receiver_address=TTAddress(1246382666),
        sender_address=TTAddress(1375928658),
        packet_number=2,
    ),
    "TTCloudHeloPacket": TTCloudHeloPacket(
        receiver_address=TTAddress(1375928658),
        sender_address=TTAddress(3254976792),
        command=190,
        time=1615386706,
    ),
    "DataPacket": DataPacketRev32(
        receiver_address=TTAddress(3254976792),
        sender_address=TTAddress(1375928658),
        packet_number=1,
        timestamp=14400,
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
    "LightSensorPacket": LightSensorPacket(
        receiver_address=TTAddress(3254976792),
        sender_address=TTAddress(1375928658),
        packet_number=2,
        timestamp=14400,
        AS7263={
            610: 18.378816604614258,
            680: 27.292129516601562,
            730: 61.32038879394531,
            760: 71.54167175292969,
            810: 89.65918731689453,
            860: 89.9144287109375,
        },
        AS7262={
            450: 57.78248977661133,
            500: 58.02138137817383,
            550: 44.12318420410156,
            570: 43.44166564941406,
            600: 35.866127014160156,
            650: 34.97055435180664,
        },
        integration_time=50,
        gain=3,
    ),
    "TTCommand1": TTCommand1(
        receiver_address=TTAddress(1375928658),
        sender_address=TTAddress(3254976792),
        command=24,
        time=1615386764,
        sleep_interval=3600,
        unknown=0,
        heating=600,
        time_slot_length=45,
        time_slot=2,
    ),
    "TTCommand2": TTCommand2(
        receiver_address=TTAddress(1375928658),
        sender_address=TTAddress(3254976792),
        command=82,
        time=1615389065,
        integration_time=50,
        gain=3,
    ),
}
