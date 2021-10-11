import time

from typing import Tuple, Union

from dataclasses import dataclass

import influxdb as influx
from influxdb.resultset import ResultSet
from sklearn.linear_model import LinearRegression

from ttt.packets import (
    TTPacket,
    DataPacket,
    DataPacket2,
    TTCommand1,
    TTCommand2,
    TTAddress,
)
from ttt.util import compute_temperature, compute_battery_voltage


rde = 1


@dataclass
class Policy:
    local_address: TTAddress
    influx_client: influx.InfluxDBClient

    def evaluate(self, packet: TTPacket) -> Tuple[bool, TTPacket]:
        """Evaluates the received packet und returns a potential reply packet

        Args:
            packet (TTPacket): Packet received by the RCI

        Returns:
            Tuple(bool, TTPacket): Boolean is true if policy detected anomaly, false otherwise.
                                   TTPacket is potential reply packet that may be sent.
        """
        pass


class LocalDataPolicy(Policy):
    def _evaluate_battery(self, packet: DataPacket) -> int:
        battery_voltage = compute_battery_voltage(
            adc_volt_bat=packet.adc_volt_bat, adc_bandgap=packet.adc_bandgap
        )

        data: ResultSet = self.influx_client.query(
            'SELECT "ttt_voltage" FROM "power" WHERE time > now() - 2d'
        )
        times = []
        voltages = []
        for datapoint in data.get_points("power"):
            timestamp = int(
                time.mktime(time.strptime(datapoint["time"], "%Y-%m-%dT%H:%M:%S.%fZ"))
            )
            times.append([timestamp])
            voltages.append(datapoint["ttt_voltage"])

        times.append([int(time.time())])
        voltages.append(battery_voltage)

        reg: LinearRegression = LinearRegression().fit(times, voltages)

        try:
            measurement_interval = next(
                self.influx_client.query(
                    f'SELECT last("measurement_interval") FROM "measurement_interval" WHERE treealker = {packet.sender_address.address}'
                ).get_points("power")
            )[
                "last"
            ]  # I hate this monstrosity and I hate influx for making me do this...
        except StopIteration:
            measurement_interval = 3600

        measurement_interval = int(
            measurement_interval
            + (rde * (3700 - reg.predict([[int(time.time()) + (3600 * 48)]])[0]))
        )

        influx_data = [
            {
                "measurement": "measurement_interval",
                "tags": {
                    "treetalker": packet.sender_address.address,
                },
                "fields": {
                    "measurement_interval": measurement_interval,
                },
            },
        ]
        self.influx_client.write_points(influx_data)

        return measurement_interval

    def _evaluate_temperatures(self, packet: DataPacket):
        temperature_reference_0 = compute_temperature(packet.temperature_reference[0])
        temperature_reference_1 = compute_temperature(packet.temperature_reference[1])

    def evaluate(
        self, packet: Union[DataPacket, DataPacket2]
    ) -> Tuple[bool, TTCommand1]:
        return True, TTCommand1(
            receiver_address=packet.sender_address,
            sender_address=self.local_address,
            command=32,
            time=int(time.time()),
            sleep_intervall=120,
            unknown=(0, 45, 1),
            heating=30,
        )


class AggregatedDataPolicy(Policy):
    def evaluate(self, packet: DataPacket2) -> Tuple[bool, TTCommand1]:
        pass


class LocalLightPolicy(Policy):
    def evaluate(self, packet: DataPacket2) -> Tuple[bool, TTCommand2]:
        return True, TTCommand2(
            receiver_address=packet.sender_address,
            sender_address=self.local_address,
            command=33,
            time=int(time.time()),
            integration_time=50,
            gain=3,
        )
