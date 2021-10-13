import time

from typing import Union, Dict, List
from collections import defaultdict
from statistics import mean, stdev

from dataclasses import dataclass

import influxdb as influx
from influxdb.resultset import ResultSet
from sklearn.linear_model import LinearRegression

from ttt.packets import (
    TTPacket,
    DataPacket,
    LightSensorPacket,
    TTCommand1,
    TTCommand2,
    TTAddress,
)
from ttt.util import compute_temperature, compute_battery_voltage


RDE = 1
ANALYSIS_INTERVAL = "2d"
SLEEP_TIME_MIN = 60


@dataclass
class Policy:
    local_address: TTAddress
    influx_client: influx.InfluxDBClient

    def evaluate(self, packet: TTPacket) -> TTPacket:
        """Evaluates the received packet und returns a potential reply packet

        Args:
            packet (TTPacket): Packet received by the RCI

        Returns:
            TTPacket: Reply packet.
        """
        pass


class LocalDataPolicy(Policy):
    def _evaluate_battery(self, packet: DataPacket) -> int:
        battery_voltage = compute_battery_voltage(
            adc_volt_bat=packet.adc_volt_bat, adc_bandgap=packet.adc_bandgap
        )

        data: ResultSet = self.influx_client.query(
            f'SELECT "ttt_voltage" FROM "power" WHERE time > now() - {ANALYSIS_INTERVAL}'
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
            + (RDE * (3700 - reg.predict([[int(time.time()) + (3600 * 48)]])[0]))
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

    def _evaluate_position(
        self, packet: DataPacket, means: Dict[str, List[int]]
    ) -> bool:
        mean_x = mean(means["x"])
        stdev_x = stdev(means["x"])
        mean_y = mean(means["y"])
        stdev_y = stdev(means["y"])
        mean_z = mean(means["z"])
        stdev_z = stdev(means["z"])

        x = packet.gravity_x_mean
        y = packet.gravity_y_mean
        z = packet.gravity_z_mean

        return (
            abs(x - mean_x) > stdev_x
            or abs(y - mean_y) > stdev_y
            or abs(z - mean_z) > stdev_z
        )

    def _evaluate_movement(
        self, packet: DataPacket, derivs: Dict[str, List[int]]
    ) -> bool:
        # FIXME: handle empty dicts
        mean_x = mean(derivs["x"])
        stdev_x = stdev(derivs["x"])
        mean_y = mean(derivs["y"])
        stdev_y = stdev(derivs["y"])
        mean_z = mean(derivs["z"])
        stdev_z = stdev(derivs["z"])

        x = packet.gravity_x_derivation
        y = packet.gravity_y_derivation
        z = packet.gravity_z_derivation

        return (
            abs(x - mean_x) > stdev_x
            or abs(y - mean_y) > stdev_y
            or abs(z - mean_z) > stdev_z
        )

    def _evaluate_gravity(self, packet: DataPacket) -> int:
        means: Dict[str, List[int]] = defaultdict(list)
        derivs: Dict[str, List[int]] = defaultdict(list)
        data: ResultSet = self.influx_client.query(
            f'SELECT "x_mean", "x_derivation", "y_mean", "y_derivation", "z_mean", "z_derivation" FROM "gravity" WHERE time > now() - {ANALYSIS_INTERVAL}'
        )

        for datapoint in data.get_points("gravity"):
            means["x"].append(datapoint["x_mean"])
            derivs["x"].append(datapoint["x_derivation"])
            means["y"].append(datapoint["y_mean"])
            derivs["y"].append(datapoint["y_derivation"])
            means["z"].append(datapoint["z_mean"])
            derivs["z"].append(datapoint["z_derivation"])

        return self._evaluate_position(
            packet=packet, means=means
        ) or self._evaluate_movement(packet=packet, derivs=derivs)

    def _evaluate_temperature(self, packet: DataPacket) -> bool:
        temperature_reference_0 = compute_temperature(packet.temperature_reference[0])
        temperature_reference_1 = compute_temperature(packet.temperature_reference[1])
        temperature_heat_0 = compute_temperature(packet.temperature_heat[0])
        temperature_heat_1 = compute_temperature(packet.temperature_heat[1])
        delta_cold = abs(temperature_heat_0 - temperature_reference_0)
        delta_hot = abs(temperature_heat_1 - temperature_reference_1)

        data: ResultSet = self.influx_client.query(
            f'SELECT "ttt_reference_probe_cold","ttt_reference_probe_hot","ttt_heat_probe_cold","ttt_heat_probe_hot" FROM "stem_temperature" WHERE time > now() - {ANALYSIS_INTERVAL}'
        )

        reference_probe_cold: List[float] = []
        reference_probe_hot: List[float] = []
        heat_probe_cold: List[float] = []
        heat_probe_hot: List[float] = []

        for datapoint in data.get_points("stem_temperature"):
            reference_probe_cold.append(datapoint["ttt_reference_probe_cold"])
            reference_probe_hot.append(datapoint["ttt_reference_probe_hot"])
            heat_probe_cold.append(datapoint["ttt_heat_probe_cold"])
            heat_probe_hot.append(datapoint["ttt_heat_probe_hot"])

        deltas_cold: List[float] = [
            abs(heat - reference)
            for heat, reference in zip(heat_probe_cold, reference_probe_cold)
        ]
        mean_delta_cold = mean(deltas_cold)
        stdev_delta_cold = stdev(deltas_cold, mean_delta_cold)

        deltas_hot: List[float] = [
            abs(heat - reference)
            for heat, reference in zip(heat_probe_hot, reference_probe_hot)
        ]
        mean_delta_hot = mean(deltas_hot)
        stdev_delta_hot = stdev(deltas_hot, mean_delta_hot)

        return (
            abs(delta_cold - mean_delta_cold) > stdev_delta_cold
            or abs(delta_hot - mean_delta_hot) > stdev_delta_hot
        )

    def evaluate(self, packet: DataPacket) -> TTCommand1:
        sleep_interval: int = max(self._evaluate_battery(packet=packet), SLEEP_TIME_MIN)

        if self._evaluate_gravity(packet=packet) or self._evaluate_temperature(
            packet=packet
        ):
            sleep_interval = SLEEP_TIME_MIN

        heating = int(sleep_interval / 6)

        return TTCommand1(
            receiver_address=packet.sender_address,
            sender_address=self.local_address,
            command=32,
            time=int(time.time()),
            sleep_interval=sleep_interval,
            unknown=(0, 45, 1),
            heating=heating,
        )


class LocalLightPolicy(Policy):
    def _evaluate_brightness(self, packet: LightSensorPacket) -> int:
        # Welche Variable enthält dies?
        pass

    def evaluate(self, packet: LightSensorPacket) -> TTCommand2:
        return TTCommand2(
            receiver_address=packet.sender_address,
            sender_address=self.local_address,
            command=33,
            time=int(time.time()),
            integration_time=50,
            gain=3,
        )
