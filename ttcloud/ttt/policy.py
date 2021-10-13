import json
import time

from typing import Tuple, Union, Dict, List
from collections import defaultdict
from statistics import mean, stdev

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
analysis_interval = "2d"


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
            f'SELECT "ttt_voltage" FROM "power" WHERE time > now() - {analysis_interval}'
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

        # TODO: discuss: evtl sinnig dies in eine extra funktion auszulagern, jedoch wegen der brisanz des Stromverbrauches mit erhöhter Priorität zu den anderen eingehenden Parametern?
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

    def _evaluate_brightness(self, packet: DataPacket) -> int:
        # Welche Variable enthält dies?
        pass

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
            f'SELECT "x_mean", "x_derivation", "y_mean", "y_derivation", "z_mean", "z_derivation" FROM "gravity" WHERE time > now() - {analysis_interval}'
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

    # return: Gesamtabweichung - Grenzwerte müssen festgelegt werden - Tuple: referenzabweichung, vorheizung delta, nachheizung delta
    # Idee: referenzabweichung am wichtigsten für umgebungswerte, vorheizung delta erkennt ob sensoren defekt, nachheizung delta ob baum tot
    # timelim sollte 1d, 2d und 5d sein, sollte in evaluate funktion mehrfach aufgerufen werden
    def _evaluate_temperatures(self, packet: DataPacket) -> Tuple[int, int, int]:
        temperature_reference_0 = compute_temperature(packet.temperature_reference[0])
        temperature_reference_1 = compute_temperature(packet.temperature_reference[1])
        temperature_heat_0 = compute_temperature(packet.temperature_heat[0])
        temperature_heat_1 = compute_temperature(packet.temperature_heat[1])
        delta_heat_pre = abs(temperature_heat_0 - temperature_reference_0)
        delta_heat_post = abs(temperature_heat_1 - temperature_reference_1)

        # Welcher Sensor speist "air_temperature"? erstmal weggelassen

        data: ResultSet = self.influx_client.query(
            f'SELECT "ttt_reference_probe_cold","ttt_reference_probe_hot","ttt_heat_probe_cold","ttt_heat_probe_hot" FROM "stem_temperature" WHERE time > now() - {analysis_interval}'
        )

        x = 0
        sum_delta_heat_pre = 0
        sum_delta_heat_post = 0
        sum_heat_reference = 0

        for datapoint in data.get_points("stem_temperature"):
            x = x + 1
            sum_delta_heat_pre = sum_delta_heat_pre + abs(
                datapoint["ttt_heat_probe_cold"] - datapoint["ttt_reference_probe_cold"]
            )
            sum_delta_heat_post = sum_delta_heat_post + abs(
                datapoint["ttt_heat_probe_hot"] - datapoint["ttt_reference_probe_hot"]
            )
            sum_heat_reference = sum_heat_reference + (
                (
                    abs(datapoint["ttt_heat_probe_cold"])
                    + abs(datapoint["ttt_reference_probe_hot"])
                    + abs(datapoint["ttt_reference_probe_cold"])
                )
                / 3
            )

        abw_referenz = (
            (
                abs(temperature_heat_0)
                + abs(temperature_reference_0)
                + abs(temperature_reference_1)
            )
            / 3
        ) - (sum_heat_reference / x)
        abw_delta_heat_pre = delta_heat_pre - (sum_delta_heat_pre / x)
        abw_delta_heat_post = delta_heat_post - (sum_delta_heat_post / x)
        return abw_referenz, abw_delta_heat_pre, abw_delta_heat_post

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
