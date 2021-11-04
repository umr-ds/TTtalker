import logging
import time

from typing import Union, Dict, List
from base64 import b64encode
from collections import defaultdict
from statistics import mean, stdev

from dataclasses import dataclass

import paho.mqtt.client as mqtt
import influxdb as influx
from influxdb.resultset import ResultSet
from sklearn.linear_model import LinearRegression

from ttt.packets import (
    DataPacketRev31,
    DataPacketRev32,
    LightSensorPacket,
    TTCommand1,
    TTCommand2,
)
from ttt.util import (
    compute_temperature,
    compute_battery_voltage_rev_3_1,
    compute_battery_voltage_rev_3_2,
)
from ttt.address import TTAddress

RDE = 1
CONFIDENCE = 3
ANALYSIS_INTERVAL = "2d"
SLEEP_TIME_MIN = 300
SLEEP_TIME_DEFAULT = 600
TIME_SLOT_LENGTH = 60
CRITICAL_TEMPERATURE = 50


@dataclass
class DataPolicy:
    local_address: TTAddress
    mqtt_client: mqtt.Client
    influx_client: influx.InfluxDBClient
    sleep_times: Dict[int, int]
    aggregated_movement: Dict[str, float]
    aggregated_temperature: Dict[str, float]

    def _evaluate_battery_3_2(self, packet: DataPacketRev32) -> int:
        battery_voltage = compute_battery_voltage_rev_3_2(
            adc_volt_bat=packet.adc_volt_bat, adc_bandgap=packet.adc_bandgap
        )
        return self._evaluate_battery(
            sender_address=packet.sender_address.address,
            battery_voltage=battery_voltage,
        )

    def _evaluate_battery_3_1(self, packet: DataPacketRev31) -> int:
        battery_voltage = compute_battery_voltage_rev_3_1(voltage=packet.voltage)
        return self._evaluate_battery(
            sender_address=packet.sender_address.address,
            battery_voltage=battery_voltage,
        )

    def _evaluate_battery(self, sender_address: int, battery_voltage: float) -> int:
        try:
            data: ResultSet = self.influx_client.query(
                f'SELECT "ttt_voltage" FROM "power" WHERE "time" > now() - {ANALYSIS_INTERVAL} AND ("treetalker" = \'{sender_address}\')'
            )
        except influx.client.InfluxDBServerError as err:
            logging.error(f"Influxdb error: {err}")
            return SLEEP_TIME_DEFAULT

        times = []
        voltages = []
        for datapoint in data.get_points("power"):
            timestamp = int(
                time.mktime(time.strptime(datapoint["time"], "%Y-%m-%dT%H:%M:%S.%fZ"))
            )
            times.append([timestamp])
            voltages.append(datapoint["ttt_voltage"])

        if not times or not voltages:
            logging.debug(
                f"No data to compute regression: [times: {len(times)}, voltages: {len(voltages)}]"
            )
            return SLEEP_TIME_DEFAULT

        logging.debug(
            f"Historical battery data from influx: [times: {len(times)}, voltages: {len(voltages)}]"
        )

        times.append([int(time.time())])
        voltages.append(battery_voltage)

        reg: LinearRegression = LinearRegression().fit(times, voltages)
        logging.debug(
            f"Linear regression: [Coefficients: {reg.coef_}, intercept: {reg.intercept_}]"
        )

        sleep_time = self.sleep_times.get(sender_address, SLEEP_TIME_DEFAULT)
        logging.debug(f"Retrieved sleep time: {sleep_time}")

        sleep_time = int(
            sleep_time
            + (RDE * (3700 - reg.predict([[int(time.time()) + (3600 * 48)]])[0]))
        )
        logging.debug(f"Computed sleep time: {sleep_time}")

        self.sleep_times[sender_address] = sleep_time

        return sleep_time

    def _evaluate_position(
        self, packet: DataPacketRev32, means: Dict[str, List[int]]
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

        logging.debug(
            f"Position data: [mean_x: {mean_x}, stdev_x: {stdev_x}, mean_y: {mean_y}, stdev_y: {stdev_y}, mean_z: {mean_z}, stdev_z: {stdev_z}, x: {x}, y: {y}, z: {z}]"
        )

        anomaly = (
            abs(x - mean_x) > (stdev_x * CONFIDENCE)
            or abs(y - mean_y) > (stdev_y * CONFIDENCE)
            or abs(z - mean_z) > (stdev_z * CONFIDENCE)
        )

        logging.debug(f"Detected position anomaly: {anomaly}")

        return anomaly

    def _evaluate_movement(self, packet: DataPacketRev32) -> bool:
        if not self.aggregated_movement:
            logging.info("Haven't received any aggregated movement data yet.")
            return False

        x = packet.gravity_x_derivation
        y = packet.gravity_y_derivation
        z = packet.gravity_z_derivation

        logging.debug(
            f"Movement data: [x: {x}, y: {y}, z: {z}, aggregate: {self.aggregated_movement}]"
        )

        anomaly = (
            abs(x - self.aggregated_movement["mean_x"])
            > (self.aggregated_movement["stdev_x"] * CONFIDENCE)
            or abs(y - self.aggregated_movement["mean_y"])
            > (self.aggregated_movement["stdev_y"] * CONFIDENCE)
            or abs(z - self.aggregated_movement["mean_z"])
            > (self.aggregated_movement["stdev_z"] * CONFIDENCE)
        )

        logging.debug(f"Detected movement anomaly: {anomaly}")

        return anomaly

    def _evaluate_gravity(
        self, packet: Union[DataPacketRev31, DataPacketRev32]
    ) -> bool:
        means: Dict[str, List[int]] = defaultdict(list)

        try:
            data: ResultSet = self.influx_client.query(
                f'SELECT "x_mean", "y_mean", "z_mean" FROM "gravity" WHERE "time" > now() - {ANALYSIS_INTERVAL} AND ("treetalker" = \'{packet.sender_address.address}\')'
            )
        except influx.client.InfluxDBServerError as err:
            logging.error(f"Influxdb error: {err}")
            return False

        for datapoint in data.get_points("gravity"):
            means["x"].append(datapoint["x_mean"])
            means["y"].append(datapoint["y_mean"])
            means["z"].append(datapoint["z_mean"])

        if not means:
            logging.debug("No historical gravity data present.")
            return False

        anomaly = self._evaluate_position(
            packet=packet, means=means
        ) or self._evaluate_movement(packet=packet)

        logging.debug(f"Detected gravity anomaly: {anomaly}")

        return anomaly

    def _evaluate_air_temperature(
        self, packet: Union[DataPacketRev31, DataPacketRev32]
    ) -> bool:
        anomaly = packet.air_temperature >= CRITICAL_TEMPERATURE * 10
        logging.debug(f"Found air temperature anomaly: {anomaly}")
        return anomaly

    def _evaluate_stem_temperature(
        self, packet: Union[DataPacketRev31, DataPacketRev32]
    ) -> bool:
        logging.info("Evaluating stem temperature")
        if not self.aggregated_temperature:
            logging.info("Haven't received any aggregated temperature data yet.")
            return False
        else:
            logging.debug(f"Aggregated temperature data: {self.aggregated_temperature}")

        temperature_reference_cold = compute_temperature(
            packet.temperature_reference_cold
        )
        temperature_reference_hot = compute_temperature(
            packet.temperature_reference_hot
        )
        temperature_heat_cold = compute_temperature(packet.temperature_heat_cold)
        temperature_heat_hot = compute_temperature(packet.temperature_heat_hot)
        delta_cold = abs(temperature_heat_cold - temperature_reference_cold)
        delta_hot = abs(temperature_heat_hot - temperature_reference_hot)

        try:
            data: ResultSet = self.influx_client.query(
                f'SELECT "ttt_reference_probe_cold", "ttt_reference_probe_hot", "ttt_heat_probe_cold", "ttt_heat_probe_hot" FROM "stem_temperature" WHERE "time" > now() - {ANALYSIS_INTERVAL} AND ("treetalker" = \'{packet.sender_address.address}\')'
            )
        except influx.client.InfluxDBServerError as err:
            logging.error(f"Influxdb error: {err}")
            return False

        reference_probe_cold: List[float] = []
        reference_probe_hot: List[float] = []
        heat_probe_cold: List[float] = []
        heat_probe_hot: List[float] = []

        for datapoint in data.get_points("stem_temperature"):
            reference_probe_cold.append(datapoint["ttt_reference_probe_cold"])
            reference_probe_hot.append(datapoint["ttt_reference_probe_hot"])
            heat_probe_cold.append(datapoint["ttt_heat_probe_cold"])
            heat_probe_hot.append(datapoint["ttt_heat_probe_hot"])

        if (
            not reference_probe_cold
            or not reference_probe_hot
            or not heat_probe_cold
            or not heat_probe_hot
        ):
            logging.debug(
                f"No historical temperature data present: [reference_probe_cold: {len(reference_probe_cold)}, reference_probe_hot: {len(reference_probe_hot)}, heat_probe_cold: {len(heat_probe_cold)}, heat_probe_hot: {len(heat_probe_hot)}]"
            )
            return False

        logging.debug(
            f"Historical temperature data: [reference_probe_cold: {len(reference_probe_cold)}, reference_probe_hot: {len(reference_probe_hot)}, heat_probe_cold: {len(heat_probe_cold)}, heat_probe_hot: {len(heat_probe_hot)}]"
        )

        deltas_cold: List[float] = [
            abs(heat - reference)
            for heat, reference in zip(heat_probe_cold, reference_probe_cold)
        ]
        mean_delta_cold = mean(deltas_cold)

        deltas_hot: List[float] = [
            abs(heat - reference)
            for heat, reference in zip(heat_probe_hot, reference_probe_hot)
        ]
        mean_delta_hot = mean(deltas_hot)

        anomaly = abs(delta_cold - mean_delta_cold) > (
            self.aggregated_temperature["stdev_delta_cold"] * CONFIDENCE
        ) or abs(delta_hot - mean_delta_hot) > (
            self.aggregated_temperature["stdev_delta_hot"] * CONFIDENCE
        )

        logging.debug(f"Detected temperature anomaly: {anomaly}")

        return anomaly

    def evaluate(self, packet: Union[DataPacketRev31, DataPacketRev32]) -> TTCommand1:
        logging.debug("Computing sleep time")
        if isinstance(packet, DataPacketRev31):
            sleep_interval: int = max(
                self._evaluate_battery_3_1(packet=packet), SLEEP_TIME_MIN
            )
        else:
            sleep_interval: int = max(
                self._evaluate_battery_3_2(packet=packet), SLEEP_TIME_MIN
            )

        logging.debug(f"Checking gravity data")
        gravity_anomaly = self._evaluate_gravity(packet=packet)
        if gravity_anomaly:
            logging.info("Detected gravity anomaly")
            self.mqtt_client.publish(
                f"anomaly/gravity/{self.local_address.address}",
                b64encode(packet.marshall()),
            )

        logging.debug(f"Checking stem temperature")
        stem_temperature_anomaly = self._evaluate_stem_temperature(packet=packet)
        if stem_temperature_anomaly:
            logging.info("Detected stem temperature anomaly")
            self.mqtt_client.publish(
                f"anomaly/stem_temperature/{self.local_address.address}",
                b64encode(packet.marshall()),
            )

        logging.debug(f"Checking air temperature")
        air_temperature_anomaly = self._evaluate_air_temperature(packet=packet)
        if air_temperature_anomaly:
            logging.info("Detected air temperature anomaly")
            self.mqtt_client.publish(
                f"anomaly/air_temperature/{self.local_address.address}",
                b64encode(packet.marshall()),
            )

        if gravity_anomaly or stem_temperature_anomaly or air_temperature_anomaly:
            sleep_interval = SLEEP_TIME_MIN

        heating = int(sleep_interval / 6)

        return TTCommand1(
            receiver_address=packet.sender_address,
            sender_address=self.local_address,
            command=32,
            time=int(time.time()),
            sleep_interval=sleep_interval,
            unknown=0,
            time_slot_length=TIME_SLOT_LENGTH,
            time_slot=0,
            heating=heating,
        )


@dataclass
class LightPolicy:
    local_address: TTAddress
    influx_client: influx.InfluxDBClient

    def _calculate_scaled_brightness(self, f: List[float], scal: List[float]) -> float:
        retval: float = 0
        for val, scalar in zip(f, scal):
            retval = retval + val * scalar
        return retval / 6

    def _evaluate_brightness(self, packet: LightSensorPacket) -> int:
        # Paper Referenz: https://www.mdpi.com/1424-8220/16/8/1310/htm
        # -> Airborne Optical and Thermal Remote Sensing for Wildfire Detection and Monitoring by Robert S. Allison et. al. 2016
        # Die wirklich wichtigen Frequenzen (z.B. 3µm für Hitze und 1.2nm für das Spektralband von Kaliumrauch) sind leider nicht erfasst
        # Darum Skalar bei den Infrarot - Rotbereich 860nm bis 610nm zugunsten des Infrarotbereiches
        scalar_red = [0.4, 0.5, 1, 2, 3, 5]
        scalar_blue = [1, 1, 1, 1, 1, 1]
        cur_redvalue = self._calculate_scaled_brightness(
            list(packet.AS7263.values()), scalar_red
        )
        cur_bluevalue = self._calculate_scaled_brightness(
            list(packet.AS7262.values()), scalar_blue
        )

        redvalues: List[float] = []
        bluevalues: List[float] = []
        data: ResultSet = self.influx_client.query(
            f'SELECT "610", "680", "730", "760", "810", "860" FROM "AS7263" WHERE "time" > now() - {ANALYSIS_INTERVAL} AND ("treetalker" = \'{packet.sender_address.address}\')'
        )
        for datapoint in data.get_points("AS7263"):
            redvalues.append(
                self._calculate_scaled_brightness(list(datapoint.values()), scalar_red)
            )

        # Optimierungsbedarf: beim Versuch ein Query über zwei Tables durchzuführen gab es bei mir
        # zwei Resultsets zurück mit gemischten Abfragefeldern. Hierdurch konnte ich nicht mehr einfach in List casten.
        # Darum lieber zwei Queries sofern es nicht anders schöner machbar ist.

        data2: ResultSet = self.influx_client.query(
            f'SELECT "450", "500", "550", "570", "600", "650" FROM "AS7262" WHERE "time" > now() - {ANALYSIS_INTERVAL} AND ("treetalker" = \'{packet.sender_address.address}\')'
        )
        for datapoint in data2.get_points("AS7262"):
            bluevalues.append(
                self._calculate_scaled_brightness(list(datapoint.values()), scalar_blue)
            )

        if not bluevalues or not redvalues:
            logging.debug(
                f"No historical light data present: [bluevalues: {bluevalues}, redvalues: {redvalues}]"
            )
            return False

        mean_redvalues = mean(redvalues)
        mean_bluevalues = mean(bluevalues)

        # max_dev_redvalues = max(abs(el - mean_redvalues) for el in redvalues)
        # max_dev_bluevalues = max(abs(el - mean_bluevalues) for el in bluevalues)

        return abs(cur_bluevalue - mean_bluevalues) > (
            stdev(bluevalues) * CONFIDENCE
        ) or abs(cur_redvalue - mean_redvalues) > (stdev(redvalues) * CONFIDENCE)

    def evaluate(self, packet: LightSensorPacket) -> TTCommand2:
        return TTCommand2(
            receiver_address=packet.sender_address,
            sender_address=self.local_address,
            command=33,
            time=int(time.time()),
            integration_time=50,
            gain=3,
        )
