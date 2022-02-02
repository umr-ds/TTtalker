import logging
import time

from typing import Union, Dict, List, Tuple, Any
from collections import defaultdict
from statistics import mean, stdev

from dataclasses import dataclass

import influxdb as influx
from influxdb.resultset import ResultSet

from ttt.packets import (
    DataPacketRev31,
    DataPacketRev32,
    LightSensorPacket,
    TTCommand2,
)
from ttt.util import (
    compute_temperature,
)

RDE = 1
CONFIDENCE = 3
ANALYSIS_INTERVAL = "2d"
SLEEP_TIME_MIN = 300
SLEEP_TIME_DEFAULT = 600
TIME_SLOT_LENGTH = 60
CRITICAL_TEMPERATURE = 50
ANALYSIS_TIME_SHORT = 172800  # 2 days
ANALYSIS_TIME_LONG = 604800  # 7 days
UPLOAD_DATABASE = "historical"


@dataclass
class DataPolicy:
    influx_client: influx.InfluxDBClient
    ttcloud: str
    aggregated_movement_short: Dict[str, float]
    aggregated_temperature_short: Dict[str, float]
    aggregated_movement_long: Dict[str, float]
    aggregated_temperature_long: Dict[str, float]

    def _evaluate_position(
        self, packet: DataPacketRev32, means: Dict[str, List[int]]
    ) -> Tuple[bool, Dict[str, float]]:
        for dimension, values in means.items():
            if len(values) < 2:
                logging.debug(f"Dimension {dimension} only has {len(values)} values")
                return False, {}

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

        reference = {
            "mean_x": mean_x,
            "stdev_x": stdev_x,
            "mean_y": mean_y,
            "stdev_y": stdev_y,
            "mean_z": mean_z,
            "stdev_z": stdev_z,
        }

        logging.debug(f"Detected position anomaly: {anomaly}")

        return anomaly, reference

    def _evaluate_movement(
        self, packet: DataPacketRev32, aggregated_movement: Dict[str, float]
    ) -> Tuple[bool, Dict[str, float]]:
        if not aggregated_movement:
            logging.info("Haven't received any aggregated movement data yet.")
            return False, {}

        x = packet.gravity_x_derivation
        y = packet.gravity_y_derivation
        z = packet.gravity_z_derivation

        logging.debug(
            f"Movement data: [x: {x}, y: {y}, z: {z}, aggregate: {aggregated_movement}]"
        )

        anomaly = (
            abs(x - aggregated_movement["mean_x"])
            > (aggregated_movement["stdev_x"] * CONFIDENCE)
            or abs(y - aggregated_movement["mean_y"])
            > (aggregated_movement["stdev_y"] * CONFIDENCE)
            or abs(z - aggregated_movement["mean_z"])
            > (aggregated_movement["stdev_z"] * CONFIDENCE)
        )

        logging.debug(f"Detected movement anomaly: {anomaly}")

        return anomaly, aggregated_movement

    def _evaluate_gravity(
        self,
        packet: Union[DataPacketRev31, DataPacketRev32],
        packet_time: int,
        analysis_time: int,
        aggregated_movement: Dict[str, float],
    ) -> Tuple[bool, Dict[str, Tuple[bool, Dict[str, float]]]]:
        means: Dict[str, List[int]] = defaultdict(list)

        ttcloud_database = f"{UPLOAD_DATABASE}-{self.ttcloud}"
        self.influx_client.switch_database(ttcloud_database)
        t_start = packet_time - analysis_time

        try:
            data: ResultSet = self.influx_client.query(
                f'SELECT "x_mean", "y_mean", "z_mean" FROM "gravity" WHERE time > {t_start}s AND time < {packet_time}s AND ("treetalker" = \'{packet.sender_address.address}\')'
            )
        except influx.client.InfluxDBServerError as err:
            logging.error(f"Influxdb error: {err}")
            return False, {}

        for datapoint in data.get_points("gravity"):
            means["x"].append(datapoint["x_mean"])
            means["y"].append(datapoint["y_mean"])
            means["z"].append(datapoint["z_mean"])

        if not means:
            logging.debug("No historical gravity data present.")
            return False, {}

        r_data: Dict[str, Tuple[bool, Dict[str, float]]] = {}
        anomaly_position, reference = self._evaluate_position(
            packet=packet, means=means
        )
        r_data["position"] = (anomaly_position, reference)
        anomaly_movement, reference = self._evaluate_movement(
            packet=packet, aggregated_movement=aggregated_movement
        )
        r_data["movement"] = (anomaly_movement, reference)
        anomaly = anomaly_position or anomaly_movement

        logging.debug(f"Detected gravity anomaly: {anomaly}")

        return anomaly, r_data

    def _evaluate_air_temperature(
        self, packet: Union[DataPacketRev31, DataPacketRev32]
    ) -> Tuple[bool, Dict[str, int]]:
        anomaly = packet.air_temperature >= CRITICAL_TEMPERATURE * 10
        logging.debug(f"Found air temperature anomaly: {anomaly}")
        reference: Dict[str, int] = {
            "measured": packet.air_temperature,
            "threshold": CRITICAL_TEMPERATURE * 10,
        }
        return anomaly, reference

    def _evaluate_stem_temperature(
        self,
        packet: Union[DataPacketRev31, DataPacketRev32],
        packet_time: int,
        analysis_time: int,
        aggregated_temperature: Dict[str, float],
    ) -> Tuple[bool, Dict[str, Union[float, Dict[str, float]]]]:
        logging.debug("Evaluating stem temperature")
        if not aggregated_temperature:
            logging.info("Haven't received any aggregated temperature data yet.")
            return False, {}
        else:
            logging.debug(f"Aggregated temperature data: {aggregated_temperature}")

        ttcloud_database = f"{UPLOAD_DATABASE}-{self.ttcloud}"
        self.influx_client.switch_database(ttcloud_database)
        t_start = packet_time - analysis_time

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
                f'SELECT "ttt_reference_probe_cold", "ttt_reference_probe_hot", "ttt_heat_probe_cold", "ttt_heat_probe_hot" FROM "stem_temperature" WHERE time > {t_start}s AND time < {packet_time}s AND ("treetalker" = \'{packet.sender_address.address}\')'
            )
        except influx.client.InfluxDBServerError as err:
            logging.error(f"Influxdb error: {err}")
            return False, {}

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
            return False, {}

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
            aggregated_temperature["stdev_delta_cold"] * CONFIDENCE
        ) or abs(delta_hot - mean_delta_hot) > (
            aggregated_temperature["stdev_delta_hot"] * CONFIDENCE
        )

        r_data: Dict[str, Union[float, Dict[str, float]]] = {
            "delta_cold": delta_cold,
            "delta_hot": delta_hot,
            "mean_delta_cold": mean_delta_cold,
            "mean_delta_hot": mean_delta_hot,
            "aggregated": aggregated_temperature,
        }

        logging.debug(f"Detected temperature anomaly: {anomaly}")

        return anomaly, r_data

    def check_anomaly(
        self, packet: Union[DataPacketRev31, DataPacketRev32], packet_time: int
    ) -> Dict[str, Any]:
        anomalies: Dict[str, Any] = {}

        logging.debug(f"Checking gravity data")
        gravity_anomaly, reference = self._evaluate_gravity(
            packet=packet,
            packet_time=packet_time,
            analysis_time=ANALYSIS_TIME_SHORT,
            aggregated_movement=self.aggregated_movement_short,
        )
        if gravity_anomaly:
            logging.debug("Detected gravity anomaly")
            anomalies["gravity"] = reference

        logging.debug(f"Checking stem temperature")
        stem_temperature_anomaly, reference = self._evaluate_stem_temperature(
            packet=packet,
            packet_time=packet_time,
            analysis_time=ANALYSIS_TIME_SHORT,
            aggregated_temperature=self.aggregated_temperature_short,
        )
        if stem_temperature_anomaly:
            logging.debug("Detected stem temperature anomaly")
            anomalies["stem temperature"] = reference

        return anomalies

    def check_critical(
        self, packet: Union[DataPacketRev31, DataPacketRev32], packet_time: int
    ) -> Dict[str, Any]:
        anomalies: Dict[str, Any] = {}

        logging.debug(f"Checking gravity data")
        gravity_anomaly, reference = self._evaluate_gravity(
            packet=packet,
            packet_time=packet_time,
            analysis_time=ANALYSIS_TIME_LONG,
            aggregated_movement=self.aggregated_movement_long,
        )
        if gravity_anomaly:
            logging.debug("Detected critical gravity anomaly")
            anomalies["gravity"] = reference

        logging.debug(f"Checking stem temperature")
        stem_temperature_anomaly, reference = self._evaluate_stem_temperature(
            packet=packet,
            packet_time=packet_time,
            analysis_time=ANALYSIS_TIME_LONG,
            aggregated_temperature=self.aggregated_temperature_long,
        )
        if stem_temperature_anomaly:
            logging.debug("Detected critical stem temperature anomaly")
            anomalies["stem temperature"] = reference

        logging.debug(f"Checking air temperature")
        air_temperature_anomaly, reference = self._evaluate_air_temperature(
            packet=packet
        )
        if air_temperature_anomaly:
            logging.debug("Detected critical air temperature anomaly")
            anomalies["air temperature"] = reference

        return anomalies


@dataclass
class LightPolicy:
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
