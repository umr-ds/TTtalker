#! /usr/bin/env python3
"""
This program aggregates data from all treetalkertalkertalkertalkertalkers to find similarities across stations
"""

from __future__ import annotations

import argparse
import logging
import time
import json

from typing import Dict, List
from statistics import mean, stdev

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
import influxdb as influx
from influxdb.resultset import ResultSet

from ttt.policy import ANALYSIS_INTERVAL


SLEEP_TIME = 600


class Aggregator:
    def __init__(
        self, broker_address: str, broker_port: int, influx_address: str
    ) -> None:
        self.mqtt_client = mqtt.Client(client_id="aggregator", protocol=mqtt.MQTTv5)
        self.mqtt_client.connect(host=broker_address, port=broker_port)

        self.influx_client = influx.InfluxDBClient(host=influx_address, port=8086)

    def __enter__(self) -> Aggregator:
        self.mqtt_client.loop_start()
        self.influx_client.create_database("ttt")
        self.influx_client.switch_database("ttt")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect(
            reasoncode=mqtt.ReasonCodes(
                packetType=PacketTypes.DISCONNECT, aName="Normal disconnection"
            )
        )
        self.influx_client.close()

    def _aggregate_movement(self) -> Dict[str, float]:
        logging.info("Aggregating movement data")

        x_derivs: List[int] = []
        y_derivs: List[int] = []
        z_derivs: List[int] = []

        try:
            data: ResultSet = self.influx_client.query(
                f'SELECT "x_derivation", "y_derivation", "z_derivation" FROM "gravity" WHERE time > now() - {ANALYSIS_INTERVAL}'
            )
        except influx.client.InfluxDBServerError as err:
            logging.error(f"Influxdb error: {err}")
            return {}

        for datapoint in data.get_points("gravity"):
            x_derivs.append(datapoint["x_derivation"])
            y_derivs.append(datapoint["y_derivation"])
            z_derivs.append(datapoint["z_derivation"])

        if not x_derivs or not y_derivs or not z_derivs:
            logging.debug(
                f"No movement data: [x: {len(x_derivs)}, y: {len(y_derivs)}, z: {len(z_derivs)}]"
            )
            return {}

        mean_x = mean(x_derivs)
        stdev_x = stdev(x_derivs, mean_x)
        mean_y = mean(y_derivs)
        stdev_y = stdev(y_derivs, mean_y)
        mean_z = mean(z_derivs)
        stdev_z = stdev(z_derivs, mean_z)

        aggregated = {
            "mean_x": mean_x,
            "stdev_x": stdev_x,
            "mean_y": mean_y,
            "stdev_y": stdev_y,
            "mean_z": mean_z,
            "stdev_z": stdev_z,
        }

        logging.debug(f"Aggreagated movement data: {aggregated}")

        return aggregated

    def _aggregate_temperature(self) -> Dict[str, float]:
        logging.info("Aggregating temperature data")

        try:
            data: ResultSet = self.influx_client.query(
                f'SELECT "ttt_reference_probe_cold","ttt_reference_probe_hot","ttt_heat_probe_cold","ttt_heat_probe_hot" FROM "stem_temperature" WHERE time > now() - {ANALYSIS_INTERVAL}'
            )
        except influx.client.InfluxDBServerError as err:
            logging.error(f"Influxdb error: {err}")
            return {}

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
                f"No temperature data: [ref_cold: {len(reference_probe_cold)}, ref_hot: {len(reference_probe_hot)}, heat_cold: {len(heat_probe_cold)}, heat_hot: {len(heat_probe_hot)}]"
            )
            return {}

        deltas_cold: List[float] = [
            abs(heat - reference)
            for heat, reference in zip(heat_probe_cold, reference_probe_cold)
        ]
        stdev_delta_cold = stdev(deltas_cold)

        deltas_hot: List[float] = [
            abs(heat - reference)
            for heat, reference in zip(heat_probe_hot, reference_probe_hot)
        ]
        stdev_delta_hot = stdev(deltas_hot)

        aggregated_data = {
            "stdev_delta_cold": stdev_delta_cold,
            "stdev_delta_hot": stdev_delta_hot,
        }

        logging.debug(f"Aggreagated temperature data: {aggregated_data}")

        return aggregated_data

    def start(self) -> None:
        logging.info("Starting data aggregation")

        while True:
            aggregated_movement = self._aggregate_movement()
            if aggregated_movement:
                logging.debug(f"Sending movement data: {aggregated_movement}")
                self.mqtt_client.publish(
                    "global/movement", payload=json.dumps(aggregated_movement)
                )
            else:
                logging.debug("No movement data to send")

            aggregated_temperature = self._aggregate_temperature()
            if aggregated_temperature:
                logging.debug(f"Sending temperature data: {aggregated_temperature}")
                self.mqtt_client.publish(
                    "global/temperature", payload=json.dumps(aggregated_temperature)
                )
            else:
                logging.debug("No temperature data to send")

            time.sleep(SLEEP_TIME)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process some integers.")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument(
        "-b", "--broker", help="Address of the MQTT broker", default="localhost"
    )
    parser.add_argument(
        "-i", "--influx", help="Address of the influxdb", default="localhost"
    )
    parser.add_argument(
        "-bp",
        "--broker-port",
        help="Port of the MQTT broker",
        default=1883,
        type=int,
        dest="broker_port",
    )
    args = parser.parse_args()

    if args.verbose:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with Aggregator(
        broker_address=args.broker,
        broker_port=args.broker_port,
        influx_address=args.influx,
    ) as aggregator:
        aggregator.start()
