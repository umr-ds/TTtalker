#! /usr/bin/env python3
"""
This program simply listens on certain mqtt topics and stores the results in the influxdb
"""

from __future__ import annotations

import argparse
import logging

from typing import Any
from base64 import b64decode
from time import sleep

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
import influxdb as influx

from ttt.packets import (
    TTPacket,
    DataPacketRev32,
    DataPacketRev31,
    LightSensorPacket,
    unmarshall,
)


class DataArchiver:
    def __init__(
        self, broker_address: str, broker_port: int, influx_address: str
    ) -> None:
        self.mqtt_client = mqtt.Client(client_id="archiver", protocol=mqtt.MQTTv5)
        self.mqtt_client.connect(host=broker_address, port=broker_port)
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.subscribe("receive/#")

        self.influx_client = influx.InfluxDBClient(host=influx_address, port=8086)

    def __enter__(self) -> DataArchiver:
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

    def on_message(
        self, client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage
    ) -> None:
        logging.debug(f"Received MQTT Message on topic {message.topic}")

        packet: TTPacket = unmarshall(b64decode(message.payload))
        logging.debug(f"Unamarshalled packet: {packet}")

        if not (
            isinstance(packet, DataPacketRev31)
            or isinstance(packet, DataPacketRev32)
            or isinstance(packet, LightSensorPacket)
        ):
            return

        packet_data = packet.to_influx_json()
        logging.debug(f"Sending data to influx: {packet_data}")

        try:
            self.influx_client.write_points(packet_data)
        except influx.client.InfluxDBServerError as err:
            logging.error(f"Influxdb error: {err}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process some integers.")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument(
        "-b", "--broker", help="Address of the MQTT broker", default="127.0.0.1"
    )
    parser.add_argument(
        "-bp",
        "--broker-port",
        help="Port of the MQTT broker",
        default=1883,
        type=int,
        dest="broker_port",
    )
    parser.add_argument(
        "-i", "--influx", help="Address of the influxdb", default="127.0.0.1"
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

    with DataArchiver(
        broker_address=args.broker,
        broker_port=args.broker_port,
        influx_address=args.influx,
    ):
        while True:
            sleep(1)
