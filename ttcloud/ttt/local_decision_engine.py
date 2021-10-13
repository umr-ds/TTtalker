#! /usr/bin/env python3

from __future__ import annotations
import argparse
import logging
import time

from typing import Any, Dict, Callable, Union
from base64 import b64encode, b64decode

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
import influxdb as influx

from ttt.packets import *
from ttt.policy import Policy, LocalDataPolicy, LocalLightPolicy


class LDE:
    def __init__(self, broker_address: str, influx_address: str, address: TTAddress):
        self.address = address

        self.mqtt_client = mqtt.Client("lde")
        self.mqtt_client.connect(broker_address)
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.subscribe("receive/#")

        self.influx_client = influx.InfluxDBClient(host=influx_address, port=8086)

        self.data_policy = LocalDataPolicy(
            local_address=address, influx_client=self.influx_client
        )

        self.light_policy = LocalLightPolicy(
            local_address=address, influx_client=self.influx_client
        )

    def __enter__(self) -> LDE:
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
        logging.debug("Received MQTT Message")

        if "receive" in message.topic:
            self._handle_packet(message)
        else:
            logging.error(f"Received message from unknown topic {message.topic}")

    def _handle_packet(self, message: mqtt.MQTTMessage) -> None:
        packet: TTPacket = unmarshall(b64decode(message.payload))
        logging.debug(f"Unamarshalled packet: {packet}")

        if isinstance(packet, TTHeloPacket):
            reply = self._on_helo(packet=packet)
        elif isinstance(packet, DataPacket):
            reply = self._on_data(packet=packet)
        elif isinstance(packet, LightSensorPacket):
            reply = self._on_light(packet=packet)
        else:
            logging.error("Unsupported packet type")
            return

        logging.debug(f"Reply: {reply}")
        self.mqtt_client.publish(topic="command", payload=b64encode(reply.marshall()))

    def _on_helo(self, packet: TTHeloPacket) -> TTCloudHeloPacket:
        return TTCloudHeloPacket(
            receiver_address=packet.sender_address,
            sender_address=self.address,
            command=190,
            time=int(time.time()),
        )

    def _on_data(self, packet: DataPacket) -> TTPacket:
        reply = self.data_policy.evaluate(packet)

        packet_data = packet.to_influx_json()
        logging.debug(f"Sending data to influx: {packet_data}")
        self.influx_client.write_points(packet_data)

        return reply

    def _on_light(self, packet: LightSensorPacket) -> TTPacket:
        reply = self.light_policy.evaluate(packet)

        packet_data = packet.to_influx_json()
        logging.debug(f"Sending data to influx: {packet_data}")
        self.influx_client.write_points(packet_data)

        return reply

    def start(self):
        logging.info("Starting Local Decision Engine")
        while True:
            time.sleep(0.5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process some integers.")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument(
        "-b", "--broker", help="Address of the MQTT broker", default="localhost"
    )
    parser.add_argument(
        "-i", "--influx", help="Address of the influxdb", default="localhost"
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

    with LDE(
        broker_address=args.broker,
        influx_address=args.influx,
        address=TTAddress(3254976792),
    ) as lde:
        lde.start()
