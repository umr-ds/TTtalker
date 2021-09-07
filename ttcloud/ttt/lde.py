#! /usr/bin/env python3

from __future__ import annotations
import argparse
import logging
import time

from typing import Any
from base64 import b64encode, b64decode

import paho.mqtt.client as mqtt

from ttt.packets import *


class LDE:
    def __init__(self, broker_address: str, address: TTAddress):
        self.address = address

        self.mqtt_client = mqtt.Client("lde")
        self.mqtt_client.connect(broker_address)
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.subscribe("receive/#")

    def __enter__(self) -> LDE:
        self.mqtt_client.loop_start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.mqtt_client.loop_stop()

    def on_message(
        self, client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage
    ) -> None:
        logging.debug("Received Packet via MQTT")
        packet: TTPacket = unmarshall(b64decode(message.payload))
        logging.debug(f"Unamarshalled packet: {packet}")

        if isinstance(packet, TTHeloPacket):
            reply = self.on_helo(packet=packet)
        elif isinstance(packet, DataPacket2):
            reply = self.on_data2(packet=packet)
        elif isinstance(packet, LightSensorPacket):
            reply = self.on_light(packet=packet)
        else:
            logging.error("Unknown packet type")
            return

        logging.debug(f"Reply: {reply}")
        self.mqtt_client.publish(topic="command", payload=b64encode(reply.marshall()))

    def on_helo(self, packet: TTHeloPacket) -> TTCloudHeloPacket:
        return TTCloudHeloPacket(
            receiver_address=packet.sender_address,
            sender_address=self.address,
            command=190,
            time=int(time.time()),
        )

    def on_data2(self, packet: DataPacket2) -> TTCommand1:
        return TTCommand1(
            receiver_address=packet.sender_address,
            sender_address=packet.receiver_address,
            command=32,
            time=int(time.time()),
            sleep_intervall=60,
            unknown=(0, 45, 1),
            heating=30,
        )

    def on_light(self, packet: LightSensorPacket) -> TTCommand2:
        return TTCommand2(
            receiver_address=packet.sender_address,
            sender_address=packet.receiver_address,
            command=33,
            time=int(time.time()),
            integration_time=50,
            gain=3,
        )

    def start(self):
        logging.info("Starting Local Decision Engine")
        while True:
            time.sleep(0.5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process some integers.")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument(
        "-b", "--broker", help="Address of the MQTT broker", default="127.0.0.1"
    )
    args = parser.parse_args()

    if args.verbose:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    logging.basicConfig(level=log_level)

    with LDE(broker_address=args.broker, address=TTAddress(3254976792)) as lde:
        lde.start()
