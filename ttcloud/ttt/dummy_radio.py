#! /usr/bin/env python3

from __future__ import annotations
import argparse
import logging
import time
import random
import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes

from typing import Any
from base64 import b64encode, b64decode

from ttt.packets import (
    unmarshall,
    SAMPLE_PACKETS,
    TTCloudHeloPacket,
    DataPacket,
    LightSensorPacket,
    TTCommand1,
    TTCommand2,
)


class DummyRadio:
    def __init__(self, broker_address: str):
        self.mqtt_client = mqtt.Client("rci")
        self.mqtt_client.connect(broker_address)
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.subscribe("command")

        self.initialised = False

    def __enter__(self) -> DummyRadio:
        self.mqtt_client.loop_start()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect(
            reasoncode=mqtt.ReasonCodes(
                packetType=PacketTypes.DISCONNECT, aName="Normal disconnection"
            )
        )

    def on_message(
        self, client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage
    ) -> None:
        packet = unmarshall(b64decode(message.payload))
        if isinstance(packet, TTCloudHeloPacket):
            logging.debug("Received TTCloudHeloPacket")
            self.initialised = True
        elif isinstance(packet, TTCommand1):
            logging.debug("Recieved data command")
        elif isinstance(packet, TTCommand2):
            logging.debug("Received light-sensor command")

    def start(self):
        logging.debug("Started dummy radio")
        packet = SAMPLE_PACKETS["TTHeloPacket"]
        logging.debug("Sending HELO-Packet")
        self.mqtt_client.publish(
            topic=f"receive/{packet.__class__.__name__}",
            payload=b64encode(packet.marshall()),
        )

        while not self.initialised:
            time.sleep(1)
            logging.debug("Endpoint not yet initialised")

        logging.debug("Endpoint initialised")

        logging.debug("Entering data-send loop")
        while True:
            sleep_time = random.randint(10, 60)
            logging.debug(f"Waiting for {sleep_time} seconds")
            time.sleep(sleep_time)

            packet = random.choice(
                [SAMPLE_PACKETS["DataPacket"], SAMPLE_PACKETS["LightSensorPacket"]]
            )
            assert isinstance(packet, DataPacket) or isinstance(
                packet, LightSensorPacket
            )
            packet.time = int(time.time())
            logging.debug(f"Sending {packet.__class__.__name__}")
            self.mqtt_client.publish(
                topic=f"receive/{packet.__class__.__name__}",
                payload=b64encode(packet.marshall()),
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Component sends dummy packets via MQTT to test other components"
    )
    parser.add_argument(
        "-b", "--broker", help="Address of the MQTT broker", default="127.0.0.1"
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)

    with DummyRadio(broker_address=args.broker) as radio_interface:
        radio_interface.start()
