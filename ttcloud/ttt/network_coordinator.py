#! /usr/bin/env python3
"""
This component assigns treetalkers to ttclouds
"""

from __future__ import annotations

import argparse
import logging
import json

from typing import Any, Dict, Union
from time import sleep

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes


class Coordinator:
    def __init__(self, broker_address: str, broker_port: int) -> None:
        self.mqtt_client = mqtt.Client(client_id="coordinator", protocol=mqtt.MQTTv5)
        self.mqtt_client.connect(host=broker_address, port=broker_port)
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.subscribe("helo/request")

        # keys: tt_address, val: cloud_address
        self.assignments: Dict[int, int] = {}

    def __enter__(self) -> Coordinator:
        logging.info("Starting coordinator")
        self.mqtt_client.loop_start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        logging.info("Stopping coordinator")
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect(
            reasoncode=mqtt.ReasonCodes(
                packetType=PacketTypes.DISCONNECT, aName="Normal disconnection"
            )
        )

    def on_message(
        self, client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage
    ) -> None:
        logging.debug(f"Received MQTT Message on topic {message.topic}")

        request: Dict[str, int] = json.loads(message.payload)
        logging.debug(f"Received connection request: {request}")
        cloud_address = request["cloud_address"]
        tt_address = request["tt_address"]

        assignment: Union[int, None] = self.assignments.get(tt_address)
        logging.debug(f"Assignment for tt {tt_address}: {assignment}")

        if assignment is None:
            connect = True
            self.assignments[tt_address] = cloud_address
        else:
            connect = assignment == cloud_address

        logging.debug(f"Should connect: {connect}")

        response: Dict[str, Union[int, bool]] = {
            "tt_address": tt_address,
            "connect": connect,
        }

        logging.debug(f"Sending response: {response}")

        self.mqtt_client.publish(
            topic=f"helo/response/{cloud_address}", payload=json.dumps(response)
        )


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

    with Coordinator(
        broker_address=args.broker,
        broker_port=args.broker_port,
    ):
        while True:
            sleep(0.5)
