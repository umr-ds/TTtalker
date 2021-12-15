#! /usr/bin/env python3

from __future__ import annotations
import argparse
import logging
import time

from typing import Any, Dict, Tuple
from base64 import b64decode

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
import influxdb as influx

from ttt.address import TTAddress
from ttt.packets import (
    unmarshall,
    TTPacket,
    DataPacketRev31,
    DataPacketRev32,
    LightSensorPacket,
    TTCommand1,
    TTCommand2,
)


class ResponseAnalyser:
    def __init__(
        self, broker_address: str, broker_port: int, influx_address: str
    ) -> None:
        self.mqtt_client = mqtt.Client(
            client_id="response-analyser", protocol=mqtt.MQTTv5
        )
        self.mqtt_client.connect(host=broker_address, port=broker_port)
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.subscribe("sniffer/#")

        self.influx_client = influx.InfluxDBClient(host=influx_address, port=8086)

        self.waiting_for_reply: Dict[Tuple[TTAddress, str], float] = {}

    def __enter__(self) -> ResponseAnalyser:
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
        logging.debug(f"Received message on {message.topic}")

        if "ttcloud" in message.topic:
            logging.debug("Received command from ttcloud")
            self._handle_receive(message, responder="ttcloud")
        elif "ttt" in message.topic:
            logging.debug("Received command from ttt")
            self._handle_receive(message, responder="ttt")
        else:
            logging.error(f"Received message from unknown MQTT-topic: {message.topic}")

    def _response_time(
        self, packet: TTPacket, responder: str, now: float, packet_type: str
    ) -> None:
        try:
            request_time = self.waiting_for_reply[packet.receiver_address, packet_type]
            response_time = now - request_time
        except KeyError:
            logging.error(
                f"Didn't receive any data packet from {packet.receiver_address}"
            )
            return

        logging.info(
            f"Answer to {packet.receiver_address} from {responder}: {response_time}"
        )

        influx_json = [
            {
                "measurement": "response_time",
                "tags": {
                    "treetalker": packet.receiver_address.address,
                    "responder": responder,
                    "packet_type": packet_type,
                },
                "fields": {"response_time": response_time},
            },
        ]

        logging.debug(f"Sending data to influx: {influx_json}")

        try:
            self.influx_client.write_points(influx_json)
        except influx.client.InfluxDBServerError as err:
            logging.error(f"Influxdb error: {err}")

    def _handle_receive(self, message: mqtt.MQTTMessage, responder: str) -> None:
        packet: TTPacket = unmarshall(b64decode(message.payload))
        now = time.time()

        if isinstance(packet, TTCommand1):
            self._response_time(
                packet=packet, responder=responder, now=now, packet_type="data"
            )
        elif isinstance(packet, TTCommand2):
            self._response_time(
                packet=packet, responder=responder, now=now, packet_type="light"
            )
        elif isinstance(packet, DataPacketRev31) or isinstance(packet, DataPacketRev32):
            self.waiting_for_reply[packet.sender_address, "data"] = now
        elif isinstance(packet, LightSensorPacket):
            self.waiting_for_reply[packet.sender_address, "light"] = now
        else:
            logging.debug(f"Not interested in packet type: {packet.__class__.__name__}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Measure & log response time for packets"
    )
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

    with ResponseAnalyser(
        broker_address=args.broker,
        broker_port=args.broker_port,
        influx_address=args.influx,
    ):
        while True:
            time.sleep(1)
