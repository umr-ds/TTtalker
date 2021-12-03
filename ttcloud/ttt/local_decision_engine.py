#! /usr/bin/env python3

from __future__ import annotations
import argparse
import logging
import time
import json

from typing import Any, Dict, Union
from base64 import b64encode, b64decode

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
import influxdb as influx

from ttt.packets import (
    unmarshall,
    TTPacket,
    TTHeloPacket,
    TTCloudHeloPacket,
    DataPacketRev31,
    DataPacketRev32,
    LightSensorPacket,
)
from ttt.policy import DataPolicy, LightPolicy
from ttt.util import generate_tt_address
from ttt.address import TTAddress


class LDE:
    def __init__(
        self,
        broker_address: str,
        influx_address: str,
        address: TTAddress,
        respond: bool,
    ):
        self.address = address
        logging.debug(f"Own address: {self.address}")

        self.respond = respond

        self.mqtt_client = mqtt.Client(f"lde-{address}")
        self.mqtt_client.connect(broker_address)
        self.mqtt_client.on_message = self.on_message

        self.mqtt_client.subscribe("global/#")
        self.mqtt_client.subscribe(f"receive/{self.address.address}")
        self.mqtt_client.subscribe(f"helo/response/{self.address.address}")

        self.influx_client = influx.InfluxDBClient(host=influx_address, port=8086)

        self.data_policy = DataPolicy(
            local_address=address,
            mqtt_client=self.mqtt_client,
            influx_client=self.influx_client,
            sleep_times={},
            aggregated_movement={},
            aggregated_temperature={},
        )

        self.light_policy = LightPolicy(
            local_address=address, influx_client=self.influx_client
        )

        self.connected_clients: Dict[TTAddress, int] = {}
        self.time_slot = 1

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
        logging.debug(f"Received MQTT Message on topic {message.topic}")

        if "receive" in message.topic:
            self._handle_packet(message)
        elif "global" in message.topic:
            self._handle_global_state(message)
        elif "helo/response" in message.topic:
            self._handle_helo_response(message)
        else:
            logging.error(f"Received message from unknown topic {message.topic}")

    def _handle_helo_response(self, message: mqtt.MQTTMessage) -> None:
        response: Dict[str, Union[int, bool]] = json.loads(message.payload)
        logging.debug(f"Received connection response: {response}")
        connect: bool = response["connect"]
        if not connect:
            logging.debug("Backend told us not to connect")
            return

        tt_address = TTAddress(address=response["tt_address"])

        cloud_helo = TTCloudHeloPacket(
            receiver_address=tt_address,
            sender_address=self.address,
            command=190,
            time=int(time.time()),
        )

        logging.debug(f"Sending response packet: {cloud_helo}")

        if self.respond:
            self.mqtt_client.publish(
                topic=f"command/{self.address.address}",
                payload=b64encode(cloud_helo.marshall()),
            )

        self._add_tt_to_connected(address=tt_address)

    def _add_tt_to_connected(self, address: TTAddress) -> None:
        logging.debug(f"Adding tt {address} to connected nodes")

        if address in self.connected_clients:
            logging.debug(
                f"tt {address} already tracked, has timeslot {self.connected_clients[address]}"
            )
            return

        self.connected_clients[address] = self.time_slot
        self.time_slot += 1

        logging.debug(
            f"Node {address} now has timeslot {self.connected_clients[address]}"
        )

    def _handle_global_state(self, message: mqtt.MQTTMessage) -> None:
        logging.debug("Received global state message")
        if "movement" in message.topic:
            data: Dict[str, float] = json.loads(message.payload)
            logging.debug(f"Received aggregated movement data: {data}")
            self.data_policy.aggregated_movement = data
        elif "temperature" in message.topic:
            data: Dict[str, float] = json.loads(message.payload)
            logging.debug(f"Received aggregated temperature data: {data}")
            self.data_policy.aggregated_temperature = data
        else:
            logging.error(f"Unknown topic: {message.topic}")

    def _handle_packet(self, message: mqtt.MQTTMessage) -> None:
        logging.debug("Received packet message")

        packet: TTPacket = unmarshall(b64decode(message.payload))
        logging.debug(f"Unamarshalled packet: {packet}")

        if packet.receiver_address.address == 1246382666:
            logging.debug(
                f"Received multicast message addressed to {packet.receiver_address.address}"
            )
        elif packet.receiver_address == self.address:
            logging.debug("Received unicast message addressed to us")
            if packet.sender_address not in self.connected_clients:
                logging.debug(
                    f"Node {packet.sender_address} sent us a unicast, but is not connected."
                )
                self._add_tt_to_connected(packet.sender_address)
        else:
            logging.debug(
                f"Received unicast message addressed to someone else ({packet.receiver_address})"
            )
            return

        if isinstance(packet, TTHeloPacket):
            self._on_helo(packet=packet)
            return
        elif isinstance(packet, DataPacketRev31) or isinstance(packet, DataPacketRev32):
            reply = self._on_data(packet=packet)
        elif isinstance(packet, LightSensorPacket):
            reply = self._on_light(packet=packet)
        else:
            logging.error("Unsupported packet type")
            return

        logging.debug(f"Reply: {reply}")

        if self.respond:
            self.mqtt_client.publish(
                topic=f"command/{self.address.address}",
                payload=b64encode(reply.marshall()),
            )

    def _on_helo(self, packet: TTHeloPacket) -> None:
        logging.debug(f"Received HELO-Request: {packet}")
        request: Dict[str, int] = {
            "cloud_address": self.address.address,
            "tt_address": packet.sender_address.address,
        }
        if self.respond:
            logging.debug(f"Sending connection request to backend: {request}")
            self.mqtt_client.publish(topic="helo/request", payload=json.dumps(request))

    def _on_data(self, packet: Union[DataPacketRev31, DataPacketRev32]) -> TTPacket:
        logging.debug("Received data packet")
        reply = self.data_policy.evaluate(packet)
        reply.time_slot = self.connected_clients.get(reply.receiver_address, 0)

        packet_data = packet.to_influx_json()
        logging.debug(f"Sending data to influx: {packet_data}")

        try:
            self.influx_client.write_points(packet_data)
        except influx.client.InfluxDBServerError as err:
            logging.error(f"Influxdb error: {err}")

        return reply

    def _on_light(self, packet: LightSensorPacket) -> TTPacket:
        reply = self.light_policy.evaluate(packet)

        packet_data = packet.to_influx_json()
        logging.debug(f"Sending data to influx: {packet_data}")

        try:
            self.influx_client.write_points(packet_data)
        except influx.client.InfluxDBServerError as err:
            logging.error(f"Influxdb error: {err}")

        return reply

    def start(self):
        logging.info("Starting Local Decision Engine")
        while True:
            time.sleep(0.5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Evaluate sensor data from connected nodes to find anomalies."
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument(
        "-b", "--broker", help="Address of the MQTT broker", default="localhost"
    )
    parser.add_argument(
        "-i", "--influx", help="Address of the influxdb", default="localhost"
    )
    parser.add_argument(
        "-n",
        "--no-response",
        help="Don't actually send the response packet",
        action="store_false",
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
        address=generate_tt_address(),
        respond=args.no_response,
    ) as lde:
        lde.start()
