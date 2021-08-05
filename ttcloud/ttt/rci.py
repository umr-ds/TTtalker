#! /usr/bin/env python3

from __future__ import annotations
import argparse
import json
import logging
import time
import paho.mqtt.client as mqtt

from typing import Any

from ttt.SX127x.LoRa import LoRa
from ttt.SX127x.board_config import BOARD
from ttt.SX127x.constants import MODE, BW, CODING_RATE

from ttt.packets import *


class LoRaParser(LoRa):
    def __init__(self, verbose: bool, broker_address: str):
        LoRa.__init__(self=self, verbose=verbose)
        self.mqtt_client = mqtt.Client("ttt")
        self.mqtt_client.connect(broker_address)
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.subscribe("command")

    def __enter__(self) -> LoRaParser:
        BOARD.setup()
        BOARD.reset()

        self.set_mode(MODE.SLEEP)
        self.set_dio_mapping([0] * 6)

        self.set_freq(868.5)

        # Slow+long range  Bw = 125 kHz, Cr = 4/8, Sf = 4096chips/symbol, CRC on. 13 dBm
        self.set_pa_config(pa_select=1, max_power=21, output_power=15)
        self.set_bw(BW.BW125)
        self.set_coding_rate(CODING_RATE.CR4_8)
        self.set_spreading_factor(12)
        self.set_rx_crc(True)
        # lora.set_lna_gain(GAIN.G1)
        # lora.set_implicit_header_mode(False)
        self.set_low_data_rate_optim(True)
        self.set_mode(MODE.STDBY)

        self.reset_ptr_rx()
        self.set_mode(MODE.RXCONT)

        self.mqtt_client.loop_start()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.set_mode(MODE.SLEEP)
        BOARD.teardown()  # !!!

        self.mqtt_client.loop_stop()

    def on_rx_done(self) -> None:
        """Callback called when a packet is received."""
        self.clear_irq_flags(RxDone=1)
        payload = self.read_payload(nocheck=True)[4:]
        logging.debug(f"RAW Receive: {bytes(payload).hex()}")
        packet: TTPacket = unmarshall(bytes(payload))
        logging.debug(f"Parsed Receive: {packet}")
        self.mqtt_client.publish(
            topic=f"receive/{type(packet)}", payload=json.dumps(packet)
        )

    def on_tx_done(self) -> None:
        """Callback called when a packet has been sent."""
        print("Sending Done - back to receiver mode")
        self.set_dio_mapping([0, 0, 0, 0, 0, 0])  # Deaktiviere alle DIOs
        self.reset_ptr_rx()
        self.set_mode(MODE.RXCONT)  # Receiver mode
        # print(self.get_irq_flags())

    def start(self):
        while True:
            time.sleep(0.5)

    def send_packet(self, packet: TTPacket) -> None:
        self.write_payload([255, 255, 0, 0] + list(packet.marshall()))
        print(f"Sending Reply: {packet}")
        self.set_dio_mapping([1, 0, 0, 0, 0, 0])  # Aktiviere DIO0 für TXDone trigger
        self.set_mode(MODE.TX)

    def on_message(
        self, client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage
    ) -> None:
        packet = json.loads(message.payload)
        self.send_packet(packet)


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

    with LoRaParser(verbose=args.verbose, broker_address=args.broker) as lora_parser:
        lora_parser.start()
