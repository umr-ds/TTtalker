#! /usr/bin/env python3

from __future__ import annotations
import argparse
import logging
import time

from SX127x.LoRa import LoRa
from SX127x.board_config import BOARD
from SX127x.constants import MODE, BW, CODING_RATE

from ttcloud.packets import *


class LoRaParser(LoRa):
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

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.set_mode(MODE.SLEEP)
        BOARD.teardown()  # !!!

    def on_rx_done(self) -> None:
        """Callback called when a packet is received."""
        self.clear_irq_flags(RxDone=1)
        payload = self.read_payload(nocheck=True)[4:]
        print(f"RAW Receive: {bytes(payload).hex()}")
        packet: TTPacket = unmarshall(bytes(payload))
        print(f"Parsed Receive: {packet}")
        self.handle_receive(packet)

    def on_tx_done(self) -> None:
        """Callback called when a packet has been sent."""
        print("Sending Done - back to receiver mode")
        self.set_dio_mapping([0, 0, 0, 0, 0, 0])  # Deaktiviere alle DIOs
        self.reset_ptr_rx()
        self.set_mode(MODE.RXCONT)  # Receiver mode
        # print(self.get_irq_flags())

    def start(self):
        self.reset_ptr_rx()
        self.set_mode(MODE.RXCONT)

        while True:
            time.sleep(0.5)

    def send_packet(self, packet: TTPacket) -> None:
        self.write_payload([255, 255, 0, 0] + list(packet.marshall()))
        print(f"Sending Reply: {packet}")
        self.set_dio_mapping([1, 0, 0, 0, 0, 0])  # Aktiviere DIO0 für TXDone trigger
        self.set_mode(MODE.TX)

    def handle_receive(self, packet: TTPacket) -> None:
        if isinstance(packet, TTHeloPacket):
            reply = TTCloudHeloPacket(
                receiver_address=packet.sender_address,
                sender_address=packet.receiver_address,
                command=190,
                time=int(time.time()),
            )
            self.send_packet(reply)
        elif isinstance(packet, DataPacket2):
            self.send_packet(
                TTCommand1(
                    receiver_address=packet.sender_address,
                    sender_address=packet.receiver_address,
                    command=32,
                    time=int(time.time()),
                    sleep_intervall=60,
                    unknown=(0, 45, 1),
                    heating=30,
                )
            )
        elif isinstance(packet, LightSensorPacket):
            self.send_packet(
                TTCommand2(
                    receiver_address=packet.sender_address,
                    sender_address=packet.receiver_address,
                    command=33,
                    time=int(time.time()),
                    integration_time=50,
                    gain=3,
                )
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process some integers.")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    logging.basicConfig(level=log_level)

    with LoRaParser(verbose=args.verbose) as lora_parser:
        lora_parser.start()
