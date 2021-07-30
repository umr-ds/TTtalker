#! /usr/bin/env python3
import struct
from socket import socket
from packets import *
import time
from typing import List

import time
from SX127x.LoRa import *
#from SX127x.LoRaArgumentParser import LoRaArgumentParser
from SX127x.board_config import BOARD

BOARD.setup()
BOARD.reset()


class LoRaParser(LoRa):
    def __init__(self, verbose=False):
        super(LoRaParser, self).__init__(verbose)
        self.set_mode(MODE.SLEEP)
        self.set_dio_mapping([0] * 6)
        self.var = 0

        try:
            print("setting up lora hat...")
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

            print("listening for TT lora packets...")
            self.start()
        except KeyboardInterrupt:
            print("Exit")
        finally:
            print("Exit")
            self.set_mode(MODE.SLEEP)
            BOARD.teardown()  # !!!

    def on_rx_done(self):
        self.clear_irq_flags(RxDone=1)
        payload = self.read_payload(nocheck=True)[4:]
        print(f"RAW Receive: {bytes(payload).hex()}")
        packet: TTPacket = unmarshall(bytes(payload))
        print(f"Parsed Receive: {packet}")
        time.sleep(1)  # Wait for the client be ready
        self.handle_receive(packet)

    def on_tx_done(self):
        print("\nTxDone")
        print(self.get_irq_flags())

    def on_cad_done(self):
        print("\non_CadDone")
        print(self.get_irq_flags())

    def on_rx_timeout(self):
        print("\non_RxTimeout")
        print(self.get_irq_flags())

    def on_valid_header(self):
        print("\non_ValidHeader")
        print(self.get_irq_flags())

    def on_payload_crc_error(self):
        print("\non_PayloadCrcError")
        print(self.get_irq_flags())

    def on_fhss_change_channel(self):
        print("\non_FhssChangeChannel")
        print(self.get_irq_flags())

    def start(self):
        while True:
            self.reset_ptr_rx()
            self.set_mode(MODE.RXCONT)
            x = 1
            while(self.var == 0):
                x += 1
                #print(f"{x}: sleeping because nothing happend")
                time.sleep(1)

            self.var = 0

    def send_packet(self, packet: TTPacket) -> None:
        self.write_payload([255, 255, 0, 0] + list(packet.marshall()))
        print(f"Sending Reply: {packet}")
        self.set_mode(MODE.TX)

    def handle_receive(self, packet: TTPacket) -> None:
        if isinstance(packet, TTHeloPacket):
            reply = TTCloudHeloPacket(receiver_address=packet.sender_address, sender_address=packet.receiver_address, command=190, time=int(time.time()))
            self.send_packet(reply)
        elif isinstance(packet, TTDummy):
            self.send_packet(TTCommand1(receiver_address=packet.sender_address, sender_address=packet.receiver_address, command=32, time=int(time.time()), sleep_intervall=60, unknown=(0, 45, 1), heating=30))
        elif isinstance(packet, LightSensorPacket):
            self.send_packet(TTCommand2(receiver_address=packet.sender_address, sender_address=packet.receiver_address, command=33, time=int(time.time()), integration_time=50, gain=3))


if __name__ == "__main__":
#    test_packet: bytes = bytes.fromhex(
#        "180103c2630799210500"
#        "180103c263079921450580510100410038ffc7260100389f0000112b2f00eff006003ffa000000000000410038ff9039"
#        "180103c2520103524d020d010000328800008c88000071b5000013aa0000111dd4004a00eafc940f0000000000007787000074570000fcc5bd430100"
#    )
#    print(test_packet.hex())
#    print(len("180103c263079921450580510100410038ffc7260100389f0000112b2f00eff006003ffa000000000000410038ff9039"))
#    parsed = unmarshall(test_packet)
#    print(parsed)
#    marshalled = parsed.marshall()
#    assert marshalled == test_packet

    lora_parser = LoRaParser()

