#! /usr/bin/env python3

from socket import socket
from packets import unmarshall
import time

import time
from SX127x.LoRa import *
#from SX127x.LoRaArgumentParser import LoRaArgumentParser
from SX127x.board_config import BOARD

BOARD.setup()
BOARD.reset()

def listen_and_process(s: socket) -> None:
    packet: bytes
    while 1:
        packet, _ = s.recvfrom(4096)
        unmarshall(packet)

class LoRaParser(LoRa):
    def __init__(self, verbose=False):
        super(LoRaParser, self).__init__(verbose)
        self.set_mode(MODE.SLEEP)
        self.set_dio_mapping([0] * 6)
        self.var=0

        try:
            print("setting up...")
            self.set_freq(868.5)
            self.set_coding_rate(CODING_RATE.CR4_6)

            #Slow+long range  Bw = 125 kHz, Cr = 4/8, Sf = 4096chips/symbol, CRC on. 13 dBm
            self.set_pa_config(pa_select=1, max_power=21, output_power=15)
            self.set_bw(BW.BW125)
            self.set_coding_rate(CODING_RATE.CR4_8)
            self.set_spreading_factor(12)
            self.set_rx_crc(True)
            #lora.set_lna_gain(GAIN.G1)
            #lora.set_implicit_header_mode(False)
            self.set_low_data_rate_optim(True) # evtl abschalten bei fehlern
            self.set_mode(MODE.STDBY)

            print("starting....")
            self.start()
        except KeyboardInterrupt:
            print("Exit")
        finally:
            print("Exit")
            self.set_mode(MODE.SLEEP)
            BOARD.teardown() # !!!

    def on_rx_done(self):
        self.clear_irq_flags(RxDone=1)
        payload = self.read_payload(nocheck=True)
        print("Receive: ")
        unmarshall(bytes(payload))
        time.sleep(2) # Wait for the client be ready
        print ("Send: ACK")
        self.write_payload([255, 255, 0, 0, 65, 67, 75, 0]) # Send ACK
        self.set_mode(MODE.TX)
        self.var = 1

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
            while(self.var == 0):
                print("sleeping because nothing happend")
                time.sleep(1)

            self.var=0

if __name__ == "__main__":
    test_packet: bytes = bytes.fromhex(
        "180103c2520103524d020d010000328800008c88000071b5000013aa0000111dd4004a00eafc940f0000000000007787000074570000fcc5bd430100"
    )
    parsed = unmarshall(test_packet)
    print(parsed)
    marshalled = parsed.marshall()

    assert marshalled == test_packet

    lora_parser = LoRaParser()

