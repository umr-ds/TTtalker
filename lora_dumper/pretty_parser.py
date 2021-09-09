#! /usr/bin/env python3
import struct
from ttt.packets import unmarshall

x = 0
with open("packets2.csv", "r") as f:
    for line in f:
        x += 1
        if "#" in line:
            print(line)
        else:
            try:
                payload = line.split(",")[1]
                packet = unmarshall(bytes.fromhex(payload))
                print(packet)
            except struct.error as err:
                print(f"{x}: unmarshalling error: {err}")
            except KeyError as err:
                print(f"{x}: unknown packet type {err}")
