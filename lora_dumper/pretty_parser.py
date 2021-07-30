#! /usr/bin/env python3
import struct
from ttcloud.packets import unmarshall

x=0
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
            except struct.error:
                print(f"{x}: unmarshalling error")
            except KeyError as err:
                print(f"{x}: unknown packet type {err}")
