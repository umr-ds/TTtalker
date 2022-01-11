#! /usr/bin/env python3

import argparse
import requests
import pickle
import influxdb as influx
from datetime import datetime
from typing import List, Tuple, Dict, Any, Set
from ttt import packets


TT_CLOUDS = ["C2030115", "C2030116", "C2030117", "C2030119", "C2030123"]


def parse_date(date: str) -> int:
    time = datetime.strptime(date, "%d.%m.%y %H:%M:%S")
    return int(time.timestamp())


def download(ttcloud: str, address: str) -> List[Tuple[int, packets.TTPacket]]:
    tt_packets: List[Tuple[int, packets.TTPacket]] = []
    unknown_types: Set[int] = set()
    cloud_address = packets.TTAddress(address=int(ttcloud, 16))

    data: requests.Response = requests.get(f"http://{address}/{ttcloud}/ttcloud.txt")

    for line in data.text.split("\r\n"):
        if line == "":
            continue

        parts: List[str] = line.strip().split(";")
        timestamp = parse_date(parts[0].split(",")[0])
        treetalker_address = packets.TTAddress(address=int(parts[0].split(",")[1], 16))

        packet_type = int(parts[2], 16)

        if packet_type == 77:
            args = [int(x) for x in parts[3:]]
            args = [cloud_address, treetalker_address, int(parts[1], 16)] + args
            packet = packets.DataPacketRev32(*args)
            tt_packets.append((timestamp, packet))
        elif packet_type == 73:
            wavelengths = [int(x) for x in parts[4:16]]
            AS7263 = {
                610: wavelengths[0],
                680: wavelengths[1],
                730: wavelengths[2],
                760: wavelengths[3],
                810: wavelengths[4],
                860: wavelengths[5],
            }
            AS7262 = {
                450: wavelengths[6],
                500: wavelengths[7],
                550: wavelengths[8],
                570: wavelengths[9],
                600: wavelengths[10],
                650: wavelengths[11],
            }
            packet = packets.LightSensorPacket(
                receiver_address=cloud_address,
                sender_address=treetalker_address,
                timestamp=int(parts[3]),
                packet_number=int(parts[1], 16),
                AS7262=AS7262,
                AS7263=AS7263,
                integration_time=int(parts[16]),
                gain=int(parts[17]),
            )
            tt_packets.append((timestamp, packet))
        else:
            unknown_types.add(packet_type)

    print(f"Unknown types in {ttcloud}: {unknown_types}")
    print(f"Number of packets in {ttcloud}: {len(tt_packets)}")
    return tt_packets


def upload(
    influx_client: influx.InfluxDBClient,
    tt_packets: List[Tuple[int, packets.TTPacket]],
    ttcloud: str,
) -> None:
    points: List[Dict[str, Any]] = []
    for packet in tt_packets:
        influx_json = packet[1].to_influx_json()
        for point in influx_json:
            point["timestamp"] = packet[0]
        points += influx_json

    print(f"Number of packets in {ttcloud}: {len(points)}")

    if len(points) <= 100000:
        influx_client.write_points(points, time_precision="s")
        return

    index = 0
    while index + 100000 < len(points):
        influx_client.write_points(points[index : index + 100000], time_precision="s")
        index += 100000

    influx_client.write_points(points[index:], time_precision="s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download historical data and do stuff with it"
    )
    parser.add_argument(
        "-a", "--address", help="Address of the server hosting the data"
    )
    parser.add_argument("action", help="One of [put, dump, upload]")
    parser.add_argument(
        "-p",
        "--pickle",
        help="If action is 'dump' or 'upload', filename for the pickled data",
    )
    args = parser.parse_args()

    if args.action == "put" or args.action == "upload":
        influx_client = influx.InfluxDBClient(host="localhost", port=8086)
        influx_client.create_database("historical")
        influx_client.switch_database("historical")

        if args.action == "put":
            for ttcloud in TT_CLOUDS:
                tt_packets = download(ttcloud=ttcloud, address=args.address)
                upload(influx_client, tt_packets, ttcloud)
        else:
            with open(args.pickle, "rb") as f:
                tt_packets: List[Tuple[int, packets.TTPacket]] = pickle.load(f)
            upload(influx_client, tt_packets, "dump")

        influx_client.close()
    elif args.action == "dump":
        all_packets: List[Tuple[int, packets.TTPacket]] = []
        for ttcloud in TT_CLOUDS:
            all_packets += download(ttcloud=ttcloud, address=args.address)

        print(f"Total number of packets: {len(all_packets)}")

        with open(args.pickle, "wb") as f:
            pickle.dump(all_packets, f, pickle.HIGHEST_PROTOCOL)

    else:
        print(f"Unknown action '{args.action}'")
