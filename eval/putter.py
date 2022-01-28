#! /usr/bin/env python3

import argparse
import requests
import pickle
import influxdb as influx
from datetime import datetime
from sys import exit
from typing import List, Tuple, Dict, Any, Set
from ttt import packets
from tqdm import tqdm


TT_CLOUDS = ["C2030115", "C2030116", "C2030117", "C2030119", "C2030123"]
UPLOAD_DATABASE = "historical"
UPLOAD_BATCH_SIZE = 100000


def parse_date(date: str) -> int:
    time = datetime.strptime(date, "%d.%m.%y %H:%M:%S")
    return int(time.timestamp())


def download_single(
    ttcloud: str, address: str
) -> Tuple[str, List[Tuple[int, packets.TTPacket]]]:
    print(f"Downloading packets for {ttcloud}")
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
    print("Sorting for time")
    tt_packets.sort(key=lambda x: x[0])
    return ttcloud, tt_packets


def download(
    ttclouds: List[str], address: str
) -> List[Tuple[str, List[Tuple[int, packets.TTPacket]]]]:
    tt_packets: List[Tuple[str, List[Tuple[int, packets.TTPacket]]]] = []

    for ttcloud in tqdm(ttclouds):
        tt_packets.append(download_single(ttcloud=ttcloud, address=address))

    return tt_packets


def upload_single(
    influx_client: influx.InfluxDBClient,
    tt_packets: List[Tuple[int, packets.TTPacket]],
    ttcloud: str,
) -> None:
    print(f"Uploading Packets from {ttcloud}")

    ttcloud_database = f"{UPLOAD_DATABASE}-{ttcloud}"
    influx_client.create_database(ttcloud_database)

    points: List[Dict[str, Any]] = []
    for packet in tt_packets:
        influx_json = packet[1].to_influx_json()
        for point in influx_json:
            point["time"] = packet[0]
        points += influx_json

    print(f"Number of packets in {ttcloud}: {len(points)}")

    if len(points) <= 100000:
        print("#Packets < 100000 -> uploading in single batch")
        influx_client.write_points(points, time_precision="s")
        return

    print("Starting upload")
    with tqdm(total=len(points)) as pbar:
        index = 0
        while index + UPLOAD_BATCH_SIZE < len(points):
            influx_client.switch_database(UPLOAD_DATABASE)
            influx_client.write_points(
                points[index : index + 100000], time_precision="s"
            )
            influx_client.switch_database(ttcloud_database)
            influx_client.write_points(
                points[index : index + 100000], time_precision="s"
            )
            index += UPLOAD_BATCH_SIZE
            pbar.update(UPLOAD_BATCH_SIZE)

    influx_client.write_points(points[index:], time_precision="s")

    print("Done")


def upload(
    influx_client: influx.InfluxDBClient,
    tt_packets: List[Tuple[str, List[Tuple[int, packets.TTPacket]]]],
) -> None:
    influx_client.create_database(UPLOAD_DATABASE)
    for ttcloud, tt_packets in tqdm(tt_packets):
        upload_single(
            influx_client=influx_client, ttcloud=ttcloud, tt_packets=tt_packets
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download historical data and do stuff with it"
    )
    parser.add_argument(
        "-a", "--address", help="Address of the server hosting the data"
    )
    parser.add_argument("action", help="One of [put, dump, upload, count]")
    parser.add_argument(
        "-p",
        "--pickle",
        help="If action is 'dump' or 'upload', filename for the pickled data",
    )
    args = parser.parse_args()

    if args.action == "put" or args.action == "dump":
        print("Downloading packets")
        tt_packets = download(ttclouds=TT_CLOUDS, address=args.address)
    elif args.action == "upload" or args.action == "count":
        print("Unpickling packets")
        with open(args.pickle, "rb") as f:
            tt_packets: List[
                Tuple[str, List[Tuple[int, packets.TTPacket]]]
            ] = pickle.load(f)
    else:
        print(f"Unknown action '{args.action}'")
        exit(1)

    if args.action == "put" or args.action == "upload":
        print("Uploading packets")
        influx_client = influx.InfluxDBClient(host="localhost", port=8086)
        upload(influx_client, tt_packets)
        influx_client.close()
    elif args.action == "dump":
        print("Pickling packets")
        with open(args.pickle, "wb") as f:
            pickle.dump(tt_packets, f, pickle.HIGHEST_PROTOCOL)
    elif args.action == "count":
        total = 0
        for _, pkts in tt_packets:
            total += len(pkts)
        print(f"Total number of packets: {total}")
