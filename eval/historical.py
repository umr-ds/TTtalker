#! /usr/bin/env python3

import pickle
import logging
import argparse
import policy
from ttt import packets
from typing import List, Tuple, Dict
from statistics import mean, stdev
import influxdb as influx
from influxdb.resultset import ResultSet
from tqdm import tqdm
from json import dump

ANALYSIS_TIME = 172800  # 2 days
ANALYSIS_WINDOW = 3600  # 1 hour
UPLOAD_DATABASE = "historical"


def aggregate_movement(
    influx_client: influx.InfluxDBClient, packet_time: int
) -> Dict[str, float]:
    logging.debug("Aggregating movement data")
    influx_client.switch_database(UPLOAD_DATABASE)

    x_derivs: List[int] = []
    y_derivs: List[int] = []
    z_derivs: List[int] = []

    t_start = packet_time - ANALYSIS_TIME

    try:
        data: ResultSet = influx_client.query(
            f'SELECT "x_derivation", "y_derivation", "z_derivation" FROM "gravity" WHERE time > {t_start}s AND time < {packet_time}s'
        )
    except influx.client.InfluxDBServerError as err:
        logging.error(f"Influxdb error: {err}")
        return {}

    for datapoint in data.get_points("gravity"):
        x_derivs.append(datapoint["x_derivation"])
        y_derivs.append(datapoint["y_derivation"])
        z_derivs.append(datapoint["z_derivation"])

    if len(x_derivs) < 2 or len(y_derivs) < 2 or len(z_derivs) < 2:
        logging.error(
            f"Insufficient movement data: [x: {len(x_derivs)}, y: {len(y_derivs)}, z: {len(z_derivs)}]"
        )
        return {}

    mean_x = mean(x_derivs)
    stdev_x = stdev(x_derivs, mean_x)
    mean_y = mean(y_derivs)
    stdev_y = stdev(y_derivs, mean_y)
    mean_z = mean(z_derivs)
    stdev_z = stdev(z_derivs, mean_z)

    aggregated = {
        "mean_x": mean_x,
        "stdev_x": stdev_x,
        "mean_y": mean_y,
        "stdev_y": stdev_y,
        "mean_z": mean_z,
        "stdev_z": stdev_z,
    }

    logging.debug(f"Aggreagated movement data: {aggregated}")

    return aggregated


def aggregate_temperature(
    influx_client: influx.InfluxDBClient, packet_time: int
) -> Dict[str, float]:
    logging.debug("Aggregating temperature data")
    influx_client.switch_database(UPLOAD_DATABASE)

    t_start = packet_time - ANALYSIS_TIME

    try:
        data: ResultSet = influx_client.query(
            f'SELECT "ttt_reference_probe_cold","ttt_reference_probe_hot","ttt_heat_probe_cold","ttt_heat_probe_hot" FROM "stem_temperature" WHERE time > {t_start}s AND time < {packet_time}s'
        )
    except influx.client.InfluxDBServerError as err:
        logging.error(f"Influxdb error: {err}")
        return {}

    reference_probe_cold: List[float] = []
    reference_probe_hot: List[float] = []
    heat_probe_cold: List[float] = []
    heat_probe_hot: List[float] = []

    for datapoint in data.get_points("stem_temperature"):
        reference_probe_cold.append(datapoint["ttt_reference_probe_cold"])
        reference_probe_hot.append(datapoint["ttt_reference_probe_hot"])
        heat_probe_cold.append(datapoint["ttt_heat_probe_cold"])
        heat_probe_hot.append(datapoint["ttt_heat_probe_hot"])

    if (
        len(reference_probe_cold) < 2
        or len(reference_probe_hot) < 2
        or len(heat_probe_cold) < 2
        or len(heat_probe_hot) < 2
    ):
        logging.debug(
            f"Insufficient temperature data: [ref_cold: {len(reference_probe_cold)}, ref_hot: {len(reference_probe_hot)}, heat_cold: {len(heat_probe_cold)}, heat_hot: {len(heat_probe_hot)}]"
        )
        return {}

    deltas_cold: List[float] = [
        abs(heat - reference)
        for heat, reference in zip(heat_probe_cold, reference_probe_cold)
    ]
    stdev_delta_cold = stdev(deltas_cold)

    deltas_hot: List[float] = [
        abs(heat - reference)
        for heat, reference in zip(heat_probe_hot, reference_probe_hot)
    ]
    stdev_delta_hot = stdev(deltas_hot)

    aggregated_data = {
        "stdev_delta_cold": stdev_delta_cold,
        "stdev_delta_hot": stdev_delta_hot,
    }

    logging.debug(f"Aggreagated temperature data: {aggregated_data}")

    return aggregated_data


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Analyse historical data for anomalies"
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("pickle", help="File containing pickled packets")
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

    logging.info("Unpickling packets")
    with open(args.pickle, "rb") as f:
        all_packets: List[Tuple[str, List[Tuple[int, packets.TTPacket]]]] = pickle.load(
            f
        )
    logging.info(f"Unpickled {len(all_packets)} packets")

    influx_client = influx.InfluxDBClient(host="localhost", port=8086)
    influx_client.switch_database("historical")

    for ttcloud, tt_packets in all_packets:
        logging.debug("Getting initial aggregated data")
        aggregation_time = tt_packets[0][0]
        aggregated_movement = aggregate_movement(
            influx_client=influx_client, packet_time=aggregation_time
        )
        aggregated_temperature = aggregate_temperature(
            influx_client=influx_client, packet_time=aggregation_time
        )

        data_policy = policy.DataPolicy(
            influx_client=influx_client,
            ttcloud=ttcloud,
            aggregated_movement=aggregated_movement,
            aggregated_temperature=aggregated_temperature,
        )

        light_policy = policy.LightPolicy(influx_client=influx_client)

        logging.info("Starting search for anomalies")
        with open("anomalies.jsonl", mode="a") as f:
            for timestamp, packet in tqdm(tt_packets):
                if timestamp > aggregation_time + ANALYSIS_WINDOW:
                    aggregation_time = timestamp
                    aggregated_movement = aggregate_movement(
                        influx_client=influx_client, packet_time=aggregation_time
                    )
                    aggregated_temperature = aggregate_temperature(
                        influx_client=influx_client, packet_time=aggregation_time
                    )
                    data_policy.aggregated_movement = aggregated_movement
                    data_policy.aggregated_temperature = aggregated_temperature

                if isinstance(packet, packets.DataPacketRev31) or isinstance(
                    packet, packets.DataPacketRev32
                ):
                    anomalies = data_policy.evaluate(
                        packet=packet, packet_time=timestamp
                    )
                else:
                    anomalies = []
                if anomalies:
                    logging.debug("Anomaly found!")
                    dump(
                        {
                            "packet": packet.to_influx_json(),
                            "timestamp": timestamp,
                            "anomalies": anomalies,
                        },
                        f,
                    )
                    f.write("\n")

    influx_client.close()
    logging.info("Done")
