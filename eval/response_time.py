import influxdb as influx
from influxdb.resultset import ResultSet

import pandas as pd


def get_data(influx_address: str, out_path: str):
    influx_client = influx.InfluxDBClient(host=influx_address, port=8086)
    influx_client.switch_database("ttt")

    data: ResultSet = influx_client.query(
        'SELECT "response_time" FROM "response_time" GROUP BY "responder", "packet_type"'
    )

    df = pd.DataFrame()

    for datapoint in data.get_points(tags={"packet_type": "data", "responder": "ttt"}):
        df = df.append(
            {"Type": "Data", "Responder": "ttt", "Time": datapoint["response_time"]},
            ignore_index=True,
        )

    for datapoint in data.get_points(
        tags={"packet_type": "data", "responder": "ttcloud"}
    ):
        df = df.append(
            {"Type": "Data", "Responder": "ttcloud", "Time": datapoint["response_time"]},
            ignore_index=True,
        )

    for datapoint in data.get_points(tags={"packet_type": "light", "responder": "ttt"}):
        df = df.append(
            {"Type": "Light", "Responder": "ttt", "Time": datapoint["response_time"]},
            ignore_index=True,
        )

    for datapoint in data.get_points(
        tags={"packet_type": "light", "responder": "ttcloud"}
    ):
        df = df.append(
            {
                "Type": "Light",
                "Responder": "ttcloud",
                "Time": datapoint["response_time"],
            },
            ignore_index=True,
        )

    influx_client.close()

    data_json = df.to_json()
    with open(out_path, "w") as f:
        f.write(data_json)


if __name__ == "__main__":
    get_data(influx_address="localhost", out_path="response_times.json")
