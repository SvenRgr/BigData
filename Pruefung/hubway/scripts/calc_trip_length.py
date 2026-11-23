import argparse
import os
from concurrent.futures import ThreadPoolExecutor
from itertools import islice

import requests
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def get_args():
    parser = argparse.ArgumentParser(description="Add OSRM route distances to Hubway trips.")
    parser.add_argument("--hdfs_trip_dir", required=True)
    parser.add_argument("--hdfs_target_dir", required=True)
    parser.add_argument("--osrm_url", default=os.environ.get("OSRM_URL"))

    return parser.parse_args()


def calc_trip_length(spark, trips, osrm_url):

    pair_columns = [
        "start_station_id", "end_station_id",
        "start_station_longitude", "start_station_latitude",
        "end_station_longitude", "end_station_latitude",
    ]
    is_round_trip = F.coalesce(
        F.col("start_station_id") == F.col("end_station_id"), F.lit(False)
    )
    trip_pairs = trips.filter(~is_round_trip).select(*pair_columns).distinct()
    trip_length = []

    def fetch_distance(row):
        start_lat = row["start_station_latitude"]
        start_long = row["start_station_longitude"]
        end_lat = row["end_station_latitude"]
        end_long = row["end_station_longitude"]

        url = (
            f"{osrm_url.rstrip('/')}/route/v1/bicycle/"
            f"{start_long},{start_lat};{end_long},{end_lat}"
        )
        try:
            response = requests.get(
                url,
                params={"overview": "false", "radiuses": "1000;1000"},
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()
            distance = (
                float(data["routes"][0]["distance"])
                if data["code"] == "Ok"
                else None
            )
        except (
            requests.exceptions.RequestException,
            ValueError,
            KeyError,
            IndexError,
            TypeError,
        ):
            distance = None
        return (*row, distance)

    # parallele Ausführung
    rows = trip_pairs.toLocalIterator()
    with ThreadPoolExecutor(max_workers=4) as executor:
        batch = list(islice(rows, 32))
        while batch:
            trip_length.extend(executor.map(fetch_distance, batch))
            batch = list(islice(rows, 32))

    distances = spark.createDataFrame(
        trip_length, trip_pairs.schema.add("distance", "double")
    )
    return trips.join(distances, pair_columns, "left")


def run(spark, args):
    if args.hdfs_trip_dir.rstrip("/") == args.hdfs_target_dir.rstrip("/"):
        raise ValueError("Trip input and output directories must be different.")

    trips = spark.read.parquet(args.hdfs_trip_dir)
    trips = calc_trip_length(
        spark, trips, args.osrm_url
    )
    trips.repartition("month").write.mode("overwrite") \
        .partitionBy("month").parquet(args.hdfs_target_dir)


def main() -> None:
    args = get_args()
    spark = SparkSession.builder.appName("hubway_calculate_trip_length").getOrCreate()
    try:
        run(spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
