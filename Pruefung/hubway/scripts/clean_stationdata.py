import argparse

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare all available station data.")
    parser.add_argument("--hdfs_source_dir", required=True)
    parser.add_argument("--hdfs_target_dir", required=True)
    return parser.parse_args()


def clean_stationdata(raw_stations: DataFrame) -> DataFrame:
    stations = raw_stations
    for column in stations.columns:
        stations = stations.withColumnRenamed(column, column.strip().replace(" ", "_"))


        
    return stations





def run(spark: SparkSession, args: argparse.Namespace) -> None:
    source = args.hdfs_source_dir.rstrip("/")
    target = args.hdfs_target_dir.rstrip("/")
    reader = spark.read.option("header", "true").option("inferSchema", "false")
    raw_stations = reader.csv(source)
    final_stations = clean_stationdata(raw_stations)
    final_stations.write.format("parquet").mode("overwrite").save(target)




def main() -> None:
    args = get_args()
    spark = SparkSession.builder.appName("hubway_clean_stationdata").getOrCreate()
    try:
        run(spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
