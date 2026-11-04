import argparse

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare all available months of Hubway trip data.")
    parser.add_argument("--hdfs_source_dir", required=True)
    parser.add_argument("--hdfs_target_dir", required=True)
    return parser.parse_args()


def clean_tripdata(raw_trips: DataFrame) -> DataFrame:
    trips = raw_trips

    birth_year = F.trim(F.col("birth year"))

    trips = trips.withColumn(
        "birth year",
        F.when(
            F.lower(birth_year).isin(
                "", "\\n", "null", "none", "nan",
                "na", "n/a", "unknown", "unbekannt"
            ),
            F.lit(None)
        ).otherwise(birth_year).cast("int")
    )

    for column in ["starttime", "stoptime"]:
        trips = trips.withColumn(
            column,
            F.to_timestamp(F.col(column), "yyyy-MM-dd HH:mm:ss")
        )

    for column in [
        "tripduration", "start station id",
        "end station id", "bikeid", "gender"
    ]:
        trips = trips.withColumn(column, F.col(column).cast("long"))

    for column in [
        "start station latitude", "start station longitude",
        "end station latitude", "end station longitude"
    ]:
        trips = trips.withColumn(column, F.col(column).cast("double"))


    for column in trips.columns:
        trips = trips.withColumnRenamed(column, column.strip().replace(" ", "_"))

    return trips




def run(spark: SparkSession, args: argparse.Namespace) -> None:
    month_pattern = "[0-9]" * 6
    source = "{}/{}/{}-hubway-tripdata.csv".format(
        args.hdfs_source_dir.rstrip("/"), month_pattern, month_pattern
    )
    reader = spark.read.option("header", "true").option("inferSchema", "false")
    raw_trips = reader.csv(source).withColumn(
        "month",
        F.regexp_extract(F.input_file_name(), r"/([0-9]{6})-hubway-tripdata\.csv$", 1)
    )
    final_trips = clean_tripdata(raw_trips)
    final_trips.repartition("month").write \
        .format("parquet") \
        .mode("overwrite") \
        .partitionBy("month") \
        .save(args.hdfs_target_dir.rstrip("/"))


def main() -> None:
    args = get_args()
    spark = SparkSession.builder.appName("hubway_clean_tripdata").getOrCreate()
    try:
        run(spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
