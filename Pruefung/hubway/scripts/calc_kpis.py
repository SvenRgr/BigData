import argparse
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F


from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)



def get_args():
    parser = argparse.ArgumentParser(description="Calculate Hubway KPIs")
    parser.add_argument("--hdfs_trip_dir", required=True, help="HDFS directory for trip data")
    parser.add_argument("--hdfs_station_dir", required=True, help="HDFS directory for station data")
    parser.add_argument("--hdfs_target_dir", required=True, help="HDFS directory for KPI results")
    return parser.parse_args()



def calc_kpis(df: DataFrame) -> list:

    # calculate average trip duration
    avg_trip_duration = round(df.agg(F.avg("tripduration")).collect()[0][0] / 60, 2)

    
    # avg trip distance air
    start_latitude = F.radians(F.col("start_station_latitude"))
    start_longitude = F.radians(F.col("start_station_longitude"))
    end_latitude = F.radians(F.col("end_station_latitude"))
    end_longitude = F.radians(F.col("end_station_longitude"))

    latitude_difference = end_latitude - start_latitude
    longitude_difference = end_longitude - start_longitude
    haversine_term = (
        F.sin(latitude_difference / 2) ** 2
        + F.cos(start_latitude)
        * F.cos(end_latitude)
        * F.sin(longitude_difference / 2) ** 2
    )
    haversine_term = F.least(F.lit(1.0), F.greatest(F.lit(0.0), haversine_term))
    trip_distance = 2 * 6371 * F.asin(F.sqrt(haversine_term))
    avg_trip_distance = round(df.agg(F.avg(trip_distance)).collect()[0][0], 2)

    # usage share by gender
    male = [row for row in df.rdd.collect() if row["gender"] == 1]
    female = [row for row in df.rdd.collect() if row["gender"] == 2]
    undefined = [row for row in df.rdd.collect() if row["gender"] == 0]


    total_trips = len(male) + len(female) + len(undefined)
    gender_share = {
        "male": round(len(male) / total_trips * 100, 2),
        "female": round(len(female) / total_trips * 100, 2),
        "undefined": round(len(undefined) / total_trips * 100, 2)
    }

    # usage share by age
    # include calculation from start year
    age_groups = {
        "under_20": [row for row in df.rdd.collect() if row["birth_year"] and row["starttime"].year - row["birth_year"] < 20],
        "20_to_40": [row for row in df.rdd.collect() if row["birth_year"] and 20 <= row["starttime"].year - row["birth_year"] <= 40],
        "40_to_60": [row for row in df.rdd.collect() if row["birth_year"] and 40 <= row["starttime"].year - row["birth_year"] <= 60],
        "above_60": [row for row in df.rdd.collect() if row["birth_year"] and row["starttime"].year - row["birth_year"] > 60],
        "undefined": [row for row in df.rdd.collect() if not row["birth_year"]],
    }

    total_trips_age = sum(len(group) for key, group in age_groups.items() if key != "undefined") 
    age_share = {key: round(len(group) / total_trips_age * 100, 2) for key, group in age_groups.items() if key != "undefined"}
    

    # top 10 most used bikes
    bike_usage = df.groupBy("bikeid").count().orderBy(F.desc("count")).limit(10)
    top_10_bikes = [(row["bikeid"], row["count"]) for row in bike_usage.collect()]

    # top 10 most used start stations
    start_station_usage = df.groupBy("start_station_id").count().orderBy(F.desc("count")).limit(10)
    top_10_start_stations = [(row["start_station_id"], row["count"]) for row in start_station_usage.collect()]

    # top 10 used end stations
    end_station_usage = df.groupBy("end_station_id").count().orderBy(F.desc("count")).limit(10)
    top_10_end_stations = [(row["end_station_id"], row["count"]) for row in end_station_usage.collect()]

    # usage share by timeslot 0-6 6-12 12-18 18-24 
    timeslots = {
        "0-6": [row for row in df.rdd.collect() if 0 <= row["starttime"].hour < 6],
        "6-12": [row for row in df.rdd.collect() if 6 <= row["starttime"].hour < 12],
        "12-18": [row for row in df.rdd.collect() if 12 <= row["starttime"].hour < 18],
        "18-24": [row for row in df.rdd.collect() if 18 <= row["starttime"].hour < 24],
    }

    total_trips_timeslot = sum(len(group) for key, group in timeslots.items())
    timeslot_share = {key: round(len(group) / total_trips_timeslot * 100, 2) for key, group in timeslots.items()}


    return [
        ["avg_trip_duration", avg_trip_duration, "minutes"],
        ["avg_trip_distance", avg_trip_distance, "kilometers"],
        ["usage_share_by_gender", gender_share, "percent"],
        ["usage_share_by_age", age_share, "percent"],
        ["top_10_most_used_bikes", top_10_bikes, "trips"],
        ["top_10_most_used_start_stations", top_10_start_stations, "trips"],
        ["top_10_most_used_end_stations", top_10_end_stations, "trips"],
        ["usage_share_by_timeslot", timeslot_share, "percent"]
    ]



def calc_month_kpis(spark: SparkSession, hdfs_trip_dir, hdfs_station_dir) -> list:
    trips = spark.read.parquet(hdfs_trip_dir)

    months = trips.select("month").distinct().orderBy("month").collect()

    kpis = []
    for name, value, unit in calc_kpis(trips):
        kpis.append(("global", name, value, unit))

    for month_row in months:
        month = month_row["month"]
        month_trips = trips.filter(F.col("month") == F.lit(month))

        for name, value, unit in calc_kpis(month_trips):
            kpis.append((str(month), name, value, unit))
    
    return kpis




def run(spark, args):
    kpis = calc_month_kpis(spark, args.hdfs_trip_dir, args.hdfs_station_dir)
    export_excel(spark, kpis, args.hdfs_target_dir)



def export_excel(spark, kpis, target_dir):
    rows = []

    for month, name, value, unit in kpis:
        if isinstance(value, dict):
            for dimension, number in value.items():
                rows.append((
                    month, name, dimension, None, None,
                    float(number), unit,
                ))
        elif isinstance(value, list):
            for rank, (item_id, count) in enumerate(value, start=1):
                rows.append((
                    month, name, None, rank, str(item_id),
                    float(count), unit,
                ))
        else:
            rows.append((
                month, name, None, None, None,
                float(value), unit,
            ))

    schema = StructType([
        StructField("month", StringType(), nullable=False),
        StructField("kpi", StringType(), nullable=False),
        StructField("dimension", StringType(), nullable=True),
        StructField("rank", IntegerType(), nullable=True),
        StructField("item", StringType(), nullable=True),
        StructField("value", DoubleType(), nullable=False),
        StructField("unit", StringType(), nullable=False),
    ])

    df = spark.createDataFrame(rows, schema)

    (
        df.write
        .format("com.crealytics.spark.excel")
        .option("useHeader", "true")
        .mode("overwrite")
        .save(f"{target_dir}/kpis.xlsx")
    )





def main() -> None:
    args = get_args()
    spark = SparkSession.builder.appName("hubway_calculate_kpis").getOrCreate()
    try:
        run(spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()