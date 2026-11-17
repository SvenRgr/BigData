import argparse
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F


from pyspark.sql.types import (
    DoubleType,
    LongType,
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

# this function is completetly AI generated
def within_boston_radius(latitude, longitude):
    latitude = F.radians(latitude)
    longitude = F.radians(longitude)
    boston_latitude = F.radians(F.lit(42.3601))
    boston_longitude = F.radians(F.lit(-71.0589))
    haversine_term = (
        F.sin((latitude - boston_latitude) / 2) ** 2
        + F.cos(latitude)
        * F.cos(boston_latitude)
        * F.sin((longitude - boston_longitude) / 2) ** 2
    )
    haversine_term = F.least(F.lit(1.0), F.greatest(F.lit(0.0), haversine_term))
    distance_km = 2 * 6371 * F.asin(F.sqrt(haversine_term))
    return distance_km <= 500



def calc_kpis(df: DataFrame) -> list:

    # calculate average trip duration
    avg_trip_duration = round(df.agg(F.avg("tripduration")).collect()[0][0] / 60, 2)

    
    # avg trip distance air
    is_round_trip = F.coalesce(
        F.col("start_station_id") == F.col("end_station_id"), F.lit(False)
    )
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
    valid_coordinates = (
        F.col("start_station_latitude").between(-90, 90)
        & F.col("start_station_longitude").between(-180, 180)
        & F.col("end_station_latitude").between(-90, 90)
        & F.col("end_station_longitude").between(-180, 180)
    )
    within_boston_area = (
        within_boston_radius(
            F.col("start_station_latitude"), F.col("start_station_longitude")
        )
        & within_boston_radius(
            F.col("end_station_latitude"), F.col("end_station_longitude")
        )
    )
    trip_distance = F.when(
        valid_coordinates & within_boston_area & ~is_round_trip,
        2 * 6371 * F.asin(F.sqrt(haversine_term)),
    )
    avg_trip_distance_km = df.agg(F.avg(trip_distance)).first()[0]
    avg_trip_distance = (
        round(avg_trip_distance_km, 2)
        if avg_trip_distance_km is not None
        else None
    )

    valid_route_distance = F.when(
        valid_coordinates & within_boston_area & ~is_round_trip,
        F.col("distance"),
    )
    avg_trip_route_distance_meters = df.agg(
        F.avg(valid_route_distance)
    ).first()[0]
    avg_trip_route_distance = (
        round(avg_trip_route_distance_meters / 1000, 2)
        if avg_trip_route_distance_meters is not None
        else None
    )

    # usage share by gender
    counts = df.agg(
        F.count(F.when(F.col("gender") == 1, 1)).alias("male"),
        F.count(F.when(F.col("gender") == 2, 1)).alias("female"),
        F.count(F.when(F.col("gender") == 0, 1)).alias("undefined"),
    ).first()

    total_trips = counts["male"] + counts["female"] + counts["undefined"]

    if total_trips == 0:
        raise ValueError('Keine Fahrten mit Geschlechtscode 0, 1 oder 2 vorhanden')

    gender_share = {
        key: round(counts[key] / total_trips * 100, 2)
        for key in ("male", "female", "undefined")
    }

    
    age = F.year("starttime") - F.col("birth_year")
    valid_age = F.col("birth_year").isNotNull() & F.col("starttime").isNotNull()

    age_group = (
        F.when(valid_age & (age < 20), "under_20")
        .when(valid_age & (age < 40), "20_to_40")
        .when(valid_age & (age <= 60), "40_to_60")
        .when(valid_age & (age > 60), "above_60")
        .otherwise("unknown")
    )

    age_counts = (
        df.withColumn("age_group", age_group)
        .groupBy("age_group")
        .count()
        .collect()
    )

    total_trips_age = sum(row["count"] for row in age_counts)


    age_share = {
        row["age_group"]: round(row["count"] / total_trips_age * 100, 2)
        for row in age_counts
    }
    

    # top 10 most used bikes
    bike_usage = df.groupBy("bikeid").count().orderBy(F.desc("count")).limit(10)
    top_10_bikes = [(row["bikeid"], row["count"]) for row in bike_usage.collect()]

    # top 10 most used start stations
    start_station_usage = (
        df.groupBy("start_station_id")
        .agg(
            F.first("start_station_name", ignorenulls=True).alias("station_name"),
            F.count("*").alias("count"),
        )
        .orderBy(F.desc("count"))
        .limit(10)
    )
    top_10_start_stations = [(row["station_name"], row["count"]) for row in start_station_usage.collect()]

    # top 10 used end stations
    end_station_usage = (
        df.groupBy("end_station_id")
        .agg(
            F.first("end_station_name", ignorenulls=True).alias("station_name"),
            F.count("*").alias("count"),
        )
        .orderBy(F.desc("count"))
        .limit(10)
    )
    top_10_end_stations = [(row["station_name"], row["count"]) for row in end_station_usage.collect()]

    # usage share by timeslot 0-6 6-12 12-18 18-24 
    hour = F.hour("starttime")

    counts = df.agg(
        F.count(F.when((hour >= 0) & (hour < 6), 1)),
        F.count(F.when((hour >= 6) & (hour < 12), 1)),
        F.count(F.when((hour >= 12) & (hour < 18), 1)),
        F.count(F.when((hour >= 18) & (hour < 24), 1)),
    ).first()

    slots = ("0-6", "6-12", "12-18", "18-24")
    total = sum(counts)


    timeslot_share = {
        slot: round(count / total * 100, 2)
        for slot, count in zip(slots, counts)
    }


    return [
        ["avg_trip_duration", avg_trip_duration, "minutes"],
        ["avg_trip_distance", avg_trip_distance, "kilometers"],
        ["avg_trip_route_distance", avg_trip_route_distance, "kilometers"],
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




def export_excel(spark, kpis, target_dir):
    data = {}
    for month, name, value, _ in kpis:
        month = str(month)
        if month == "global":
            period = "Gesamt"
        elif len(month) == 6 and month.isdigit():
            period = f"{month[:4]}-{month[4:]}"
        else:
            period = month

        data.setdefault(period, {})[name] = value

    periods = sorted(data, key=lambda period: (period != "Gesamt", period))
    share_columns = (
        ("usage_share_by_gender", (
            ("male", "Maennlich (%)"),
            ("female", "Weiblich (%)"),
            ("undefined", "Unbekannt (%)"),
        )),
        ("usage_share_by_age", (
            ("under_20", "Unter 20 (in %)"),
            ("20_to_40", "20 bis 39 (in %)"),
            ("40_to_60", "40 bis 60 (in %)"),
            ("above_60", "Ueber 60 (in %)"),
            ("unknown", "Alter unbekannt (in %)"),
        )),
        ("usage_share_by_timeslot", (
            ("0-6", "00:00-06:00 (in %)"),
            ("6-12", "06:00-12:00 (in %)"),
            ("12-18", "12:00-18:00 (in %)"),
            ("18-24", "18:00-24:00 (in %)"),
        )),
    )
    fields = [
        StructField("Zeitraum", StringType(), nullable=False),
        StructField("Fahrtdauer (Min.)", DoubleType(), nullable=True),
        StructField("Luftlinienentfernung (km)", DoubleType(), nullable=True),
        StructField("Routenentfernung geschätzt OSRM (km)", DoubleType(), nullable=True),
    ]

    for _, dimensions in share_columns:
        fields.extend(
            StructField(label, DoubleType(), nullable=False)
            for _, label in dimensions
        )

    top_groups = (
        ("top_10_most_used_bikes", "Fahrrad-ID", "Fahrrad"),
        ("top_10_most_used_start_stations", "Startstation", "Startstation"),
        ("top_10_most_used_end_stations", "Endstation", "Endstation"),
    )
    for _, id_label, group_label in top_groups:
        for rank in range(1, 11):
            fields.append(StructField(f"{id_label} {rank}", StringType(), nullable=True))
            fields.append(StructField(f"{group_label} Fahrten {rank}", LongType(), nullable=True))

    rows = []
    for period in periods:
        values = data[period]
        row = [
            period,
            values.get("avg_trip_duration"),
            values.get("avg_trip_distance"),
            values.get("avg_trip_route_distance"),
        ]
        for kpi_name, dimensions in share_columns:
            shares = values.get(kpi_name, {})
            row.extend(shares.get(dimension, 0.0) for dimension, _ in dimensions)
        for kpi_name, _, _ in top_groups:
            ranked_values = values.get(kpi_name, [])[:10]
            for item_id, count in ranked_values:
                row.extend((str(item_id) if item_id is not None else None, int(count)))
            row.extend([None] * (2 * (10 - len(ranked_values))))
        rows.append(tuple(row))

    excel_path = f"{target_dir.rstrip('/')}/kpis.xlsx"
    df = spark.createDataFrame(rows, StructType(fields)).coalesce(1)
    (
        df.write
        .format("com.crealytics.spark.excel")
        .option("useHeader", "true")
        .mode("overwrite")
        .save(excel_path)
    )



def run(spark, args):
    kpis = calc_month_kpis(spark, args.hdfs_trip_dir, args.hdfs_station_dir)
    export_excel(spark, kpis, args.hdfs_target_dir)



def main() -> None:
    args = get_args()
    spark = SparkSession.builder.appName("hubway_calculate_kpis").getOrCreate()
    try:
        run(spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()