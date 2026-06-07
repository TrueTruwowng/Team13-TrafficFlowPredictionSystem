"""
Batch backfill for a single missing date.

Reads simulation traffic JSON directly from GCS (no Kafka),
fetches historical weather from Open-Meteo Archive API if missing,
then applies the full bronze → silver → gold pipeline and MERGEs
results into the existing Delta tables.

Usage:
    spark-submit --packages io.delta:delta-spark_2.12:3.2.0 \\
        /home/dis/Project/backfill.py 2026-05-27
"""

import csv
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)

from Bronze.bronze_processing import _transform_traffic, traffic_landing_schema
from config import CONGESTION_SPEED_THRESHOLD_KMH, GCS_BUCKET
from spark_session import get_spark
from utils.api_utils import WeatherAPI

load_dotenv()

if len(sys.argv) < 2:
    print("Usage: spark-submit backfill.py YYYY-MM-DD")
    sys.exit(1)

TARGET_DATE = sys.argv[1]

CONGESTION_SPEED_THRESHOLD = CONGESTION_SPEED_THRESHOLD_KMH / 3.6
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
LOOKUP_PATH = os.path.join(PROJECT_DIR, "data", "road_lookup_table.csv")

TRAFFIC_BRONZE = f"gs://{GCS_BUCKET}/bronze/traffic"
WEATHER_BRONZE = f"gs://{GCS_BUCKET}/bronze/weather"
SILVER_PATH    = f"gs://{GCS_BUCKET}/silver"
GOLD_PATH      = f"gs://{GCS_BUCKET}/gold"

# Schema cho weather DataFrame khớp với bronze/weather Delta schema
_WEATHER_SCHEMA = StructType([
    StructField("road_id",         StringType(),  True),
    StructField("road_name",       StringType(),  True),
    StructField("datetime",        StringType(),  True),
    StructField("temperature",     DoubleType(),  True),
    StructField("windspeed",       DoubleType(),  True),
    StructField("humidity",        IntegerType(), True),
    StructField("precipitation",   DoubleType(),  True),
    StructField("weather",         StringType(),  True),
    StructField("processDatetime", StringType(),  True),
    StructField("date",            StringType(),  True),
])


def _load_roads() -> list[dict]:
    seen, roads = set(), []
    with open(LOOKUP_PATH, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["name"] in seen:
                continue
            seen.add(r["name"])
            roads.append({
                "id":  r["way_id"],
                "name": r["name"],
                "lat": float(r["lat"]),
                "lon": float(r["lon"]),
            })
    return roads


def _fetch_weather_records(target_date: str) -> list[dict]:
    """
    Fetch hourly weather from Open-Meteo Archive for every road on target_date.
    Returns a list of dicts compatible with _WEATHER_SCHEMA.
    """
    api   = WeatherAPI()
    roads = _load_roads()
    now_s = datetime.now().strftime("%Y-%m-%dT%H:%M")
    records = []

    for road in roads:
        hourly = api.get_historical_weather(road["lat"], road["lon"], target_date, target_date)
        for h in hourly:
            raw_humidity = h.get("humidity")
            records.append({
                "road_id":         road["id"],
                "road_name":       road["name"],
                "datetime":        h["datetime"],        # "2026-05-27T07:00"
                "temperature":     h.get("temperature"),
                "windspeed":       h.get("windspeed"),
                "humidity":        int(raw_humidity) if raw_humidity is not None else None,
                "precipitation":   h.get("precipitation") or 0.0,
                "weather":         h.get("weather", "Unknown"),
                "processDatetime": now_s,
                "date":            target_date,
            })

    print(f"[bronze/weather] Fetched {len(records)} archive records for {target_date}", flush=True)
    return records


def backfill(target_date: str):
    from delta.tables import DeltaTable

    spark = get_spark("Backfill")
    print(f"\n{'='*60}", flush=True)
    print(f"[backfill] Starting backfill for {target_date}", flush=True)
    print(f"{'='*60}\n", flush=True)

    # ── 1. BRONZE / TRAFFIC ───────────────────────────────────────────────────
    sim_path = f"gs://{GCS_BUCKET}/simulation/traffic/{target_date}/*.json"
    print(f"[bronze/traffic] Reading {sim_path}", flush=True)

    traffic_raw    = spark.read.schema(traffic_landing_schema).json(sim_path)
    traffic_bronze = _transform_traffic(traffic_raw)
    traffic_bronze.cache()
    count_bt = traffic_bronze.count()
    print(f"[bronze/traffic] {count_bt} rows read from simulation", flush=True)

    if count_bt == 0:
        print(f"[bronze/traffic] ERROR: No simulation data found at {sim_path}", flush=True)
        spark.stop()
        sys.exit(1)

    if DeltaTable.isDeltaTable(spark, TRAFFIC_BRONZE):
        (
            DeltaTable.forPath(spark, TRAFFIC_BRONZE)
            .alias("t")
            .merge(
                traffic_bronze.alias("s"),
                "t.date = s.date AND t.id = s.id AND t.recordDatetime = s.recordDatetime",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            traffic_bronze.write
            .format("delta")
            .option("mergeSchema", "true")
            .partitionBy("date")
            .save(TRAFFIC_BRONZE)
        )
    traffic_bronze.unpersist()
    print(f"[bronze/traffic] Done: {count_bt} rows → delta", flush=True)

    # ── 2. BRONZE / WEATHER (fetch from archive if hourly weather missing) ────
    # Silver join floor weather_time xuong HH:00, nen can weather o moc HH:00.
    # Streaming weather chi co moc HH:03/HH:06… → KHONG khop. Vi vay phai kiem
    # tra rieng moc HH:00 (archive) thay vi chi dem so dong > 0.
    try:
        hourly_w = (
            spark.read.format("delta").load(WEATHER_BRONZE)
            .filter(F.col("date") == target_date)
            .filter(F.col("datetime").endswith(":00"))
            .count()
        )
    except Exception:
        hourly_w = 0

    if hourly_w > 0:
        print(f"[bronze/weather] Already has {hourly_w} hourly rows for {target_date}, skipping fetch", flush=True)
    else:
        print(f"[bronze/weather] No hourly weather for {target_date}, fetching from Open-Meteo Archive…", flush=True)
        w_records = _fetch_weather_records(target_date)
        if not w_records:
            print("[bronze/weather] ERROR: Archive fetch returned 0 records. Aborting.", flush=True)
            spark.stop()
            sys.exit(1)

        weather_df = spark.createDataFrame(w_records, schema=_WEATHER_SCHEMA)

        if DeltaTable.isDeltaTable(spark, WEATHER_BRONZE):
            (
                DeltaTable.forPath(spark, WEATHER_BRONZE)
                .alias("t")
                .merge(
                    weather_df.alias("s"),
                    "t.date = s.date AND t.road_name = s.road_name AND t.datetime = s.datetime",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            (
                weather_df.write
                .format("delta")
                .option("mergeSchema", "true")
                .partitionBy("date")
                .save(WEATHER_BRONZE)
            )
        print(f"[bronze/weather] Done: {len(w_records)} rows → delta", flush=True)

    # ── 3. SILVER ─────────────────────────────────────────────────────────────
    # Bronze/weather từ archive API là hourly (HH:00), nên floor weather_time
    # xuống giờ (thay vì 3 phút như streaming pipeline) để join khớp.
    traffic_for_silver = (
        spark.read.format("delta").load(TRAFFIC_BRONZE)
        .filter(F.col("date") == target_date)
        # Loai edge khong co ten trong OSM (road_name null) — junk, tranh row
        # rac voi peak/min null vi khong join duoc tren road_name null.
        .filter(F.col("road_name").isNotNull())
        .withColumn(
            "weather_time",
            F.date_format(
                F.to_timestamp("recordDatetime", "yyyy-MM-dd'T'HH:mm"),
                "yyyy-MM-dd'T'HH:00",
            ),
        )
    )

    weather_for_silver = (
        spark.read.format("delta").load(WEATHER_BRONZE)
        .filter(F.col("date") == target_date)
        .select("road_name", "datetime", "temperature", "windspeed", "precipitation", "weather")
        .withColumnRenamed("road_name", "w_road_name")
        .withColumnRenamed("datetime",  "w_datetime")
    )

    # LEFT JOIN: giu lai moi traffic interval ke ca khi thieu weather gio do.
    joined = (
        traffic_for_silver
        .join(
            weather_for_silver,
            (traffic_for_silver["road_name"]   == weather_for_silver["w_road_name"]) &
            (traffic_for_silver["weather_time"] == weather_for_silver["w_datetime"]),
            how="left",
        )
        .drop("w_road_name", "w_datetime", "weather_time")
    )

    # Silver muc 3 PHUT (giong silver_processing.py live): gom cac lane → 1 dong
    # / (road_name, moc 3 phut). KHONG tinh peak/min o silver — de gold tinh.
    silver_df = (
        joined
        .groupBy("road_name", "recordDatetime")
        .agg(
            F.round(F.avg("speed"),       2).alias("avg_speed"),
            F.round(F.avg("laneDensity"), 2).alias("avg_density"),
            F.round(F.avg("occupancy"),   2).alias("avg_occupancy"),
            F.round(F.avg("waitingTime"), 2).alias("avg_waitingTime"),
            F.round(F.avg("traveltime"),  2).alias("avg_traveltime"),
            F.coalesce(F.round(F.avg("flow"), 2), F.lit(0.0)).alias("avg_flow"),
            F.round(F.sum("entered"),     2).alias("total_entered"),
            F.round(F.sum("left"),        2).alias("total_left"),
            F.round(F.avg("timeloss"),    2).alias("avg_timeloss"),
            F.first("temperature").alias("temperature"),
            F.first("windspeed").alias("windspeed"),
            F.first("precipitation").alias("precipitation"),
            F.first("weather").alias("weather"),
        )
        .withColumn("date", F.to_date("recordDatetime", "yyyy-MM-dd'T'HH:mm"))
        # Loai cac dong bi cuon sang ngay khac (vd interval 24:00 → 00:00 hom sau)
        # de thoa man replaceWhere theo dung partition `date`.
        .filter(F.col("date") == F.lit(target_date))
    )

    silver_df.cache()
    count_s = silver_df.count()
    print(f"[silver] {count_s} rows after join + aggregation (3-min)", flush=True)

    if count_s == 0:
        print("[silver] ERROR: 0 rows after weather join. Check weather data covers this date.", flush=True)
        silver_df.unpersist()
        spark.stop()
        sys.exit(1)

    # Ghi de partition `date` (replaceWhere) → thay sach moi dinh dang cu (vd
    # hourly) bang dinh dang 3 phut, atomic, khong de lai dong rac.
    silver_writer = (
        silver_df.write
        .format("delta")
        .option("mergeSchema", "true")
        .partitionBy("date")
    )
    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        silver_writer.mode("overwrite").option("replaceWhere", f"date = '{target_date}'").save(SILVER_PATH)
    else:
        silver_writer.save(SILVER_PATH)
    silver_df.unpersist()
    print(f"[silver] Done: {count_s} rows → delta (replaceWhere {target_date})", flush=True)

    # ── 4. GOLD ───────────────────────────────────────────────────────────────
    # Giong gold_processing.py live: giu mức 3 phut, peakFlow/minFlow = max/min
    # cua avg_flow theo (road, date, hour), actualFlow = avg_flow tai moc do.
    ts_col = F.to_timestamp("recordDatetime", "yyyy-MM-dd'T'HH:mm")
    silver_day = (
        spark.read.format("delta").load(SILVER_PATH)
        .filter(F.col("date") == target_date)
        # Bo cot peak/min ton du tu schema hourly cu (neu co) — gold tu tinh lai,
        # tranh AMBIGUOUS_REFERENCE khi join voi hourly_stats.
        .drop("peakFlow", "minFlow")
        .withColumn("hour", F.hour(ts_col))
    )

    hourly_stats = (
        silver_day
        .groupBy("road_name", "date", "hour")
        .agg(
            F.round(F.max("avg_flow"), 2).alias("peakFlow"),
            F.round(F.min("avg_flow"), 2).alias("minFlow"),
        )
    )

    GOLD_COLS = [
        "roadName", "recordDatetime",
        "totalVehicle", "congestionHour",
        "peakFlow", "minFlow", "actualFlow",
        "avg_speed",
        "temperature", "windspeed", "precipitation", "weather",
        "date",
    ]

    gold_df = (
        silver_day
        .join(hourly_stats, on=["road_name", "date", "hour"], how="left")
        .drop("hour")
        .withColumnRenamed("road_name", "roadName")
        .withColumn("totalVehicle",  F.col("total_entered").cast("long"))
        .withColumn("congestionHour",
            F.when(F.col("avg_speed") < CONGESTION_SPEED_THRESHOLD, 1).otherwise(0))
        .withColumn("actualFlow", F.col("avg_flow"))
        .select(*GOLD_COLS)
    )

    gold_df.cache()
    count_g = gold_df.count()

    gold_writer = (
        gold_df.write
        .format("delta")
        .option("mergeSchema", "true")
        .partitionBy("date")
    )
    if DeltaTable.isDeltaTable(spark, GOLD_PATH):
        gold_writer.mode("overwrite").option("replaceWhere", f"date = '{target_date}'").save(GOLD_PATH)
    else:
        gold_writer.save(GOLD_PATH)

    gold_df.unpersist()
    print(f"[gold] Done: {count_g} rows → delta (replaceWhere {target_date})", flush=True)

    print(f"\n{'='*60}", flush=True)
    print(f"[backfill] COMPLETE for {target_date}", flush=True)
    print(f"  bronze/traffic : {count_bt} rows", flush=True)
    print(f"  silver         : {count_s} rows", flush=True)
    print(f"  gold           : {count_g} rows", flush=True)
    print(f"{'='*60}\n", flush=True)
    spark.stop()


if __name__ == "__main__":
    backfill(TARGET_DATE)
