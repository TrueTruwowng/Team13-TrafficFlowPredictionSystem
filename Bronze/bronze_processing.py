import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType,
)
from dotenv import load_dotenv

from spark_session import get_spark
from config import GCS_BUCKET

load_dotenv()

spark = None  # injected via start_streams(); created locally when run standalone

TRAFFIC_LANDING = f"gs://{GCS_BUCKET}/landing/traffic/*/*.json"
WEATHER_LANDING = f"gs://{GCS_BUCKET}/landing/weather/*/*.json"
TRAFFIC_BRONZE  = f"gs://{GCS_BUCKET}/bronze/traffic"
WEATHER_BRONZE  = f"gs://{GCS_BUCKET}/bronze/weather"
TRAFFIC_CKPT    = f"gs://{GCS_BUCKET}/checkpoints/bronze_traffic"
WEATHER_CKPT    = f"gs://{GCS_BUCKET}/checkpoints/bronze_weather"

# ── Schemas ────────────────────────────────────────────────────────────────────

traffic_landing_schema = StructType([
    StructField("id",             StringType(), True),
    StructField("road_name",      StringType(), True),
    StructField("date",           StringType(), True),
    StructField("begin",          StringType(), True),
    StructField("end",            StringType(), True),
    StructField("laneDensity",    StringType(), True),
    StructField("occupancy",      StringType(), True),
    StructField("waitingTime",    StringType(), True),
    StructField("speed",          StringType(), True),
    StructField("sampledSeconds", StringType(), True),
    StructField("traveltime",     StringType(), True),
    StructField("flow",           StringType(), True),
    StructField("entered",        StringType(), True),
    StructField("left",           StringType(), True),
    StructField("timeLoss",       StringType(), True),
])

weather_landing_schema = StructType([
    StructField("road_id",        StringType(),  True),
    StructField("road_name",      StringType(),  True),
    StructField("date",           StringType(),  True),
    StructField("begin",          StringType(),  True),
    StructField("end",            StringType(),  True),
    StructField("temperature",    DoubleType(),  True),
    StructField("windspeed",      DoubleType(),  True),
    StructField("humidity",       IntegerType(), True),
    StructField("precipitation",  DoubleType(),  True),
    StructField("weathercode",    IntegerType(), True),
    StructField("weather",        StringType(),  True),
    StructField("ingestion_time", StringType(),  True),
])

# ── Transform helpers ──────────────────────────────────────────────────────────

def _transform_traffic(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("id"),
        F.col("road_name"),
        F.date_format(
            F.concat(F.col("date"), F.lit("T"), F.col("end")).cast("timestamp"),
            "yyyy-MM-dd'T'HH:mm"
        ).alias("recordDatetime"),
        F.col("laneDensity").cast(DoubleType()),
        F.col("occupancy").cast(DoubleType()),
        F.col("waitingTime").cast(DoubleType()),
        F.col("speed").cast(DoubleType()),
        F.col("sampledSeconds").cast(DoubleType()),
        F.col("traveltime").cast(DoubleType()),
        F.col("flow").cast(DoubleType()),
        F.col("entered").cast(DoubleType()),
        F.col("left").cast(DoubleType()),
        F.col("timeLoss").cast(DoubleType()).alias("timeloss"),
        F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm").alias("processDatetime"),
        F.col("date"),
    )


def _transform_weather(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("road_id"),
        F.col("road_name"),
        F.date_format(
            F.concat(F.col("date"), F.lit("T"), F.col("end")).cast("timestamp"),
            "yyyy-MM-dd'T'HH:mm"
        ).alias("datetime"),
        F.col("temperature"),
        F.col("windspeed"),
        F.col("humidity"),
        F.col("precipitation"),
        F.col("weather"),
        F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm").alias("processDatetime"),
        F.col("date"),
    )

# ── foreachBatch handlers ──────────────────────────────────────────────────────

def _write_traffic_batch(batch_df: DataFrame, batch_id: int):
    if batch_df.isEmpty():
        return
    count = batch_df.count()
    typed = _transform_traffic(batch_df)
    (
        typed.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("date")
        .save(TRAFFIC_BRONZE)
    )
    print(f"[bronze/traffic] batch {batch_id}: {count} rows → delta", flush=True)


def _write_weather_batch(batch_df: DataFrame, batch_id: int):
    if batch_df.isEmpty():
        return
    count = batch_df.count()
    (
        _transform_weather(batch_df).write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("date")
        .save(WEATHER_BRONZE)
    )
    print(f"[bronze/weather] batch {batch_id}: {count} rows → delta", flush=True)

# ── Streaming queries ──────────────────────────────────────────────────────────

def start_streams(spark_session):
    """Start bronze traffic + weather streams. Does NOT block."""
    global spark
    spark = spark_session

    traffic_stream = (
        spark.readStream
        .schema(traffic_landing_schema)
        .option("recursiveFileLookup", "true")
        .json(TRAFFIC_LANDING)
        .withColumn("recordDatetime",
            F.concat(F.col("date"), F.lit("T"), F.col("end")).cast("timestamp"))
        .withWatermark("recordDatetime", "5 minutes")
    )

    weather_stream = (
        spark.readStream
        .schema(weather_landing_schema)
        .option("recursiveFileLookup", "true")
        .json(WEATHER_LANDING)
        .withColumn("datetime",
            F.concat(F.col("date"), F.lit("T"), F.col("end")).cast("timestamp"))
        .withWatermark("datetime", "5 minutes")
    )

    traffic_stream.writeStream \
        .foreachBatch(_write_traffic_batch) \
        .option("checkpointLocation", TRAFFIC_CKPT) \
        .trigger(processingTime="3 minutes") \
        .start()

    weather_stream.writeStream \
        .foreachBatch(_write_weather_batch) \
        .option("checkpointLocation", WEATHER_CKPT) \
        .trigger(processingTime="3 minutes") \
        .start()


def run():
    start_streams(get_spark("BronzeProcessing"))
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()
