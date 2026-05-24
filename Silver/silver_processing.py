import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import DataFrame, functions as F
from dotenv import load_dotenv

from spark_session import get_spark
from config import GCS_BUCKET

load_dotenv()

spark = None  # injected via start_streams(); created locally when run standalone

TRAFFIC_BRONZE        = f"gs://{GCS_BUCKET}/bronze/traffic"
WEATHER_BRONZE        = f"gs://{GCS_BUCKET}/bronze/weather"
SILVER_PATH           = f"gs://{GCS_BUCKET}/silver"
SILVER_CKPT           = f"gs://{GCS_BUCKET}/checkpoints/silver"
WEATHER_INTERVAL_SECS = 180  # 3 minutes, same as traffic interval


def _load_weather() -> DataFrame:
    return (
        spark.read.format("delta").load(WEATHER_BRONZE)
        .select(
            "road_name", "datetime",
            "temperature", "windspeed", "precipitation", "weather",
        )
    )


def _write_silver_batch(batch_df: DataFrame, batch_id: int):
    if batch_df.isEmpty():
        return

    try:
        # Floor recordDatetime xuong moc weather gan nhat de join
        traffic = batch_df.withColumn(
            "weather_time",
            F.from_unixtime(
                (F.unix_timestamp("recordDatetime", "yyyy-MM-dd'T'HH:mm")
                 / WEATHER_INTERVAL_SECS).cast("long") * WEATHER_INTERVAL_SECS,
                "yyyy-MM-dd'T'HH:mm",
            ),
        )

        w = (
            _load_weather()
            .withColumnRenamed("road_name", "w_road_name")
            .withColumnRenamed("datetime",  "w_datetime")
        )

        silver_df = (
            traffic
            .join(
                w,
                (traffic["road_name"]    == w["w_road_name"]) &
                (traffic["weather_time"] == w["w_datetime"]),
                how="inner",
            )
            .drop("w_road_name", "w_datetime", "weather_time")
            .withColumn("date", F.to_date("recordDatetime", "yyyy-MM-dd'T'HH:mm"))
        )

        silver_df.cache()
        count = silver_df.count()
        (
            silver_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("date")
            .save(SILVER_PATH)
        )
        silver_df.unpersist()
        print(f"[silver] batch {batch_id}: {count} rows → delta", flush=True)
    except Exception as e:
        print(f"[silver] batch {batch_id}: ERROR — {e}", flush=True)
        raise


def _wait_for_delta(path: str, spark_session=None, retries: int = 40, delay: int = 30) -> None:
    """Block until a Delta table exists and has a committed schema."""
    import time
    s = spark_session or spark
    for i in range(1, retries + 1):
        try:
            s.read.format("delta").load(path).limit(0).count()
            return
        except Exception:
            print(f"[silver] Waiting for Delta table {path} ({i}/{retries})…")
            time.sleep(delay)
    raise RuntimeError(f"Delta table not ready after {retries} attempts: {path}")


def start_streams(spark_session):
    """Start silver stream. Does NOT block."""
    global spark
    spark = spark_session

    traffic_stream = (
        spark.readStream
        .format("delta")
        .load(TRAFFIC_BRONZE)
        .withColumn("event_time", F.to_timestamp("recordDatetime", "yyyy-MM-dd'T'HH:mm"))
        .withWatermark("event_time", "5 minutes")
    )

    traffic_stream.writeStream \
        .foreachBatch(_write_silver_batch) \
        .option("checkpointLocation", SILVER_CKPT) \
        .trigger(processingTime="3 minutes") \
        .start()


def run():
    s = get_spark("SilverProcessing")
    _wait_for_delta(TRAFFIC_BRONZE, spark_session=s)
    _wait_for_delta(WEATHER_BRONZE, spark_session=s)
    start_streams(s)
    s.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()
