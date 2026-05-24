import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import DataFrame, functions as F
from dotenv import load_dotenv

from spark_session import get_spark
from config import GCS_BUCKET

load_dotenv()

spark = None  # injected via start_streams(); created locally when run standalone

SILVER_PATH = f"gs://{GCS_BUCKET}/silver"
GOLD_PATH   = f"gs://{GCS_BUCKET}/gold"
GOLD_CKPT   = f"gs://{GCS_BUCKET}/checkpoints/gold"


def _write_gold_batch(batch_df: DataFrame, batch_id: int):
    if batch_df.isEmpty():
        return

    try:
        # Lấy các (road_name, recordDatetime) có trong batch này
        affected_keys = batch_df.select("road_name", "recordDatetime").distinct()

        # Re-aggregate toàn bộ silver cho các key đó (kể cả data cũ + late data)
        silver_full = (
            batch_df.sparkSession.read.format("delta").load(SILVER_PATH)
            .join(affected_keys, on=["road_name", "recordDatetime"], how="inner")
        )
        gold_df = (
            silver_full
            .groupBy("road_name", "recordDatetime")
            .agg(
                F.round(F.avg("speed"),       2).alias("avg_speed"),
                F.round(F.avg("laneDensity"), 2).alias("avg_density"),
                F.round(F.avg("occupancy"),   2).alias("avg_occupancy"),
                F.round(F.avg("waitingTime"), 2).alias("avg_waitingTime"),
                F.round(F.avg("traveltime"),  2).alias("avg_traveltime"),
                F.round(F.avg("flow"),        2).alias("avg_flow"),
                F.round(F.sum("entered"),     2).alias("total_entered"),
                F.round(F.sum("left"),        2).alias("total_left"),
                F.round(F.avg("timeloss"),    2).alias("avg_timeloss"),
                F.first("temperature").alias("temperature"),
                F.first("windspeed").alias("windspeed"),
                F.first("precipitation").alias("precipitation"),
                F.first("weather").alias("weather"),
            )
            .withColumn("date", F.to_date("recordDatetime", "yyyy-MM-dd'T'HH:mm"))
        )

        gold_df.cache()
        count = gold_df.count()

        from delta.tables import DeltaTable
        if DeltaTable.isDeltaTable(batch_df.sparkSession, GOLD_PATH):
            (
                DeltaTable.forPath(batch_df.sparkSession, GOLD_PATH)
                .alias("t")
                .merge(
                    gold_df.alias("s"),
                    "t.road_name = s.road_name AND t.recordDatetime = s.recordDatetime",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            (
                gold_df.write
                .format("delta")
                .option("mergeSchema", "true")
                .partitionBy("date")
                .save(GOLD_PATH)
            )

        gold_df.unpersist()
        print(f"[gold] batch {batch_id}: {count} rows → delta (upsert)", flush=True)
    except Exception as e:
        print(f"[gold] batch {batch_id}: ERROR — {e}", flush=True)
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
            print(f"[gold] Waiting for Delta table {path} ({i}/{retries})…")
            time.sleep(delay)
    raise RuntimeError(f"Delta table not ready after {retries} attempts: {path}")


def start_streams(spark_session):
    """Start gold stream. Does NOT block."""
    global spark
    spark = spark_session

    silver_stream = (
        spark.readStream
        .format("delta")
        .load(SILVER_PATH)
        .withColumn("event_time", F.to_timestamp("recordDatetime", "yyyy-MM-dd'T'HH:mm"))
        .withWatermark("event_time", "5 minutes")
    )

    silver_stream.writeStream \
        .foreachBatch(_write_gold_batch) \
        .option("checkpointLocation", GOLD_CKPT) \
        .trigger(processingTime="3 minutes") \
        .start()


def run():
    s = get_spark("GoldProcessing")
    _wait_for_delta(SILVER_PATH, spark_session=s)
    start_streams(s)
    s.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()
