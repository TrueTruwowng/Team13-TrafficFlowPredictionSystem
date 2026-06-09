import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import DataFrame, functions as F
from dotenv import load_dotenv

from spark_session import get_spark
from config import GCS_PATHS

load_dotenv()

spark = None

TRAFFIC_BRONZE  = GCS_PATHS["bronze_traffic"]
WEATHER_BRONZE  = GCS_PATHS["bronze_weather"]
SILVER_PATH     = GCS_PATHS["silver"]
SILVER_CKPT     = GCS_PATHS["ckpt_silver"]

# Weather records arrive every 3 minutes; floor recordDatetime to the nearest
# 3-minute mark so traffic rows join correctly against weather rows.
WEATHER_INTERVAL_SECS = 180


def _load_weather() -> DataFrame:
    return (
        spark.read.format("delta").load(WEATHER_BRONZE)
        .select("road_name", "datetime",
                "temperature", "windspeed", "precipitation", "weather")
    )


def _write_silver_batch(batch_df: DataFrame, batch_id: int):
    if batch_df.isEmpty():
        return

    try:
        # Drop edges with no OSM road name — they produce null peak/min values
        # because they can't join on road_name, and pollute the silver table.
        batch_df = batch_df.filter(F.col("road_name").isNotNull())

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

        # LEFT JOIN so traffic rows are never dropped due to missing weather intervals.
        joined_df = (
            traffic
            .join(
                w,
                (traffic["road_name"]    == w["w_road_name"]) &
                (traffic["weather_time"] == w["w_datetime"]),
                how="left",
            )
            .drop("w_road_name", "w_datetime", "weather_time")
        )

        # Aggregate multiple SUMO lanes into one row per (road_name, recordDatetime).
        silver_df = (
            joined_df
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
        )

        silver_df.cache()
        count = silver_df.count()
        if count == 0:
            silver_df.unpersist()
            return

        from delta.tables import DeltaTable
        if DeltaTable.isDeltaTable(batch_df.sparkSession, SILVER_PATH):
            (
                DeltaTable.forPath(batch_df.sparkSession, SILVER_PATH)
                .alias("t")
                .merge(
                    silver_df.alias("s"),
                    "t.date = s.date AND t.road_name = s.road_name AND t.recordDatetime = s.recordDatetime",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            (
                silver_df.write
                .format("delta")
                .option("mergeSchema", "true")
                .partitionBy("date")
                .save(SILVER_PATH)
            )

        silver_df.unpersist()
        print(f"[silver] batch {batch_id}: {count} rows → delta (upsert)", flush=True)
    except Exception as e:
        print(f"[silver] batch {batch_id}: ERROR — {e}", flush=True)
        raise


def _wait_for_delta(path: str, spark_session=None, retries: int = 40, delay: int = 30) -> None:
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
        .option("schemaEvolutionMode", "addNewColumns")
        .option("skipChangeCommits", "true")
        .load(TRAFFIC_BRONZE)
        .withColumn("event_time", F.to_timestamp("recordDatetime", "yyyy-MM-dd'T'HH:mm"))
        .withWatermark("event_time", "5 minutes")
    )

    traffic_stream.writeStream \
        .foreachBatch(_write_silver_batch) \
        .option("checkpointLocation", SILVER_CKPT) \
        .trigger(processingTime="60 seconds") \
        .start()


def run():
    s = get_spark("SilverProcessing")
    _wait_for_delta(TRAFFIC_BRONZE, spark_session=s)
    _wait_for_delta(WEATHER_BRONZE, spark_session=s)
    start_streams(s)
    s.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()
