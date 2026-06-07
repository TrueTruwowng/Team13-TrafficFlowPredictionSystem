import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import DataFrame, functions as F
from dotenv import load_dotenv

from spark_session import get_spark
from config import GCS_BUCKET, CONGESTION_SPEED_THRESHOLD_KMH

load_dotenv()

spark = None  # injected via start_streams(); created locally when run standalone

SILVER_PATH = f"gs://{GCS_BUCKET}/silver"
GOLD_PATH   = f"gs://{GCS_BUCKET}/gold"
GOLD_CKPT   = f"gs://{GCS_BUCKET}/checkpoints/gold"

# SUMO tra toc do theo m/s, config dinh nghia nguong theo km/h
CONGESTION_SPEED_THRESHOLD = CONGESTION_SPEED_THRESHOLD_KMH / 3.6


def _write_gold_batch(batch_df: DataFrame, batch_id: int):
    if batch_df.isEmpty():
        return

    try:
        # batch_df da duoc aggregate o silver: 1 dong / (road_name, recordDatetime)
        # Lay cac (road_name, date, hour) bi anh huong de tinh peakFlow/minFlow trong gio
        ts_col = F.to_timestamp("recordDatetime", "yyyy-MM-dd'T'HH:mm")
        batch_with_hour = batch_df.withColumn("hour", F.hour(ts_col))
        affected_road_hours = batch_with_hour.select("road_name", "date", "hour").distinct()
        date_list = [r["date"] for r in affected_road_hours.select("date").distinct().collect()]

        silver_all = (
            batch_df.sparkSession.read.format("delta").load(SILVER_PATH)
            .filter(F.col("date").isin(date_list))
            .withColumn("hour", F.hour(F.to_timestamp("recordDatetime", "yyyy-MM-dd'T'HH:mm")))
            .join(affected_road_hours, on=["road_name", "date", "hour"], how="inner")
        )

        # Tinh peakFlow / minFlow theo gio cho tung tuyen duong (cap nhat moi 3p trong gio)
        hourly_stats = (
            silver_all
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
        hourly_stats_rn = hourly_stats.withColumnRenamed("road_name", "roadName")

        new_gold = (
            batch_with_hour
            .join(hourly_stats, on=["road_name", "date", "hour"], how="left")
            .drop("hour")
            .withColumnRenamed("road_name", "roadName")
            .withColumn("totalVehicle", F.col("total_entered").cast("long"))
            .withColumn("congestionHour",
                F.when(F.col("avg_speed") < CONGESTION_SPEED_THRESHOLD, 1).otherwise(0))
            .withColumn("actualFlow", F.col("avg_flow"))
            .select(*GOLD_COLS)
        )

        from delta.tables import DeltaTable
        gold_exists = DeltaTable.isDeltaTable(batch_df.sparkSession, GOLD_PATH)

        if gold_exists:
            # Re-apply updated peakFlow/minFlow to existing Gold records in the same hours
            affected_rn = affected_road_hours.withColumnRenamed("road_name", "roadName")
            existing_gold = (
                batch_df.sparkSession.read.format("delta").load(GOLD_PATH)
                .filter(F.col("date").isin(date_list))
                .withColumn("hour", F.hour(F.to_timestamp("recordDatetime", "yyyy-MM-dd'T'HH:mm")))
                .join(affected_rn, on=["roadName", "date", "hour"], how="inner")
                .join(
                    new_gold.select("roadName", "date", "recordDatetime"),
                    on=["roadName", "date", "recordDatetime"],
                    how="left_anti",  # exclude records already in new_gold
                )
                .drop("peakFlow", "minFlow")
                .join(hourly_stats_rn, on=["roadName", "date", "hour"], how="left")
                .drop("hour")
                .select(*GOLD_COLS)
            )
            gold_df = new_gold.unionByName(existing_gold).dropDuplicates(["roadName", "date", "recordDatetime"])
        else:
            gold_df = new_gold

        gold_df.cache()
        count = gold_df.count()

        if gold_exists:
            (
                DeltaTable.forPath(batch_df.sparkSession, GOLD_PATH)
                .alias("t")
                .merge(
                    gold_df.alias("s"),
                    "t.date = s.date AND t.roadName = s.roadName AND t.recordDatetime = s.recordDatetime",
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
        .option("schemaEvolutionMode", "addNewColumns")
        .option("skipChangeCommits", "true")
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
