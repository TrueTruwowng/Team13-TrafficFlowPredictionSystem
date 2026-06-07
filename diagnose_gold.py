"""
Per-day diagnostic for the Gold table.

Compares source dates (simulation/traffic on GCS) against what actually
landed in Gold, so we know exactly which days are MISSING (loại 1) vs
SPARSE (loại 2), and how badly the peakFlow==minFlow issue affects each day.

Usage:
    spark-submit --packages io.delta:delta-spark_2.12:3.2.0 \\
        /home/dis/Project/diagnose_gold.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
from pyspark.sql import functions as F

from config import GCS_BUCKET
from spark_session import get_spark

load_dotenv()

SILVER_PATH = f"gs://{GCS_BUCKET}/silver"
GOLD_PATH   = f"gs://{GCS_BUCKET}/gold"
SIM_PREFIX  = "simulation/traffic"

# Mỗi ngày đầy đủ 24h, mỗi giờ 20 interval (3 phút) → ~20*24 record/road.
# Ở đây xét theo số GIỜ có dữ liệu là đủ để phân loại.
FULL_DAY_HOURS = 24
SPARSE_HOUR_THRESHOLD = 20  # < ngưỡng này coi là lác đác


def _source_dates(spark) -> set[str]:
    """Các ngày có simulation traffic trên GCS (ground truth).

    Dùng Hadoop FileSystem của Spark (đã cấu hình GCS connector) thay vì
    google.cloud — vì Python env của spark-submit không có thư viện đó.
    """
    base = f"gs://{GCS_BUCKET}/{SIM_PREFIX}"
    jvm  = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    path  = jvm.org.apache.hadoop.fs.Path(base)
    fs    = path.getFileSystem(hconf)

    dates = set()
    if not fs.exists(path):
        return dates
    for status in fs.listStatus(path):
        if status.isDirectory():
            d = status.getPath().getName()
            if len(d) == 10 and d[4] == "-":
                dates.add(d)
    return dates


def diagnose():
    spark = get_spark("DiagnoseGold")

    source = _source_dates(spark)
    print(f"\n[source] {len(source)} ngày có simulation traffic trên GCS\n", flush=True)

    # ── Gold per-day stats ────────────────────────────────────────────────────
    try:
        gold = (
            spark.read.format("delta").load(GOLD_PATH)
            .withColumn("hour", F.hour(F.to_timestamp("recordDatetime", "yyyy-MM-dd'T'HH:mm")))
        )
        gold_stats = (
            gold.withColumn("date", F.date_format(F.col("date").cast("date"), "yyyy-MM-dd"))
            .groupBy("date")
            .agg(
                F.count("*").alias("rows"),
                F.countDistinct("hour").alias("hours"),
                F.sum(F.when(F.col("peakFlow") == F.col("minFlow"), 1).otherwise(0)).alias("peak_eq_min"),
            )
            .collect()
        )
        gold_map = {r["date"]: r for r in gold_stats}
    except Exception as e:
        print(f"[gold] Không đọc được Gold: {e}", flush=True)
        gold_map = {}

    # ── Build verdict table ───────────────────────────────────────────────────
    all_dates = sorted(source | set(gold_map.keys()))

    print(f"{'date':<12}{'gold_rows':>10}{'hours':>7}{'peak=min%':>11}  verdict", flush=True)
    print("-" * 70, flush=True)

    missing, sparse, ok = [], [], []
    for d in all_dates:
        g = gold_map.get(d)
        in_source = d in source

        if g is None:
            verdict = "MISSING (loại 1)" if in_source else "MISSING (no source!)"
            missing.append(d)
            print(f"{d:<12}{'-':>10}{'-':>7}{'-':>11}  {verdict}", flush=True)
            continue

        rows  = g["rows"]
        hours = g["hours"]
        peak_pct = (g["peak_eq_min"] / rows * 100) if rows else 0

        if hours < SPARSE_HOUR_THRESHOLD:
            verdict = "SPARSE (loại 2)"
            sparse.append(d)
        else:
            verdict = "OK"
            ok.append(d)
        if not in_source:
            verdict += " [no source]"

        print(f"{d:<12}{rows:>10}{hours:>7}{peak_pct:>10.0f}%  {verdict}", flush=True)

    # ── Summary ─────────────────────────────────────────────────────────────────
    print("\n" + "=" * 70, flush=True)
    print(f"MISSING (loại 1, cần backfill): {len(missing)}", flush=True)
    if missing:
        print("  " + " ".join(missing), flush=True)
    print(f"SPARSE  (loại 2, cần backfill): {len(sparse)}", flush=True)
    if sparse:
        print("  " + " ".join(sparse), flush=True)
    print(f"OK: {len(ok)}", flush=True)

    backfill_list = sorted(set(missing) & source | set(sparse))
    print("\n[backfill candidates] (có source + cần sửa):", flush=True)
    print("  " + " ".join(backfill_list) if backfill_list else "  (none)", flush=True)
    print("=" * 70 + "\n", flush=True)

    spark.stop()


if __name__ == "__main__":
    diagnose()
