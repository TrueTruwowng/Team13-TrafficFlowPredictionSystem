import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()

TIMEZONE = "Asia/Ho_Chi_Minh"
VN_TZ    = ZoneInfo(TIMEZONE)

def now_vn() -> datetime:
    return datetime.now(tz=VN_TZ)

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

KAFKA_TOPICS = {
    "traffic_simulate": "traffic_simulate",
    "weather":          "weather_raw",
}

# GCS
GCS_BUCKET = os.getenv("GCS_BUCKET_NAME", "big-data-storage13")

GCS_PATHS = {
    # Landing (raw JSON written by Kafka consumer)
    "landing_traffic":     f"gs://{GCS_BUCKET}/landing/traffic",
    "landing_weather":     f"gs://{GCS_BUCKET}/landing/weather",

    # Simulation source (pre-generated SUMO data)
    "simulation_traffic":  f"gs://{GCS_BUCKET}/simulation/traffic",

    # Bronze (Delta Lake)
    "bronze_traffic":      f"gs://{GCS_BUCKET}/bronze/traffic",
    "bronze_weather":      f"gs://{GCS_BUCKET}/bronze/weather",

    # Silver (Delta Lake)
    "silver":              f"gs://{GCS_BUCKET}/silver",

    # Gold (Delta Lake)
    "gold":                f"gs://{GCS_BUCKET}/gold",

    # Checkpoints
    "ckpt_bronze_traffic": f"gs://{GCS_BUCKET}/checkpoints/bronze_traffic",
    "ckpt_bronze_weather": f"gs://{GCS_BUCKET}/checkpoints/bronze_weather",
    "ckpt_silver":         f"gs://{GCS_BUCKET}/checkpoints/silver",
    "ckpt_gold":           f"gs://{GCS_BUCKET}/checkpoints/gold",
}

# Spark
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://master14:7077")

SPARK_CONF = {
    "spark.sql.extensions":
        "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog":
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.fs.gs.impl":
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    "spark.hadoop.fs.AbstractFileSystem.gs.impl":
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
    "spark.hadoop.fs.gs.auth.type": "APPLICATION_DEFAULT",
    # 3 executors x 2 cores x 2 = 12 shuffle partitions
    "spark.driver.memory":          "4g",
    "spark.driver.maxResultSize":   "2g",
    "spark.executor.instances":     "3",
    "spark.executor.cores":         "2",
    "spark.executor.memory":        "5g",
    "spark.sql.shuffle.partitions": "12",
}

# Business logic
CONGESTION_SPEED_THRESHOLD_KMH = 20  # below this (km/h) is counted as congested

# Binary paths — auto-detected; override via .env if needed
SPARK_SUBMIT = os.getenv("SPARK_SUBMIT", shutil.which("spark-submit") or "/opt/spark/bin/spark-submit")
UVICORN_BIN  = os.getenv("UVICORN_BIN",  str(Path(sys.executable).parent / "uvicorn"))
NPM_BIN      = os.getenv("NPM_BIN",      shutil.which("npm") or "npm")
FRONTEND_DIR = os.getenv("FRONTEND_DIR", str(Path(__file__).parent / "dashboard/frontend"))
