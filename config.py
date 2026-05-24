import os
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

def now_vn() -> datetime:
    """Trả về thời gian hiện tại theo giờ Việt Nam (UTC+7)."""
    return datetime.now(tz=VN_TZ)

# ── Kafka ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

KAFKA_TOPICS = {
    "traffic_simulate": "traffic_simulate",   # từ SUMO
    "weather":          "weather_raw",         # từ Open-Meteo API
}

# ── GCS ───────────────────────────────────────────────────────────────────────
GCS_BUCKET = os.getenv("GCS_BUCKET_NAME", "big-data-storage13")

GCS_PATHS = {
    # Landing (JSON thô từ Kafka consumer)
    "landing_traffic":  f"gs://{GCS_BUCKET}/landing/traffic/traffic_simulate",
    "landing_weather":  f"gs://{GCS_BUCKET}/landing/weather/weather_raw",

    # Bronze (Delta Lake)
    "bronze_traffic":   f"gs://{GCS_BUCKET}/bronze/traffic_data_raw",
    "bronze_weather":   f"gs://{GCS_BUCKET}/bronze/weather_raw",
    "bronze_road_info": f"gs://{GCS_BUCKET}/bronze/road_info",

    # Silver (Delta Lake)
    "silver_traffic":   f"gs://{GCS_BUCKET}/silver/traffic_data_formatted",

    # Gold (Delta Lake)
    "gold_traffic":     f"gs://{GCS_BUCKET}/gold/traffic_featured",
}

# ── Spark ─────────────────────────────────────────────────────────────────────
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
    # Memory & parallelism
    "spark.driver.memory":          "4g",
    "spark.driver.maxResultSize":   "2g",
    "spark.executor.instances":     "3",   # 1 executor / worker
    "spark.executor.cores":         "2",   # fit ca 2 worker nho (2 cores)
    "spark.executor.memory":        "5g",  # an toan cho worker 6.9GB
    "spark.sql.shuffle.partitions": "12",  # 3 executors x 2 cores x 2
}

# ── Business Logic ─────────────────────────────────────────────────────────────
CONGESTION_SPEED_THRESHOLD_KMH = 20   # dưới ngưỡng này → tắc
