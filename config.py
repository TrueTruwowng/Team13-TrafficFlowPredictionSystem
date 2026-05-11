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
    "spark.hadoop.google.cloud.auth.service.account.enable": "true",
}

# ── Road Segments (metadata tĩnh) ─────────────────────────────────────────────
ROAD_SEGMENTS = [
    {"id": "SEG001", "name": "HQV - PTT", "lat": 21.04610761029762,  "long": 105.786337500623},
    {"id": "SEG002", "name": "HQV - NPS", "lat": 21.04605840393917,  "long": 105.7902484330184},
    {"id": "SEG003", "name": "HQV - NT",  "lat": 21.046144780811982, "long": 105.79441294799092},
    {"id": "SEG004", "name": "HQV - NVH", "lat": 21.046111488932937, "long": 105.7973369997308},
    {"id": "SEG005", "name": "TDN - NPS", "lat": 21.040249360573526, "long": 105.79042205748846},
    {"id": "SEG006", "name": "NVH - NKT", "lat": 21.039019705238665, "long": 105.79764595466595},
    {"id": "SEG007", "name": "NKT - DQH", "lat": 21.03690218878549,  "long": 105.80183528538696},
    {"id": "SEG008", "name": "CG - TQK",  "lat": 21.035756749196693, "long": 105.7916665327835},
    {"id": "SEG009", "name": "CG - TDN",  "lat": 21.035091818986388, "long": 105.79362570931829},
]

# Dùng cho Open-Meteo API calls (không cần API key)
COOR_LIST = [
    (seg["name"], f"{seg['lat']},{seg['long']}") for seg in ROAD_SEGMENTS
]

# Open-Meteo endpoint (free, no key required)
OPENMETEO_URL = "https://api.open-meteo.com/v1/forecast"

# Các weather variable cần lấy từ Open-Meteo
OPENMETEO_PARAMS = {
    "hourly": [
        "temperature_2m",
        "precipitation",
        "weathercode",
        "windspeed_10m",
        "relativehumidity_2m",
    ],
    "current_weather": True,
    "timezone": "Asia/Bangkok",
}

# ── Business Logic ─────────────────────────────────────────────────────────────
CONGESTION_SPEED_THRESHOLD_KMH = 20   # dưới ngưỡng này → tắc
PRODUCER_INTERVAL_SECONDS      = 60   # chu kỳ gửi lên Kafka
