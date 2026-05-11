import json
import time
import os
import sys
from datetime import date, timedelta
from loguru import logger
from dotenv import load_dotenv
from google.cloud import storage

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.api_utils import WeatherAPI
from config import ROAD_SEGMENTS, GCS_BUCKET

load_dotenv()

START_DATE       = date(2026, 1, 5)
INTERVAL_SECONDS = 0.1 * 60           # 30 phút
ARCHIVE_LAG_DAYS = 1                 # Open-Meteo archive chậm ~5 ngày
GCS_PREFIX       = "landing/weather"
CHECKPOINT_BLOB  = f"{GCS_PREFIX}/_checkpoint.json"

# Cấu trúc GCS:
#   landing/weather/weather_historical/
#     2026-01-05.json    ← tất cả đường, 24h/đường (216 records)
#     2026-01-06.json
#     ...
#     _checkpoint.json


def get_bucket():
    return storage.Client().bucket(GCS_BUCKET)


def read_checkpoint(bucket) -> date:
    blob = bucket.blob(CHECKPOINT_BLOB)
    if blob.exists():
        return date.fromisoformat(json.loads(blob.download_as_text())["last_fetched_date"])
    return START_DATE - timedelta(days=1)


def write_checkpoint(bucket, last_date: date):
    bucket.blob(CHECKPOINT_BLOB).upload_from_string(
        json.dumps({"last_fetched_date": last_date.isoformat()}),
        content_type="application/json",
    )


def fetch_day(api: WeatherAPI, bucket, target_date: date):
    """
    Fetch weather cho tất cả road segments trong 1 ngày → 1 file JSON duy nhất.
    Cấu trúc: landing/weather/weather_historical/{date}.json
    """
    blob_path = f"{GCS_PREFIX}/{target_date.isoformat()}.json"
    blob      = bucket.blob(blob_path)

    if blob.exists():
        logger.debug(f"Đã có: {blob_path}")
        return

    all_records = []

    for seg in ROAD_SEGMENTS:
        records = api.get_historical_weather(
            lat        = seg["lat"],
            lon        = seg["long"],
            start_date = target_date,
            end_date   = target_date,
        )
        if not records:
            logger.warning(f"Không có dữ liệu cho {seg['name']} ngày {target_date}")
            continue

        for r in records:
            r["road_name"] = seg["name"]
            r["road_id"]   = seg["id"]
        all_records.extend(records)

    if not all_records:
        return

    blob.upload_from_string(
        json.dumps(all_records, ensure_ascii=False),
        content_type="application/json",
    )
    logger.info(f"Saved {len(all_records)} records → {blob_path}")


def run():
    api    = WeatherAPI()
    bucket = get_bucket()

    logger.info(f"=== Historical Weather Fetcher started (from {START_DATE}) ===")

    while True:
        last_fetched = read_checkpoint(bucket)
        end_date     = date.today() - timedelta(days=ARCHIVE_LAG_DAYS)
        from_date    = last_fetched + timedelta(days=1)

        if from_date > end_date:
            logger.info(f"Đã cập nhật đến {last_fetched}, chờ {INTERVAL_SECONDS // 60} phút...")
        else:
            logger.info(f"Fetching {from_date} → {end_date}...")
            current = from_date
            while current <= end_date:
                fetch_day(api, bucket, current)
                write_checkpoint(bucket, current)
                current += timedelta(days=1)

        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    run()
