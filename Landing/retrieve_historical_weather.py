import csv
import json
import os
import sys
from datetime import date, timedelta
from loguru import logger
from dotenv import load_dotenv
from google.cloud import storage

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.api_utils import WeatherAPI
from config import GCS_BUCKET

load_dotenv()

START_DATE       = date(2026, 5, 1)
ARCHIVE_LAG_DAYS = 1
GCS_PREFIX       = "landing/weather"
CHECKPOINT_BLOB  = f"{GCS_PREFIX}/_checkpoint.json"
LOOKUP_PATH      = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "road_lookup_table.csv")


def _load_roads() -> list[dict]:
    seen = set()
    roads = []
    with open(LOOKUP_PATH, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["name"] in seen:
                continue
            seen.add(r["name"])
            roads.append({"id": r["way_id"], "name": r["name"], "lat": float(r["lat"]), "lon": float(r["lon"])})
    return roads


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


def fetch_day(api: WeatherAPI, bucket, roads: list[dict], target_date: date):
    blob_path = f"{GCS_PREFIX}/{target_date.isoformat()}.json"
    blob      = bucket.blob(blob_path)

    if blob.exists():
        logger.debug(f"Already exists: {blob_path}")
        return

    all_records = []
    for road in roads:
        records = api.get_historical_weather(
            lat        = road["lat"],
            lon        = road["lon"],
            start_date = target_date,
            end_date   = target_date,
        )
        if not records:
            logger.warning(f"No data for {road['name']} on {target_date}")
            continue
        for r in records:
            r["road_id"]   = road["id"]
            r["road_name"] = road["name"]
        all_records.extend(records)

    if not all_records:
        return

    blob.upload_from_string(
        json.dumps(all_records, ensure_ascii=False),
        content_type="application/json",
    )
    logger.info(f"Saved {len(all_records)} records → {blob_path}")


def run():
    """Run once and exit — meant to be scheduled externally."""
    api    = WeatherAPI()
    bucket = get_bucket()
    roads  = _load_roads()

    last_fetched = read_checkpoint(bucket)
    end_date     = date.today() - timedelta(days=ARCHIVE_LAG_DAYS)
    from_date    = last_fetched + timedelta(days=1)

    if from_date > end_date:
        logger.info(f"Already up to date through {last_fetched}.")
        return

    logger.info(f"Fetching {from_date} → {end_date} ({len(roads)} roads)...")
    current = from_date
    while current <= end_date:
        fetch_day(api, bucket, roads, current)
        write_checkpoint(bucket, current)
        current += timedelta(days=1)

    logger.info("Done.")


if __name__ == "__main__":
    run()
