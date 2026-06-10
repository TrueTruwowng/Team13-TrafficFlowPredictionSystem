import csv
import os
import sys
import threading
import time
from datetime import date as date_type, datetime, timedelta

os.environ.setdefault("TZ", "Asia/Ho_Chi_Minh")
time.tzset()

from loguru import logger
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.kafka_utils import KafkaBroker
from utils.api_utils import WeatherAPI
from config import KAFKA_TOPICS

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
BUCKET_NAME       = os.getenv("GCS_BUCKET_NAME")
LOOKUP_PATH       = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                  "data", "road_lookup_table.csv")
LANDING_PREFIX    = "landing/weather"
TOPIC             = KAFKA_TOPICS["weather"]

WRITE_INTERVAL_MINUTES = 3   # how often weather is pushed to Kafka
FETCH_INTERVAL_MINUTES = 60  # how often a live API call is made (cached otherwise)
INTERVAL_MINUTES       = WRITE_INTERVAL_MINUTES

if not BOOTSTRAP_SERVERS or not BUCKET_NAME:
    logger.critical("Missing KAFKA_BOOTSTRAP_SERVERS or GCS_BUCKET_NAME")
    sys.exit(1)


def _load_roads() -> list[dict]:
    seen  = set()
    roads = []
    with open(LOOKUP_PATH, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["name"] in seen:
                continue
            seen.add(r["name"])
            roads.append({"id": r["way_id"], "name": r["name"],
                           "lat": float(r["lat"]), "lon": float(r["lon"])})
    return roads


def _fetch_and_send(broker: KafkaBroker, api: WeatherAPI, roads: list[dict],
                    date_str: str, begin: str, end: str, hhmm: str,
                    weather_cache: dict | None = None) -> dict:
    """Fetch weather (or reuse cache) → Kafka → consumer saves to GCS.
    Returns new cache dict on live fetch, empty dict when cache was used."""
    landing_path = f"{LANDING_PREFIX}/{date_str}/{date_str}_{hhmm}.json"

    ready_event     = threading.Event()
    consumer_thread = threading.Thread(
        target=broker.consume_batch_to_file,
        kwargs={
            "topic":               TOPIC,
            "gcs_path":            landing_path,
            "group_id":            "weather_landing_group",
            "consumer_timeout_ms": 20_000,
            "key_as_road_name":    True,
            "ready_event":         ready_event,
        },
        daemon=True,
    )
    consumer_thread.start()
    ready_event.wait(timeout=10)

    new_cache: dict = {}
    sent = 0
    for road in roads:
        if weather_cache and road["name"] in weather_cache:
            data = dict(weather_cache[road["name"]])
            data["ingestion_time"] = datetime.now().isoformat()
        else:
            data = api.get_weather(road["lat"], road["lon"])
            if not data:
                logger.warning(f"Could not fetch weather: {road['name']}")
                continue
            new_cache[road["name"]] = dict(data)

        data["road_id"] = road["id"]
        data["date"]    = date_str
        data["begin"]   = begin
        data["end"]     = end
        broker.send_to_topic(topic=TOPIC, key=road["name"], data=data, data_format="json")
        sent += 1

    broker.flush()
    consumer_thread.join(timeout=60)
    logger.info(f"[{date_str} {hhmm}] {sent}/{len(roads)} roads → {landing_path}")
    return new_cache


def _refresh_cache(api: WeatherAPI, roads: list[dict]) -> dict:
    """Live-fetch all roads into a new cache dict. Send nothing.
    Roads that fail are omitted so the caller keeps their old cache entry."""
    fresh = {}
    for road in roads:
        data = api.get_weather(road["lat"], road["lon"])
        if data:
            fresh[road["name"]] = data
        else:
            logger.warning(f"Could not fetch weather: {road['name']}")
    logger.info(f"Weather cache refreshed: {len(fresh)}/{len(roads)} roads")
    return fresh


def _min_to_hhmm(total_min: int) -> str:
    return f"{total_min // 60:02d}:{total_min % 60:02d}"


def run(target_date: str = None):
    """Start from the current interval and continue until end of day."""
    if target_date is None:
        target_date = date_type.today().isoformat()

    roads  = _load_roads()
    broker = KafkaBroker(BOOTSTRAP_SERVERS, BUCKET_NAME)
    api    = WeatherAPI()

    now           = datetime.now()
    today_00      = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elapsed_min   = now.hour * 60 + now.minute
    first_end_min = ((elapsed_min // INTERVAL_MINUTES) + 1) * INTERVAL_MINUTES

    if first_end_min > 1440:
        logger.info("Past end of day.")
        broker.close()
        return

    schedule = [
        (today_00 + timedelta(minutes=m), m)
        for m in range(first_end_min, 1441, INTERVAL_MINUTES)
    ]

    logger.info(
        f"Weather retrieve {target_date} starting from "
        f"{_min_to_hhmm(first_end_min)}: {len(schedule)} intervals remaining"
    )

    _cached_weather: dict = {}
    _last_fetch_min: int  = -1

    for send_time, end_min in schedule:
        wait_sec = (send_time - datetime.now()).total_seconds()
        if wait_sec > 0:
            logger.debug(f"Waiting {wait_sec:.0f}s until {send_time.strftime('%H:%M')}…")
            time.sleep(wait_sec)

        begin_min  = end_min - WRITE_INTERVAL_MINUTES
        begin_hhmm = _min_to_hhmm(begin_min)
        end_hhmm   = _min_to_hhmm(end_min)
        hhmm       = f"{(end_min // 60):02d}{(end_min % 60):02d}"

        result = _fetch_and_send(
            broker, api, roads, target_date, begin_hhmm, end_hhmm, hhmm,
            weather_cache=_cached_weather if _cached_weather else None,
        )
        if result:
            _cached_weather = {**_cached_weather, **result}
            _last_fetch_min = end_min

        if (end_min + WRITE_INTERVAL_MINUTES) % FETCH_INTERVAL_MINUTES == 0:
            fresh = _refresh_cache(api, roads)
            if fresh:
                _cached_weather = {**_cached_weather, **fresh}
                _last_fetch_min = end_min

    broker.close()
    logger.info(f"Weather retrieve {target_date} complete.")


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else None
    run(target)
