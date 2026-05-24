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
LOOKUP_PATH       = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "road_lookup_table.csv")
LANDING_PREFIX    = "landing/weather"
TOPIC             = KAFKA_TOPICS["weather"]
INTERVAL_MINUTES = 3

if not BOOTSTRAP_SERVERS or not BUCKET_NAME:
    logger.critical("Thieu KAFKA_BOOTSTRAP_SERVERS hoac GCS_BUCKET_NAME!")
    sys.exit(1)


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


def _fetch_and_send(broker: KafkaBroker, api: WeatherAPI, roads: list[dict],
                    date_str: str, begin: str, end: str, hhmm: str):
    """Fetch weather tat ca duong -> Kafka -> consumer luu GCS."""
    landing_path = f"{LANDING_PREFIX}/{date_str}/{date_str}_{hhmm}.json"

    ready_event = threading.Event()
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

    sent = 0
    for road in roads:
        data = api.get_weather(road["lat"], road["lon"])
        if not data:
            logger.warning(f"Khong lay duoc weather: {road['name']}")
            continue
        data["road_id"] = road["id"]
        data["date"]    = date_str
        data["begin"]   = begin
        data["end"]     = end
        broker.send_to_topic(topic=TOPIC, key=road["name"], data=data, data_format="json")
        sent += 1

    broker.flush()
    consumer_thread.join(timeout=60)
    logger.info(f"[{date_str} {hhmm}] {sent}/{len(roads)} roads -> {landing_path}")


def _min_to_hhmm(total_min: int) -> str:
    return f"{total_min // 60:02d}:{total_min % 60:02d}"


def run(target_date: str = None):
    """
    Bat dau tu interval hien tai, fetch weather dung moc realtime, tiep tuc het ngay.
    Vi du: start luc 11:59 -> cho den 12:00 -> fetch + push weather -> cho den 12:03 -> ...
    Dung: python retrieve_data.py [YYYY-MM-DD]
    """
    if target_date is None:
        target_date = date_type.today().isoformat()

    roads  = _load_roads()
    broker = KafkaBroker(BOOTSTRAP_SERVERS, BUCKET_NAME)
    api    = WeatherAPI()

    now      = datetime.now()
    today_00 = now.replace(hour=0, minute=0, second=0, microsecond=0)

    elapsed_min   = now.hour * 60 + now.minute
    first_end_min = ((elapsed_min // INTERVAL_MINUTES) + 1) * INTERVAL_MINUTES

    if first_end_min > 1440:
        logger.info("Da qua cuoi ngay.")
        broker.close()
        return

    schedule = [
        (today_00 + timedelta(minutes=m), m)
        for m in range(first_end_min, 1441, INTERVAL_MINUTES)
    ]

    logger.info(
        f"Weather retrieve {target_date} bat dau tu "
        f"{_min_to_hhmm(first_end_min)}: {len(schedule)} intervals con lai"
    )

    for send_time, end_min in schedule:
        wait_sec = (send_time - datetime.now()).total_seconds()
        if wait_sec > 0:
            logger.debug(f"Cho {wait_sec:.0f}s den {send_time.strftime('%H:%M')}...")
            time.sleep(wait_sec)

        begin_min = end_min - INTERVAL_MINUTES
        begin_hhmm = _min_to_hhmm(begin_min)
        end_hhmm   = _min_to_hhmm(end_min)
        hhmm       = f"{(end_min // 60):02d}{(end_min % 60):02d}"

        _fetch_and_send(broker, api, roads, target_date, begin_hhmm, end_hhmm, hhmm)

    broker.close()
    logger.info(f"Weather retrieve {target_date} hoan tat.")


if __name__ == "__main__":
    import sys
    target = sys.argv[1] if len(sys.argv) > 1 else None
    run(target)
