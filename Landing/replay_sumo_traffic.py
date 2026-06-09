import json
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
from config import KAFKA_TOPICS, GCS_PATHS, GCS_BUCKET

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
BUCKET_NAME       = os.getenv("GCS_BUCKET_NAME")
TOPIC             = KAFKA_TOPICS["traffic_simulate"]

# Blob path within the bucket (strip gs://<bucket>/ prefix)
SIMULATION_PREFIX = GCS_PATHS["simulation_traffic"].removeprefix(f"gs://{GCS_BUCKET}/")
LANDING_PREFIX    = "landing/traffic"

# Must match the interval used when generating the SUMO simulation files.
INTERVAL_MINUTES  = 3

if not BOOTSTRAP_SERVERS or not BUCKET_NAME:
    logger.critical("Missing KAFKA_BOOTSTRAP_SERVERS or GCS_BUCKET_NAME")
    sys.exit(1)


def _send_interval(broker: KafkaBroker, date_str: str, hhmm: str):
    sim_path     = f"{SIMULATION_PREFIX}/{date_str}/{date_str}_{hhmm}.json"
    landing_path = f"{LANDING_PREFIX}/{date_str}/{date_str}_{hhmm}.json"

    blob = broker.bucket.blob(sim_path)
    if not blob.exists():
        logger.warning(f"Not found: {sim_path}")
        return

    records = json.loads(blob.download_as_text())
    logger.info(f"[{date_str} {hhmm}] {len(records)} records → Kafka")

    ready_event     = threading.Event()
    consumer_thread = threading.Thread(
        target=broker.consume_batch_to_file,
        kwargs={
            "topic":               TOPIC,
            "gcs_path":            landing_path,
            "group_id":            "sumo_replay_group",
            "consumer_timeout_ms": 15_000,
            "key_as_road_name":    False,
            "ready_event":         ready_event,
        },
        daemon=True,
    )
    consumer_thread.start()
    ready_event.wait(timeout=10)

    for record in records:
        broker.send_to_topic(topic=TOPIC, key=record.get("id", ""), data=record, data_format="json")
    broker.flush()

    consumer_thread.join(timeout=60)
    logger.info(f"Done → {landing_path}")


def replay_day(target_date: str = None):
    """Start from the current interval and replay in real time until end of day."""
    if target_date is None:
        target_date = date_type.today().isoformat()

    now           = datetime.now()
    today_00      = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elapsed_min   = now.hour * 60 + now.minute
    first_end_min = ((elapsed_min // INTERVAL_MINUTES) + 1) * INTERVAL_MINUTES

    if first_end_min > 1440:
        logger.info("Past end of day, no intervals remaining.")
        return

    schedule = [
        (today_00 + timedelta(minutes=m), f"{(m // 60):02d}{(m % 60):02d}")
        for m in range(first_end_min, 1441, INTERVAL_MINUTES)
    ]

    broker = KafkaBroker(BOOTSTRAP_SERVERS, BUCKET_NAME)
    logger.info(
        f"Replay {target_date} starting from {schedule[0][1]}: "
        f"{len(schedule)} intervals remaining"
    )

    for send_time, hhmm in schedule:
        # Sleep to exact wall-clock mark to prevent drift accumulation.
        wait_sec = (send_time - datetime.now()).total_seconds()
        if wait_sec > 0:
            logger.debug(f"Waiting {wait_sec:.0f}s until {send_time.strftime('%H:%M')}…")
            time.sleep(wait_sec)

        _send_interval(broker, target_date, hhmm)

    broker.close()
    logger.info(f"Replay {target_date} complete.")


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else None
    replay_day(target)
