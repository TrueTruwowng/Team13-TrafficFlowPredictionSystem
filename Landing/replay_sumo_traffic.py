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
from config import KAFKA_TOPICS

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
BUCKET_NAME       = os.getenv("GCS_BUCKET_NAME")
SIMULATION_PREFIX = "simulation/traffic"
LANDING_PREFIX    = "landing/traffic"
TOPIC             = KAFKA_TOPICS["traffic_simulate"]
INTERVAL_MINUTES  = 3  # phai khop voi interval trong file SUMO XML

if not BOOTSTRAP_SERVERS or not BUCKET_NAME:
    logger.critical("Thieu KAFKA_BOOTSTRAP_SERVERS hoac GCS_BUCKET_NAME!")
    sys.exit(1)


def _send_interval(broker: KafkaBroker, date_str: str, hhmm: str):
    sim_path     = f"{SIMULATION_PREFIX}/{date_str}/{date_str}_{hhmm}.json"
    landing_path = f"{LANDING_PREFIX}/{date_str}/{date_str}_{hhmm}.json"

    blob = broker.bucket.blob(sim_path)
    if not blob.exists():
        logger.warning(f"Khong tim thay: {sim_path}")
        return

    records = json.loads(blob.download_as_text())
    logger.info(f"[{date_str} {hhmm}] {len(records)} records -> Kafka")

    ready_event = threading.Event()
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
    logger.info(f"Done -> {landing_path}")


def replay_day(target_date: str = None):
    """
    Bat dau tu interval hien tai, push dung vao moc realtime, tiep tuc het ngay.
    Vi du: start luc 11:59 -> cho den 12:00 -> push interval 11:57-12:00 -> cho den 12:03 -> ...
    Dung: python replay_sumo_traffic.py [YYYY-MM-DD]
    """
    if target_date is None:
        target_date = date_type.today().isoformat()

    now      = datetime.now()
    today_00 = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # Tinh moc interval ke tiep tinh tu hien tai
    elapsed_min   = now.hour * 60 + now.minute
    first_end_min = ((elapsed_min // INTERVAL_MINUTES) + 1) * INTERVAL_MINUTES

    if first_end_min > 1440:
        logger.info("Da qua cuoi ngay, khong con interval nao.")
        return

    # Danh sach (thoi_diem_push, hhmm) cho phan con lai cua ngay
    schedule = [
        (today_00 + timedelta(minutes=m), f"{(m // 60):02d}{(m % 60):02d}")
        for m in range(first_end_min, 1441, INTERVAL_MINUTES)
    ]

    broker = KafkaBroker(BOOTSTRAP_SERVERS, BUCKET_NAME)
    logger.info(
        f"Replay {target_date} bat dau tu {schedule[0][1]}: "
        f"{len(schedule)} intervals con lai"
    )

    for send_time, hhmm in schedule:
        # Ngu chinh xac den moc wall-clock, tranh drift tich luy
        wait_sec = (send_time - datetime.now()).total_seconds()
        if wait_sec > 0:
            logger.debug(f"Cho {wait_sec:.0f}s den {send_time.strftime('%H:%M')}...")
            time.sleep(wait_sec)

        _send_interval(broker, target_date, hhmm)

    broker.close()
    logger.info(f"Replay {target_date} hoan tat.")


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else None
    replay_day(target)
