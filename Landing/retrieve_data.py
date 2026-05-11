import threading
import os
import sys
from loguru import logger
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.kafka_utils import KafkaBroker
from utils.api_utils import WeatherAPI
from config import COOR_LIST, KAFKA_TOPICS, now_vn

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
BUCKET_NAME       = os.getenv("GCS_BUCKET_NAME")

if not BOOTSTRAP_SERVERS or not BUCKET_NAME:
    logger.critical("Thiếu KAFKA_BOOTSTRAP_SERVERS hoặc GCS_BUCKET_NAME!")
    sys.exit(1)


def retrieve_data():
    """
    Chạy 1 lần rồi thoát, Airflow lo schedule.
    Flow: fetch weather → Kafka (per road) → gom batch → 1 file GCS.
    File: landing/weather/{YYYY-MM-DD_HHMM}.json
    """
    broker   = KafkaBroker(BOOTSTRAP_SERVERS, BUCKET_NAME)
    api      = WeatherAPI()
    gcs_path = f"landing/weather/{now_vn().strftime('%Y-%m-%d_%H%M')}.json"

    # Consumer chạy nền: gom hết message → save 1 file
    consumer_thread = threading.Thread(
        target=broker.consume_batch_to_file,
        kwargs={
            "topic":               KAFKA_TOPICS["weather"],
            "gcs_path":            gcs_path,
            "consumer_timeout_ms": 10_000,
        },
        daemon=True,
    )
    consumer_thread.start()

    # Producer: fetch từng road → đẩy Kafka
    for road_name, coords in COOR_LIST:
        lat, lon = coords.split(",")
        data = api.get_weather(lat.strip(), lon.strip())
        if not data:
            logger.warning(f"Không lấy được weather cho {road_name}")
            continue
        broker.send_to_topic(
            topic=KAFKA_TOPICS["weather"],
            key=road_name,
            data=data,
            data_format="json",
        )
    broker.flush()

    # Đợi consumer gom xong rồi thoát
    consumer_thread.join(timeout=30)
    broker.close()


if __name__ == "__main__":
    retrieve_data()
