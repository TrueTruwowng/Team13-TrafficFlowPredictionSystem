import json
import time
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import now_vn
from loguru import logger
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import (
    TopicAlreadyExistsError, KafkaError,
    NoBrokersAvailable, InvalidReplicationFactorError,
)
from google.cloud import storage
from google.api_core.exceptions import GoogleAPIError

logger.remove()
logger.add(sys.stderr, format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}", level="INFO")

GCS_UPLOAD_RETRIES  = 3
GCS_RETRY_DELAY_SEC = 5


class KafkaBroker:
    def __init__(self, bootstrap_servers, bucket_name="big-data-storage13"):
        self.bootstrap_servers = bootstrap_servers
        self.bucket_name       = bucket_name

        self._ensure_topics(bootstrap_servers)

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=5,
            retry_backoff_ms=300,
            request_timeout_ms=30_000,
        )

        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(bucket_name)
        logger.info(f"Connected to Kafka ({bootstrap_servers}) and GCS ({bucket_name})")

    # ── Topic management ───────────────────────────────────────────────────────

    def _ensure_topics(self, bootstrap_servers,
                       num_partitions=3, replication_factor=3,
                       retries=5, delay=10):
        from config import KAFKA_TOPICS

        for attempt in range(1, retries + 1):
            try:
                admin = KafkaAdminClient(
                    bootstrap_servers=bootstrap_servers,
                    request_timeout_ms=10_000,
                )
                self._create_topics(admin, KAFKA_TOPICS, num_partitions, replication_factor)
                admin.close()
                return

            except NoBrokersAvailable:
                logger.warning(
                    f"Kafka không kết nối được (lần {attempt}/{retries}) — "
                    f"thử lại sau {delay}s"
                )
                if attempt < retries:
                    time.sleep(delay)
            except Exception as e:
                logger.error(f"_ensure_topics lỗi không mong muốn: {e}")
                if attempt < retries:
                    time.sleep(delay)

        raise RuntimeError(f"Kafka không phản hồi sau {retries} lần thử")

    def _create_topics(self, admin, kafka_topics, num_partitions, replication_factor):
        try:
            admin.create_topics([
                NewTopic(name=t, num_partitions=num_partitions,
                         replication_factor=replication_factor)
                for t in kafka_topics.values()
            ])
            logger.info(f"Topics đã tạo: {list(kafka_topics.values())}")

        except TopicAlreadyExistsError:
            logger.debug("Topics đã tồn tại.")

        except InvalidReplicationFactorError:
            # Fallback khi cluster có ít broker hơn replication_factor
            logger.warning(
                f"replication_factor={replication_factor} vượt quá số broker — "
                "thử lại với rf=1"
            )
            try:
                admin.create_topics([
                    NewTopic(name=t, num_partitions=num_partitions, replication_factor=1)
                    for t in kafka_topics.values()
                ])
            except TopicAlreadyExistsError:
                pass

    # ── GCS upload với retry ───────────────────────────────────────────────────

    def _upload_to_gcs(self, blob_path: str, content: str,
                       content_type: str = "application/json"):
        for attempt in range(1, GCS_UPLOAD_RETRIES + 1):
            try:
                self.bucket.blob(blob_path).upload_from_string(
                    content, content_type=content_type
                )
                return
            except GoogleAPIError as e:
                logger.warning(
                    f"GCS upload thất bại (lần {attempt}/{GCS_UPLOAD_RETRIES}): {e}"
                )
                if attempt < GCS_UPLOAD_RETRIES:
                    time.sleep(GCS_RETRY_DELAY_SEC * attempt)

        raise RuntimeError(
            f"GCS upload thất bại sau {GCS_UPLOAD_RETRIES} lần: {blob_path}"
        )

    # ── Serialization ──────────────────────────────────────────────────────────

    def _serialize(self, data, data_format):
        if data_format.lower() == "json":
            return json.dumps(data, ensure_ascii=False).encode("utf-8")
        if data_format.lower() == "xml":
            return data.encode("utf-8") if isinstance(data, str) else data
        return str(data).encode("utf-8")

    def _deserialize(self, data, data_format):
        if data_format.lower() == "json":
            return json.loads(data.decode("utf-8"))
        if data_format.lower() == "xml":
            return data.decode("utf-8")
        return data

    # ── Producer ───────────────────────────────────────────────────────────────

    def send_to_topic(self, topic, key, data, data_format="json"):
        try:
            value  = self._serialize(data, data_format)
            future = self.producer.send(topic, key=key, value=value)
            # Errback chạy trong background thread của producer khi gửi thất bại
            future.add_errback(
                lambda exc: logger.error(
                    f"[{topic}] Gửi thất bại key={key}: {exc}"
                )
            )
            logger.debug(f"Queued → {topic} key={key}")
        except KafkaError as e:
            logger.error(f"send_to_topic KafkaError [{topic}]: {e}")
            raise
        except Exception as e:
            logger.error(f"send_to_topic lỗi [{topic}]: {e}")
            raise

    # ── Consumer: batch → 1 file GCS ──────────────────────────────────────────

    def consume_batch_to_file(self, topic, gcs_path, group_id="landing_group",
                              consumer_timeout_ms=10_000, key_as_road_name=True,
                              offset_reset="latest", ready_event=None):
        """
        Gom toàn bộ message trong 1 batch → lưu thành 1 file JSON duy nhất lên GCS.

        Dùng assign()+seek_to_end() thay vì subscribe() để tránh race condition:
        group coordinator rebalance có thể chậm hơn producer gửi message đầu tiên.
        Sau khi seek xong, set ready_event để báo caller producer có thể bắt đầu gửi.
        """
        from kafka import TopicPartition

        consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,
            consumer_timeout_ms=consumer_timeout_ms,
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        try:
            # Lấy partition list (poll một lần để fetch metadata nếu chưa có)
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                consumer.poll(timeout_ms=3_000)
                partitions = consumer.partitions_for_topic(topic) or set()

            tps = [TopicPartition(topic, p) for p in partitions]
            consumer.assign(tps)
            consumer.seek_to_end(*tps)  # bắt đầu đọc từ cuối hiện tại

            if ready_event:
                ready_event.set()  # báo producer: consumer đã sẵn sàng

            records = []
            for message in consumer:
                data = message.value
                if key_as_road_name:
                    data["road_name"] = message.key
                records.append(data)

            if not records:
                logger.warning(f"Không có message từ topic {topic}")
                return

            # Upload trước, commit offset sau — đảm bảo không mất data
            self._upload_to_gcs(gcs_path, json.dumps(records, ensure_ascii=False))
            consumer.commit()
            logger.info(f"Saved {len(records)} records → {gcs_path}")

        except RuntimeError:
            logger.error(f"[{topic}] GCS upload thất bại — offset KHÔNG commit, sẽ đọc lại")
        except Exception as e:
            logger.opt(exception=True).error(f"consume_batch_to_file lỗi [{topic}]: {e}")
        finally:
            consumer.close()

    # ── Consumer: stream liên tục → nhiều file GCS ────────────────────────────

    def consume_to_storage(self, topic, sub_folder, data_format="json",
                           group_id="traffic_group", consumer_timeout_ms=None,
                           datetime_path=False):
        base_folder = "landing"
        full_prefix = f"{base_folder}/{sub_folder}"
        self._ensure_prefix_exists(full_prefix)
        logger.info(f"Listening to {topic}. Saving to {full_prefix}/")

        consumer_kwargs = dict(
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
        )
        if consumer_timeout_ms is not None:
            consumer_kwargs["consumer_timeout_ms"] = consumer_timeout_ms

        consumer = KafkaConsumer(topic, **consumer_kwargs)
        try:
            for message in consumer:
                key     = message.key if message.key else "unknown"
                raw     = message.value
                try:
                    parsed    = self._deserialize(raw, data_format)
                    extension = "xml" if data_format == "xml" else "json"
                except Exception as e:
                    logger.error(f"Parse error: {e}")
                    consumer.commit()
                    continue

                now = now_vn()
                if datetime_path:
                    dest = (f"{full_prefix}/{now.strftime('%Y-%m-%d')}/"
                            f"{now.strftime('%H%M')}/{key}.{extension}")
                else:
                    dest = (f"{full_prefix}/{topic}/{key}/"
                            f"{now.strftime('%Y%m%d_%H%M%S_%f')}.{extension}")

                content      = parsed if data_format == "xml" else json.dumps(parsed, ensure_ascii=False)
                content_type = "application/xml" if data_format == "xml" else "application/json"
                try:
                    self._upload_to_gcs(dest, content, content_type)
                    consumer.commit()
                    logger.info(f"Stored → {dest}")
                except RuntimeError:
                    logger.error(f"GCS upload thất bại sau retry — offset KHÔNG commit: {dest}")

        except Exception:
            logger.opt(exception=True).error(f"consume_to_storage lỗi [{topic}]")
        finally:
            consumer.close()
            logger.warning(f"Consumer stopped: {topic}")

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _ensure_prefix_exists(self, prefix):
        blobs = list(self.bucket.list_blobs(prefix=prefix, max_results=1))
        if not blobs:
            self.bucket.blob(f"{prefix}/").upload_from_string("")

    def flush(self):
        if self.producer:
            self.producer.flush()
            logger.debug("Producer flushed.")

    def close(self):
        self.producer.flush()
        self.producer.close()
        logger.info("Kafka connections closed.")
