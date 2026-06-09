import json
import os
import sys
import time

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

    # Topic management

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
                logger.warning(f"Kafka not reachable (attempt {attempt}/{retries}) — retry in {delay}s")
                if attempt < retries:
                    time.sleep(delay)
            except Exception as e:
                logger.error(f"_ensure_topics unexpected error: {e}")
                if attempt < retries:
                    time.sleep(delay)

        raise RuntimeError(f"Kafka did not respond after {retries} attempts")

    def _create_topics(self, admin, kafka_topics, num_partitions, replication_factor):
        try:
            admin.create_topics([
                NewTopic(name=t, num_partitions=num_partitions,
                         replication_factor=replication_factor)
                for t in kafka_topics.values()
            ])
            logger.info(f"Topics created: {list(kafka_topics.values())}")

        except TopicAlreadyExistsError:
            logger.debug("Topics already exist.")

        except InvalidReplicationFactorError:
            # Cluster has fewer brokers than replication_factor; fall back to rf=1.
            logger.warning(
                f"replication_factor={replication_factor} exceeds broker count — retrying with rf=1"
            )
            try:
                admin.create_topics([
                    NewTopic(name=t, num_partitions=num_partitions, replication_factor=1)
                    for t in kafka_topics.values()
                ])
            except TopicAlreadyExistsError:
                pass

    # GCS upload with retry

    def _upload_to_gcs(self, blob_path: str, content: str,
                       content_type: str = "application/json"):
        for attempt in range(1, GCS_UPLOAD_RETRIES + 1):
            try:
                self.bucket.blob(blob_path).upload_from_string(content, content_type=content_type)
                return
            except GoogleAPIError as e:
                logger.warning(f"GCS upload failed (attempt {attempt}/{GCS_UPLOAD_RETRIES}): {e}")
                if attempt < GCS_UPLOAD_RETRIES:
                    time.sleep(GCS_RETRY_DELAY_SEC * attempt)

        raise RuntimeError(f"GCS upload failed after {GCS_UPLOAD_RETRIES} attempts: {blob_path}")

    # Serialization

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

    # Producer

    def send_to_topic(self, topic, key, data, data_format="json"):
        try:
            value  = self._serialize(data, data_format)
            future = self.producer.send(topic, key=key, value=value)
            future.add_errback(
                lambda exc: logger.error(f"[{topic}] Send failed key={key}: {exc}")
            )
            logger.debug(f"Queued → {topic} key={key}")
        except KafkaError as e:
            logger.error(f"send_to_topic KafkaError [{topic}]: {e}")
            raise
        except Exception as e:
            logger.error(f"send_to_topic error [{topic}]: {e}")
            raise

    # Consumer: batch → single GCS file

    def consume_batch_to_file(self, topic, gcs_path, group_id="landing_group",
                              consumer_timeout_ms=10_000, key_as_road_name=True,
                              offset_reset="latest", ready_event=None):
        """Consume all messages in one batch and write them as a single JSON file to GCS.

        Uses assign()+seek_to_end() instead of subscribe() to avoid rebalance race:
        the group coordinator rebalance can be slower than the first producer message.
        Signals ready_event after seek so the caller can start producing.
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
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                consumer.poll(timeout_ms=3_000)
                partitions = consumer.partitions_for_topic(topic) or set()

            tps = [TopicPartition(topic, p) for p in partitions]
            consumer.assign(tps)
            consumer.seek_to_end(*tps)

            if ready_event:
                ready_event.set()

            records = []
            for message in consumer:
                data = message.value
                if key_as_road_name:
                    data["road_name"] = message.key
                records.append(data)

            if not records:
                logger.warning(f"No messages from topic {topic}")
                return

            # Upload before committing offset to avoid data loss on upload failure.
            self._upload_to_gcs(gcs_path, json.dumps(records, ensure_ascii=False))
            consumer.commit()
            logger.info(f"Saved {len(records)} records → {gcs_path}")

        except RuntimeError:
            logger.error(f"[{topic}] GCS upload failed — offset NOT committed, will re-read")
        except Exception as e:
            logger.opt(exception=True).error(f"consume_batch_to_file error [{topic}]: {e}")
        finally:
            consumer.close()

    # Helpers

    def flush(self):
        if self.producer:
            self.producer.flush()
            logger.debug("Producer flushed.")

    def close(self):
        self.producer.flush()
        self.producer.close()
        logger.info("Kafka connections closed.")
