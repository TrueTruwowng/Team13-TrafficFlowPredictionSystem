import json
import datetime
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import now_vn
from loguru import logger
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from google.cloud import storage

# Configure Loguru
logger.remove()
logger.add(sys.stderr, format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}", level="INFO")

class KafkaBroker:
    def __init__(self, bootstrap_servers, bucket_name="big-data-storage13"):
        self.bootstrap_servers = bootstrap_servers
        self.bucket_name = bucket_name
        
        try:
            self._ensure_topics(bootstrap_servers)

            # Initialize Producer without a fixed value_serializer to allow multiple formats
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=5
            )
            
            self.storage_client = storage.Client()
            self.bucket = self.storage_client.bucket(bucket_name)
            
            logger.info(f"Connected to Kafka ({bootstrap_servers}) and GCS ({bucket_name})")
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            raise

    def _ensure_topics(self, bootstrap_servers, num_partitions=3, replication_factor=3):
        """Tạo topics với đúng config nếu chưa tồn tại."""
        from config import KAFKA_TOPICS
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        topics_to_create = [
            NewTopic(name=topic, num_partitions=num_partitions, replication_factor=replication_factor)
            for topic in KAFKA_TOPICS.values()
        ]
        try:
            admin.create_topics(topics_to_create)
            logger.info(f"Topics created: {list(KAFKA_TOPICS.values())} (p={num_partitions}, r={replication_factor})")
        except TopicAlreadyExistsError:
            logger.debug("Topics đã tồn tại.")
        finally:
            admin.close()

    def _ensure_prefix_exists(self, prefix):
        """Checks if the GCS prefix exists; creates a placeholder if not."""
        blobs = list(self.bucket.list_blobs(prefix=prefix, max_results=1))
        if not blobs:
            logger.info(f"Prefix '{prefix}' not found. Creating implicit directory.")
            placeholder = self.bucket.blob(f"{prefix}/")
            placeholder.upload_from_string("")
        else:
            logger.info(f"Target prefix '{prefix}' verified.")

    def _serialize(self, data, data_format):
        """Handles encoding based on the data source type."""
        if data_format.lower() == 'json':
            return json.dumps(data, ensure_ascii=False).encode('utf-8')
        elif data_format.lower() == 'xml':
            # XML is usually already a string, just encode to bytes
            return data.encode('utf-8') if isinstance(data, str) else data
        else:
            # Raw bytes or fallback
            return str(data).encode('utf-8')

    def _deserialize(self, data, data_format):
        """Handles decoding based on the expected data source type."""
        if data_format.lower() == 'json':
            return json.loads(data.decode('utf-8'))
        elif data_format.lower() == 'xml':
            # Return as string for XML processing
            return data.decode('utf-8')
        return data

    # --- PRODUCER ---
    def send_to_topic(self, topic, key, data, data_format='json'):
        """Sends data with flexible serialization."""
        try:
            serialized_value = self._serialize(data, data_format)
            self.producer.send(topic, key=key, value=serialized_value)
            logger.debug(f"Message sent to {topic} [Format: {data_format}]")
        except Exception as e:
            logger.error(f"Producer error on topic {topic}: {e}")

    # --- CONSUMER ---
    def consume_to_storage(self, topic, sub_folder, data_format='json', group_id='traffic_group', consumer_timeout_ms=None, datetime_path=False):
        """
        Consumes from Kafka and saves to GCS.
        datetime_path=False : landing/{sub_folder}/{topic}/{key}/{timestamp}.{ext}
        datetime_path=True  : landing/{sub_folder}/{YYYY-MM-DD}/{HHMM}/{key}.{ext}
        """
        base_folder = "landing"
        full_prefix = f"{base_folder}/{sub_folder}"
        self._ensure_prefix_exists(full_prefix)
        
        logger.info(f"Listening to {topic} for {data_format} data. Saving to {full_prefix}/")

        try:
            consumer_kwargs = dict(
                bootstrap_servers=self.bootstrap_servers,
                group_id=group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
            )
            if consumer_timeout_ms is not None:
                consumer_kwargs["consumer_timeout_ms"] = consumer_timeout_ms

            consumer = KafkaConsumer(topic, **consumer_kwargs)

            for message in consumer:
                key = message.key if message.key else "unknown"
                raw_data = message.value
                
                # Parse data
                try:
                    parsed_data = self._deserialize(raw_data, data_format)
                    extension = 'xml' if data_format == 'xml' else 'json'
                except Exception as parse_err:
                    logger.error(f"Parsing error: {parse_err}")
                    continue

                # Generate path
                now = now_vn()
                if datetime_path:
                    date_str = now.strftime("%Y-%m-%d")
                    time_str = now.strftime("%H%M")
                    destination_path = f"{full_prefix}/{date_str}/{time_str}/{key}.{extension}"
                else:
                    timestamp = now.strftime("%Y%m%d_%H%M%S_%f")
                    destination_path = f"{full_prefix}/{topic}/{key}/{timestamp}.{extension}"

                # Upload to GCS
                try:
                    blob = self.bucket.blob(destination_path)
                    content_type = 'application/xml' if data_format == 'xml' else 'application/json'
                    
                    content = parsed_data if data_format == 'xml' else json.dumps(parsed_data, ensure_ascii=False)
                    
                    blob.upload_from_string(data=content, content_type=content_type)
                    logger.info(f"Stored {data_format} data to {destination_path}")
                except Exception as upload_err:
                    logger.error(f"GCS upload error: {upload_err}")

        except Exception as e:
            logger.opt(exception=True).error(f"Fatal consumer error on topic {topic}")
        finally:
            logger.warning(f"Consumer stopped for topic: {topic}")

    def consume_batch_to_file(self, topic, gcs_path, group_id='landing_group', consumer_timeout_ms=10_000):
        """
        Gom toàn bộ message trong 1 batch → lưu thành 1 file JSON duy nhất lên GCS.
        Tự thoát sau consumer_timeout_ms ms không có message mới.

        Args:
            gcs_path: đường dẫn đầy đủ trên GCS, vd "landing/weather/2026-05-11_1430.json"
        """
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                consumer_timeout_ms=consumer_timeout_ms,
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            )

            records = []
            for message in consumer:
                data = message.value
                data["road_name"] = message.key
                records.append(data)
            consumer.close()

            if not records:
                logger.warning(f"Không có message nào từ topic {topic}")
                return

            blob = self.bucket.blob(gcs_path)
            blob.upload_from_string(
                json.dumps(records, ensure_ascii=False),
                content_type='application/json',
            )
            logger.info(f"Saved {len(records)} records → {gcs_path}")

        except Exception as e:
            logger.opt(exception=True).error(f"consume_batch_to_file error on topic {topic}")

    def close(self):
        self.producer.flush()
        self.producer.close()
        logger.info("Kafka connections closed.")
        # Thêm vào trong class KafkaBroker (cùng cấp với send_to_topic, consume_to_storage...)
    def flush(self):
        """Đảm bảo toàn bộ tin nhắn đang chờ trong buffer được gửi đi ngay lập tức"""
        if self.producer:
            self.producer.flush()
            logger.debug("Kafka producer flushed.")
    