import json
from kafka import KafkaProducer

class KafkaBroker:
    def __init__(self, bootstrap_servers):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            # Mã hóa Key là String
            key_serializer=lambda k: k.encode('utf-8'), 
            # Mã hóa Value là JSON
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all'
        )

    def send_to_topic(self, topic, key, data):
        """Gửi dữ liệu kèm theo Key (tên đường/ID)"""
        if data:
            # Truyền thêm tham số key vào đây
            self.producer.send(topic, key=key, value=data)
            
    def flush(self):
        self.producer.flush()

    def close(self):
        self.producer.close()