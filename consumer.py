import datetime
import json
import logging
import os
import psycopg

from confluent_kafka import Consumer, KafkaError

# Set up standard Python logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
    "group.id": os.getenv("KAFKA_GROUP_ID", "stream-events-consumer"),
    "auto.offset.reset": "earliest",
}



TOPIC = os.getenv("KAFKA_TOPIC", "stream-events")

CONNECTION_STRING = os.getenv(
    "DATABASE_URL",
    "postgresql://analytics:analytics_dev_password@localhost:5433/streaming_analytics",
)

MAX_ROWS_PER_TRANSACTION = 50
MAX_INTERVAL_BETWEEN_TRANSACTIONS = datetime.timedelta(seconds=1)

consumer = Consumer(CONFIG)
consumer.subscribe([TOPIC])
print(f"Listening for events on '{TOPIC}' at {CONFIG['bootstrap.servers']}", flush=True)

message_count = 0
prev_timestamp = datetime.datetime.now()
data = []

try:
    with psycopg.connect(CONNECTION_STRING, autocommit=True) as conn, conn.cursor() as cursor:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            error = message.error()
            if error is not None:
                if error.code() != KafkaError._PARTITION_EOF:
                    print(f"Kafka consumer error: {message.error()}", flush=True)
                continue

            value = message.value()
            if value is not None:
                print(f"Received message: {value.decode('utf-8')}", flush=True)
                parsed = json.loads(value)
                data.append((message.partition(), message.offset(), parsed["track_id"], parsed["user_id"], parsed["timestamp"], parsed["platform"]))
                message_count += 1
                if (message_count == MAX_ROWS_PER_TRANSACTION or
                    datetime.datetime.now() - prev_timestamp > MAX_INTERVAL_BETWEEN_TRANSACTIONS):
                        cursor.executemany(
                            """
                            INSERT INTO raw_streaming_events (kafka_partition, kafka_offset, track_id, user_id, created_at, platform)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING;
                            """,
                            data,
                        )
                        prev_timestamp = datetime.datetime.now()
                        data = []

finally:
    consumer.close()
