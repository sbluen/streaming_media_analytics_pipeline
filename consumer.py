import os

from confluent_kafka import Consumer, KafkaError

config = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
    "group.id": os.getenv("KAFKA_GROUP_ID", "stream-events-consumer"),
    "auto.offset.reset": "earliest",
}
TOPIC = os.getenv("KAFKA_TOPIC", "stream-events")

consumer = Consumer(config)
consumer.subscribe([TOPIC])
print(f"Listening for events on '{TOPIC}' at {config['bootstrap.servers']}", flush=True)

try:
    while True:
        message = consumer.poll(1.0)
        if message is None:
            continue
        if message.error():
            if message.error().code() != KafkaError._PARTITION_EOF:
                print(f"Kafka consumer error: {message.error()}", flush=True)
            continue
        print(f"Received message: {message.value().decode('utf-8')}", flush=True)
finally:
    consumer.close()