import csv
import json
import os
import random
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer

BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
TOPIC = os.getenv("KAFKA_TOPIC", "stream-events")
INTERVAL_SECONDS = float(os.getenv("EVENT_INTERVAL_SECONDS", "2"))
PLATFORMS = ("web", "ios", "android", "roku", "fire_tv")
TRACKS_CSV = Path(__file__).resolve().parent / "royalty_transformation" / "seeds" / "tracks.csv"

with TRACKS_CSV.open(newline="", encoding="utf-8") as tracks_file:
    TRACK_IDS = [row["track_id"] for row in csv.DictReader(tracks_file) if row.get("track_id")]

if not TRACK_IDS:
    raise ValueError(f"No track IDs found in {TRACKS_CSV}")

running = True


def stop(_signum, _frame):
    global running
    running = False


def make_event():
    return {
        "track_id": random.choice(TRACK_IDS),
        "user_id": f"user-{random.randint(1, 10000):05d}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "platform": random.choice(PLATFORMS),
    }


def main():
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda event: json.dumps(event).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
    )

    print(f"Sending events to topic '{TOPIC}' at {BROKER} every {INTERVAL_SECONDS:g}s. Press Ctrl+C to stop.")
    try:
        while running:
            event = make_event()
            producer.send(TOPIC, key=event["user_id"], value=event)
            producer.flush()
            print(json.dumps(event), flush=True)

            # Wait interruptibly so Ctrl+C stops promptly.
            deadline = time.monotonic() + INTERVAL_SECONDS
            while running and time.monotonic() < deadline:
                time.sleep(min(0.2, deadline - time.monotonic()))
    finally:
        producer.close()
        print("Producer stopped.")


if __name__ == "__main__":
    main()
