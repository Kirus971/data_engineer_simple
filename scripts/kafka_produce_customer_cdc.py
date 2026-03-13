import json
import os
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def make_event(customer_id: int) -> dict:
    status = random.choice(["active", "inactive", "vip"])
    return {
        "op": random.choice(["I", "U"]),
        "event_ts": utc_now_iso(),
        "customer_id": customer_id,
        "name": f"customer_{customer_id}",
        "status": status,
        "updated_at": utc_now_iso(),
        "source": "demo-generator",
    }


def main() -> None:
    brokers = os.environ.get("KAFKA_BROKERS", "localhost:29092")
    topic = os.environ.get("KAFKA_TOPIC_CUSTOMER_CDC", "customer_cdc")
    rate = float(os.environ.get("EVENTS_PER_SEC", "5"))
    max_id = int(os.environ.get("MAX_CUSTOMER_ID", "1000"))

    producer = Producer({"bootstrap.servers": brokers})

    interval = 1.0 / rate if rate > 0 else 0.0
    while True:
        cid = random.randint(1, max_id)
        event = make_event(cid)
        producer.produce(topic, json.dumps(event).encode("utf-8"))
        producer.poll(0)
        time.sleep(interval)


if __name__ == "__main__":
    main()

