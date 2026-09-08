from __future__ import annotations

import argparse
import json
import random
import time
import uuid
from pathlib import Path

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

from .config import Settings


ROOT = Path(__file__).resolve().parent.parent


def delivery_report(error, message) -> None:
    if error:
        print(f"Delivery failed: {error}")
    else:
        print(f"Produced {message.key()} to {message.topic()}[{message.partition()}]@{message.offset()}")


def build_producer(settings: Settings) -> SerializingProducer:
    schema = (ROOT / "order.avsc").read_text(encoding="utf-8")
    registry = SchemaRegistryClient({"url": settings.schema_registry_url})
    return SerializingProducer({
        "bootstrap.servers": settings.bootstrap_servers,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": AvroSerializer(registry, schema),
        "acks": "all",
        "enable.idempotence": True,
    })


def main() -> None:
    parser = argparse.ArgumentParser(description="Produce randomized Avro order messages")
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--interval", type=float, default=0.5)
    parser.add_argument("--include-failure", action="store_true", help="send one permanent-failure demo order")
    args = parser.parse_args()
    settings = Settings()
    producer = build_producer(settings)

    for index in range(args.count):
        order = {
            "orderId": str(uuid.uuid4()),
            "product": "FAIL" if args.include_failure and index == args.count - 1 else f"Item{random.randint(1, 5)}",
            "price": round(random.uniform(10, 500), 2),
        }
        producer.produce(settings.orders_topic, key=order["orderId"], value=order, on_delivery=delivery_report)
        producer.poll(0)
        print("Order:", json.dumps(order))
        time.sleep(args.interval)
    producer.flush()


if __name__ == "__main__":
    main()

