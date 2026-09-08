from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import DeserializingConsumer, KafkaException, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import StringDeserializer, StringSerializer

from .config import Settings
from .processing import OrderProcessor, PermanentProcessingError, TemporaryProcessingError


ROOT = Path(__file__).resolve().parent.parent


def build_clients(settings: Settings):
    registry = SchemaRegistryClient({"url": settings.schema_registry_url})
    order_schema = (ROOT / "order.avsc").read_text(encoding="utf-8")
    dlq_schema = (ROOT / "dlq.avsc").read_text(encoding="utf-8")
    consumer = DeserializingConsumer({
        "bootstrap.servers": settings.bootstrap_servers,
        "group.id": settings.consumer_group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "key.deserializer": StringDeserializer("utf_8"),
        "value.deserializer": AvroDeserializer(registry, order_schema),
    })
    dlq_producer = SerializingProducer({
        "bootstrap.servers": settings.bootstrap_servers,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": AvroSerializer(registry, dlq_schema),
        "acks": "all",
        "enable.idempotence": True,
    })
    return consumer, dlq_producer


def send_to_dlq(producer, topic: str, order: dict, error: Exception, attempts: int) -> None:
    record = {
        **order,
        "error": str(error),
        "attempts": attempts,
        "failedAt": datetime.now(timezone.utc).isoformat(),
    }
    delivery_errors = []

    def delivered(err, _message):
        if err:
            delivery_errors.append(err)

    producer.produce(topic, key=order["orderId"], value=record, on_delivery=delivered)
    remaining = producer.flush(10)
    if remaining or delivery_errors:
        raise KafkaException(delivery_errors[0] if delivery_errors else "DLQ delivery timed out")
    print("DLQ:", json.dumps(record))


def main() -> None:
    settings = Settings()
    consumer, dlq_producer = build_clients(settings)
    processor = OrderProcessor(
        max_retries=settings.max_retries,
        backoff_seconds=settings.retry_backoff_seconds,
        transient_failure_rate=settings.transient_failure_rate,
        on_retry=lambda attempt, delay, error: print(
            f"Temporary failure after attempt {attempt}; retrying in {delay:.2f}s: {error}"
        ),
    )
    consumer.subscribe([settings.orders_topic])
    print(f"Listening on {settings.orders_topic}; Ctrl+C to stop")
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            order = message.value()
            try:
                result = processor.process(order)
                print(
                    f"Processed order={order['orderId']} price={order['price']:.2f} "
                    f"attempts={result.attempts} count={result.count} "
                    f"running_average={result.running_average:.2f}"
                )
            except (PermanentProcessingError, TemporaryProcessingError) as error:
                attempts = 1 if isinstance(error, PermanentProcessingError) else settings.max_retries + 1
                send_to_dlq(dlq_producer, settings.dlq_topic, order, error, attempts)
            # Commit only after successful processing or confirmed DLQ delivery.
            consumer.commit(message=message, asynchronous=False)
    except KeyboardInterrupt:
        print("Stopping consumer")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
