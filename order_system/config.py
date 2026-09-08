from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    schema_registry_url: str = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    orders_topic: str = os.getenv("ORDERS_TOPIC", "orders")
    dlq_topic: str = os.getenv("DLQ_TOPIC", "orders-dlq")
    consumer_group: str = os.getenv("CONSUMER_GROUP", "order-processor-v1")
    max_retries: int = int(os.getenv("MAX_RETRIES", "3"))
    retry_backoff_seconds: float = float(os.getenv("RETRY_BACKOFF_SECONDS", "1"))
    transient_failure_rate: float = float(os.getenv("TRANSIENT_FAILURE_RATE", "0"))

