from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Callable, Mapping, Optional


class TemporaryProcessingError(RuntimeError):
    """A retryable processing error."""


class PermanentProcessingError(RuntimeError):
    """A non-retryable invalid-order error."""


@dataclass(frozen=True)
class ProcessingResult:
    count: int
    running_average: float
    attempts: int


class RunningAverage:
    def __init__(self) -> None:
        self.count = 0
        self.total = 0.0

    def add(self, price: float) -> float:
        self.count += 1
        self.total += price
        return self.total / self.count


class OrderProcessor:
    def __init__(
        self,
        max_retries: int = 3,
        backoff_seconds: float = 1.0,
        transient_failure_rate: float = 0.0,
        sleep: Callable[[float], None] = time.sleep,
        random_value: Callable[[], float] = random.random,
        on_retry: Optional[Callable[[int, float, Exception], None]] = None,
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries cannot be negative")
        if not 0 <= transient_failure_rate <= 1:
            raise ValueError("transient_failure_rate must be between 0 and 1")
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.transient_failure_rate = transient_failure_rate
        self.sleep = sleep
        self.random_value = random_value
        self.on_retry = on_retry or (lambda _attempt, _delay, _error: None)
        self.average = RunningAverage()

    def _validate_and_process(self, order: Mapping[str, object]) -> None:
        if not str(order.get("orderId", "")).strip():
            raise PermanentProcessingError("orderId is required")
        if not str(order.get("product", "")).strip():
            raise PermanentProcessingError("product is required")
        if float(order.get("price", 0)) <= 0:
            raise PermanentProcessingError("price must be greater than zero")
        # Product FAIL is a deterministic live-demo path to the DLQ.
        if str(order["product"]).upper() == "FAIL":
            raise PermanentProcessingError("product was marked as permanently failing")
        if self.random_value() < self.transient_failure_rate:
            raise TemporaryProcessingError("simulated temporary downstream failure")

    def process(self, order: Mapping[str, object]) -> ProcessingResult:
        attempts = 0
        while True:
            attempts += 1
            try:
                self._validate_and_process(order)
                value = self.average.add(float(order["price"]))
                return ProcessingResult(self.average.count, value, attempts)
            except PermanentProcessingError:
                raise
            except TemporaryProcessingError as error:
                if attempts > self.max_retries:
                    raise
                delay = self.backoff_seconds * (2 ** (attempts - 1))
                self.on_retry(attempts, delay, error)
                self.sleep(delay)
