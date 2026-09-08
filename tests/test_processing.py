import pytest

from order_system.processing import (
    OrderProcessor,
    PermanentProcessingError,
    TemporaryProcessingError,
)


def test_running_average_is_updated_for_successful_orders():
    processor = OrderProcessor(random_value=lambda: 1.0)
    first = processor.process({"orderId": "1001", "product": "Item1", "price": 10.0})
    second = processor.process({"orderId": "1002", "product": "Item2", "price": 20.0})
    assert first.running_average == 10.0
    assert second.count == 2
    assert second.running_average == 15.0


def test_temporary_failure_is_retried_with_exponential_backoff():
    values = iter([0.0, 0.0, 1.0])
    sleeps = []
    retries = []
    processor = OrderProcessor(
        max_retries=3,
        backoff_seconds=0.25,
        transient_failure_rate=0.5,
        random_value=lambda: next(values),
        sleep=sleeps.append,
        on_retry=lambda attempt, delay, _error: retries.append((attempt, delay)),
    )
    result = processor.process({"orderId": "1001", "product": "Item1", "price": 30.0})
    assert result.attempts == 3
    assert sleeps == [0.25, 0.5]
    assert retries == [(1, 0.25), (2, 0.5)]
    assert result.running_average == 30.0


def test_exhausted_temporary_failure_is_raised_for_dlq():
    processor = OrderProcessor(max_retries=2, transient_failure_rate=1, sleep=lambda _: None)
    with pytest.raises(TemporaryProcessingError):
        processor.process({"orderId": "1001", "product": "Item1", "price": 10.0})
    assert processor.average.count == 0


@pytest.mark.parametrize(
    "order",
    [
        {"orderId": "", "product": "Item1", "price": 10.0},
        {"orderId": "1001", "product": "", "price": 10.0},
        {"orderId": "1001", "product": "Item1", "price": 0.0},
        {"orderId": "1001", "product": "FAIL", "price": 10.0},
    ],
)
def test_invalid_order_fails_permanently_without_retry(order):
    sleeps = []
    processor = OrderProcessor(sleep=sleeps.append)
    with pytest.raises(PermanentProcessingError):
        processor.process(order)
    assert sleeps == []
    assert processor.average.count == 0
