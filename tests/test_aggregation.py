import pytest

from src.aggregation import OrderAggregator, RunningAverage


def test_empty_running_average_is_zero() -> None:
    assert RunningAverage().average == 0.0


def test_overall_and_product_averages() -> None:
    aggregator = OrderAggregator()

    aggregator.add({"product": "Item1", "price": 10.0})
    aggregator.add({"product": "Item2", "price": 20.0})
    snapshot = aggregator.add({"product": "Item1", "price": 30.0})

    assert snapshot["overall_count"] == 3
    assert snapshot["overall_average"] == pytest.approx(20.0)
    assert snapshot["product_count"] == 2
    assert snapshot["product_average"] == pytest.approx(20.0)

