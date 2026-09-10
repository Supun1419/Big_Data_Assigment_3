"""In-memory running-price aggregation for successfully processed orders."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class RunningAverage:
    """Track an average without retaining every observed price."""

    count: int = 0
    total: float = 0.0

    @property
    def average(self) -> float:
        return self.total / self.count if self.count else 0.0

    def add(self, value: float) -> None:
        self.count += 1
        self.total += value


@dataclass
class OrderAggregator:
    """Maintain global and per-product running price averages."""

    overall: RunningAverage = field(default_factory=RunningAverage)
    by_product: dict[str, RunningAverage] = field(default_factory=dict)

    def add(self, order: dict[str, Any]) -> dict[str, float | int | str]:
        product = str(order["product"])
        price = float(order["price"])
        product_average = self.by_product.setdefault(product, RunningAverage())

        self.overall.add(price)
        product_average.add(price)

        return {
            "product": product,
            "overall_count": self.overall.count,
            "overall_average": self.overall.average,
            "product_count": product_average.count,
            "product_average": product_average.average,
        }

