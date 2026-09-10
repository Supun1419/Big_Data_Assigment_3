"""Business validation and controlled failure scenarios for the live demo."""

from __future__ import annotations

from typing import Any


class TemporaryOrderError(Exception):
    """An order-processing failure that may succeed when retried."""


class PermanentOrderError(Exception):
    """An order-processing failure that must be sent directly to the DLQ."""


def validate_and_process(order: dict[str, Any], retry_count: int) -> None:
    """Validate an order and simulate deterministic demo failures.

    RetryItem succeeds on its third processing attempt. AlwaysFailItem remains
    unavailable until retries are exhausted. InvalidItem represents a permanent
    business-rule failure. All three remain valid Avro order records.
    """

    if not order["orderId"].strip():
        raise PermanentOrderError("orderId cannot be blank")
    if float(order["price"]) <= 0:
        raise PermanentOrderError("price must be greater than zero")

    product = order["product"]
    if product == "InvalidItem":
        raise PermanentOrderError("product is not present in the catalogue")
    if product == "AlwaysFailItem":
        raise TemporaryOrderError("inventory service remains unavailable")
    if product == "RetryItem" and retry_count < 2:
        raise TemporaryOrderError("inventory service is temporarily unavailable")

