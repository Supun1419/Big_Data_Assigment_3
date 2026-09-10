import pytest

from src.processing import PermanentOrderError, TemporaryOrderError, validate_and_process


def order(product: str, price: float = 25.0) -> dict[str, object]:
    return {"orderId": "1001", "product": product, "price": price}


def test_normal_order_succeeds() -> None:
    validate_and_process(order("Item1"), retry_count=0)


def test_retry_item_succeeds_on_third_attempt() -> None:
    with pytest.raises(TemporaryOrderError):
        validate_and_process(order("RetryItem"), retry_count=0)
    with pytest.raises(TemporaryOrderError):
        validate_and_process(order("RetryItem"), retry_count=1)

    validate_and_process(order("RetryItem"), retry_count=2)


def test_always_fail_item_remains_temporary() -> None:
    with pytest.raises(TemporaryOrderError):
        validate_and_process(order("AlwaysFailItem"), retry_count=3)


@pytest.mark.parametrize(
    "invalid_order",
    [order("InvalidItem"), order("Item1", price=0.0)],
)
def test_permanent_failures(invalid_order: dict[str, object]) -> None:
    with pytest.raises(PermanentOrderError):
        validate_and_process(invalid_order, retry_count=0)

