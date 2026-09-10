import pytest

from src.avro_codec import deserialize_order, serialize_order


def test_order_avro_round_trip() -> None:
    order = {"orderId": "1001", "product": "Item1", "price": 125.5}

    decoded = deserialize_order(serialize_order(order))

    assert decoded == order


def test_schema_rejects_a_missing_field() -> None:
    incomplete_order = {"orderId": "1002", "product": "Item2"}

    with pytest.raises(ValueError):
        serialize_order(incomplete_order)

