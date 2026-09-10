"""Avro serialization helpers shared by the producer and consumers."""

from __future__ import annotations

import json
import os
from functools import lru_cache
from io import BytesIO
from pathlib import Path
from typing import Any

from fastavro import parse_schema, schemaless_reader, schemaless_writer


DEFAULT_SCHEMA_PATH = Path(__file__).resolve().parents[1] / "schemas" / "order.avsc"


@lru_cache(maxsize=1)
def get_order_schema() -> dict[str, Any]:
    """Load and parse the order schema once per process."""

    schema_path = Path(os.getenv("ORDER_SCHEMA_PATH", DEFAULT_SCHEMA_PATH))
    with schema_path.open("r", encoding="utf-8") as schema_file:
        return parse_schema(json.load(schema_file))


def serialize_order(order: dict[str, Any]) -> bytes:
    """Encode an order as an Avro binary record."""

    buffer = BytesIO()
    schemaless_writer(buffer, get_order_schema(), order, strict=True)
    return buffer.getvalue()


def deserialize_order(payload: bytes) -> dict[str, Any]:
    """Decode an Avro binary record into an order dictionary."""

    return schemaless_reader(BytesIO(payload), get_order_schema())

