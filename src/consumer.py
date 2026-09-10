"""Consume Avro orders and print real-time running price averages."""

from __future__ import annotations

import os
import signal
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from src.aggregation import OrderAggregator
from src.avro_codec import deserialize_order


running = True


def request_shutdown(_signum: int, _frame: Any) -> None:
    global running
    running = False


def log_aggregate(order: dict[str, Any], snapshot: dict[str, Any]) -> None:
    print(
        f"[SUCCESS] orderId={order['orderId']} product={order['product']} "
        f"price={order['price']:.2f} | "
        f"overall count={snapshot['overall_count']} "
        f"average={snapshot['overall_average']:.2f} | "
        f"{snapshot['product']} count={snapshot['product_count']} "
        f"average={snapshot['product_average']:.2f}",
        flush=True,
    )


def main() -> int:
    bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
    orders_topic = os.getenv("ORDERS_TOPIC", "orders")
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": os.getenv("CONSUMER_GROUP", "order-aggregator"),
            "client.id": "order-consumer",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    aggregator = OrderAggregator()

    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)
    consumer.subscribe([orders_topic])
    print(
        f"[CONSUMER] listening topics={orders_topic} via {bootstrap_servers}",
        flush=True,
    )

    try:
        while running:
            message: Message | None = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())

            order = deserialize_order(message.value())
            snapshot = aggregator.add(order)
            log_aggregate(order, snapshot)
            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()
        print("[CONSUMER] stopped", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

