"""Produce randomized Avro order messages for the assignment demo."""

from __future__ import annotations

import argparse
import os
import random
import sys
import time
from itertools import cycle
from typing import Any

from confluent_kafka import KafkaError, Message, Producer

from src.avro_codec import serialize_order


DEFAULT_PRODUCTS = ("Item1", "Item2", "Item3", "Item4")
DEMO_PRODUCTS = ("Item1", "RetryItem", "Item2", "InvalidItem", "AlwaysFailItem")


def build_order(order_id: str, product: str, rng: random.Random) -> dict[str, Any]:
    """Create one schema-valid order with a randomized price."""

    return {
        "orderId": order_id,
        "product": product,
        "price": round(rng.uniform(10.0, 500.0), 2),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=10, help="number of orders to send")
    parser.add_argument(
        "--interval",
        type=float,
        default=0.25,
        help="seconds to wait between orders",
    )
    parser.add_argument("--seed", type=int, default=42, help="random-price seed")
    parser.add_argument(
        "--include-failures",
        action="store_true",
        help="include orders that demonstrate retry and DLQ handling",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.count < 1:
        raise SystemExit("--count must be at least 1")
    if args.interval < 0:
        raise SystemExit("--interval cannot be negative")

    bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
    orders_topic = os.getenv("ORDERS_TOPIC", "orders")
    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "order-producer",
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    rng = random.Random(args.seed)
    products = cycle(DEMO_PRODUCTS if args.include_failures else DEFAULT_PRODUCTS)
    run_id = int(time.time() * 1000)
    delivery_errors: list[str] = []

    def delivered(error: KafkaError | None, message: Message) -> None:
        if error is not None:
            delivery_errors.append(str(error))
            print(f"[PRODUCER] delivery failed: {error}", file=sys.stderr, flush=True)
            return
        print(
            f"[PRODUCER] delivered topic={message.topic()} "
            f"partition={message.partition()} offset={message.offset()}",
            flush=True,
        )

    print(
        f"[PRODUCER] sending {args.count} Avro orders to {orders_topic} "
        f"via {bootstrap_servers}",
        flush=True,
    )
    for sequence in range(1, args.count + 1):
        product = next(products)
        order = build_order(f"{run_id}-{sequence:03d}", product, rng)
        producer.produce(
            topic=orders_topic,
            key=order["orderId"].encode("utf-8"),
            value=serialize_order(order),
            headers={"content-type": "avro/binary", "event-type": "order-created"},
            on_delivery=delivered,
        )
        producer.poll(0)
        print(
            f"[PRODUCER] queued orderId={order['orderId']} "
            f"product={order['product']} price={order['price']:.2f}",
            flush=True,
        )
        time.sleep(args.interval)

    undelivered = producer.flush(15)
    if undelivered or delivery_errors:
        print(
            f"[PRODUCER] failed: undelivered={undelivered} errors={len(delivery_errors)}",
            file=sys.stderr,
        )
        return 1

    print(f"[PRODUCER] complete: delivered={args.count}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

