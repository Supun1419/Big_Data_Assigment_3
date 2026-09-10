"""Consume orders with aggregation, retry, and dead-letter handling."""

from __future__ import annotations

import os
import signal
import time
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer

from src.aggregation import OrderAggregator
from src.avro_codec import deserialize_order
from src.processing import PermanentOrderError, TemporaryOrderError, validate_and_process


running = True


def request_shutdown(_signum: int, _frame: Any) -> None:
    global running
    running = False


def message_headers(message: Message) -> dict[str, bytes | None]:
    return dict(message.headers() or [])


def header_int(headers: dict[str, bytes | None], name: str, default: int = 0) -> int:
    value = headers.get(name)
    if value is None:
        return default
    try:
        return int(value.decode("utf-8"))
    except (AttributeError, UnicodeDecodeError, ValueError):
        return default


def publish_and_wait(
    producer: Producer,
    topic: str,
    message: Message,
    headers: dict[str, bytes | None],
) -> None:
    """Forward a record and wait for Kafka to acknowledge it before committing."""

    delivery_errors: list[str] = []

    def delivered(error: KafkaError | None, _message: Message) -> None:
        if error is not None:
            delivery_errors.append(str(error))

    producer.produce(
        topic=topic,
        key=message.key(),
        value=message.value(),
        headers=list(headers.items()),
        on_delivery=delivered,
    )
    remaining = producer.flush(10)
    if remaining or delivery_errors:
        raise RuntimeError(
            f"could not publish to {topic}: remaining={remaining}, errors={delivery_errors}"
        )


def route_to_dlq(
    producer: Producer,
    dlq_topic: str,
    message: Message,
    retry_count: int,
    reason: str,
    order_id: str,
) -> None:
    headers = message_headers(message)
    headers.update(
        {
            "x-original-topic": message.topic().encode("utf-8"),
            "x-retry-count": str(retry_count).encode("utf-8"),
            "x-error-reason": reason.encode("utf-8"),
        }
    )
    publish_and_wait(producer, dlq_topic, message, headers)
    print(
        f"[DLQ] orderId={order_id} retries={retry_count} reason={reason}",
        flush=True,
    )


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
    retry_topic = os.getenv("RETRY_TOPIC", "orders-retry")
    dlq_topic = os.getenv("DLQ_TOPIC", "orders-dlq")
    max_retries = int(os.getenv("MAX_RETRIES", "3"))
    retry_base_seconds = float(os.getenv("RETRY_BASE_SECONDS", "1"))

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": os.getenv("CONSUMER_GROUP", "order-aggregator"),
            "client.id": "order-consumer",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    forwarding_producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "order-router",
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    aggregator = OrderAggregator()

    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)
    consumer.subscribe([orders_topic, retry_topic])
    print(
        f"[CONSUMER] listening topics={orders_topic},{retry_topic} "
        f"max_retries={max_retries} via {bootstrap_servers}",
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

            headers = message_headers(message)
            retry_count = header_int(headers, "x-retry-count")
            key = message.key().decode("utf-8", errors="replace") if message.key() else "unknown"

            try:
                order = deserialize_order(message.value())
            except Exception as error:
                route_to_dlq(
                    forwarding_producer,
                    dlq_topic,
                    message,
                    retry_count,
                    f"invalid Avro payload: {error}",
                    key,
                )
                consumer.commit(message=message, asynchronous=False)
                continue

            try:
                validate_and_process(order, retry_count)
            except TemporaryOrderError as error:
                if retry_count < max_retries:
                    next_retry = retry_count + 1
                    delay = retry_base_seconds * (2**retry_count)
                    print(
                        f"[RETRY] orderId={order['orderId']} "
                        f"attempt={next_retry}/{max_retries} delay={delay:.1f}s "
                        f"reason={error}",
                        flush=True,
                    )
                    time.sleep(delay)
                    headers.update(
                        {
                            "x-original-topic": headers.get(
                                "x-original-topic", message.topic().encode("utf-8")
                            ),
                            "x-retry-count": str(next_retry).encode("utf-8"),
                            "x-error-reason": str(error).encode("utf-8"),
                        }
                    )
                    publish_and_wait(forwarding_producer, retry_topic, message, headers)
                else:
                    route_to_dlq(
                        forwarding_producer,
                        dlq_topic,
                        message,
                        retry_count,
                        f"retries exhausted: {error}",
                        order["orderId"],
                    )
            except PermanentOrderError as error:
                route_to_dlq(
                    forwarding_producer,
                    dlq_topic,
                    message,
                    retry_count,
                    f"permanent failure: {error}",
                    order["orderId"],
                )
            else:
                snapshot = aggregator.add(order)
                log_aggregate(order, snapshot)

            consumer.commit(message=message, asynchronous=False)
    finally:
        forwarding_producer.flush(5)
        consumer.close()
        print("[CONSUMER] stopped", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
