"""Read and display Avro orders routed to the dead-letter queue."""

from __future__ import annotations

import argparse
import os
import time
import uuid

from confluent_kafka import Consumer, KafkaError, KafkaException

from src.avro_codec import deserialize_order


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-messages", type=int, default=10)
    parser.add_argument("--idle-timeout", type=float, default=10.0)
    return parser.parse_args()


def decode_headers(headers: list[tuple[str, bytes | None]] | None) -> dict[str, str]:
    return {
        key: value.decode("utf-8", errors="replace") if value is not None else ""
        for key, value in (headers or [])
    }


def main() -> int:
    args = parse_args()
    bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
    dlq_topic = os.getenv("DLQ_TOPIC", "orders-dlq")
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"dlq-demo-{uuid.uuid4()}",
            "client.id": "dlq-reader",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([dlq_topic])
    print(f"[DLQ-READER] reading {dlq_topic}", flush=True)
    messages_read = 0
    deadline = time.monotonic() + args.idle_timeout

    try:
        while messages_read < args.max_messages and time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())

            headers = decode_headers(message.headers())
            try:
                order = deserialize_order(message.value())
            except Exception as error:
                order = {"unreadablePayload": str(error)}
            print(
                f"[DLQ-RECORD] order={order} "
                f"retries={headers.get('x-retry-count', '0')} "
                f"reason={headers.get('x-error-reason', 'unknown')}",
                flush=True,
            )
            messages_read += 1
            deadline = time.monotonic() + args.idle_timeout
    finally:
        consumer.close()

    print(f"[DLQ-READER] complete: records={messages_read}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

