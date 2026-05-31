import json
import logging
import signal
from typing import Any

import duckdb
from kafka import KafkaConsumer

from bronze.config import settings
from bronze.models import Event, Order

logger = logging.getLogger(__name__)

_shutdown_requested = False


def _handle_signal(signum: int, frame: Any) -> None:
    global _shutdown_requested
    logger.info("Received signal %d — initiating graceful shutdown", signum)
    _shutdown_requested = True


def _init_database(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bronze.events (
            event_id      VARCHAR,
            event_type    VARCHAR,
            user_id       VARCHAR,
            session_id    VARCHAR,
            page          VARCHAR,
            referrer      VARCHAR,
            metadata      VARCHAR,
            timestamp     TIMESTAMP
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bronze.orders (
            order_id          VARCHAR,
            user_id           VARCHAR,
            items             VARCHAR,
            total_amount      DOUBLE,
            currency          VARCHAR,
            status            VARCHAR,
            shipping_address  VARCHAR,
            created_at        TIMESTAMP
        )
        """
    )

    logger.info("Database initialised — schema bronze, tables events/orders ready")


def _process_event(conn: duckdb.DuckDBPyConnection, data: dict[str, Any]) -> None:
    event = Event(**data)
    conn.execute(
        """
        INSERT INTO bronze.events
            (event_id, event_type, user_id, session_id, page, referrer, metadata, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?::TIMESTAMP)
        """,
        [
            event.event_id,
            event.event_type.value,
            event.user_id,
            event.session_id,
            event.page,
            event.referrer,
            json.dumps(event.metadata),
            event.timestamp.isoformat(),
        ],
    )
    logger.info("Wrote event %s [%s]", event.event_id, event.event_type.value)


def _process_order(conn: duckdb.DuckDBPyConnection, data: dict[str, Any]) -> None:
    order = Order(**data)
    conn.execute(
        """
        INSERT INTO bronze.orders
            (order_id, user_id, items, total_amount, currency, status, shipping_address, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?::TIMESTAMP)
        """,
        [
            order.order_id,
            order.user_id,
            json.dumps([it.model_dump(mode="json") for it in order.items]),
            order.total_amount,
            order.currency,
            order.status,
            order.shipping_address,
            order.created_at.isoformat(),
        ],
    )
    logger.info(
        "Wrote order %s ($%.2f, %d items)",
        order.order_id,
        order.total_amount,
        len(order.items),
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger.info("Starting Bronze consumer — reading from Redpanda, writing to Iceberg")

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    conn = duckdb.connect(settings.duckdb_path)
    _init_database(conn)

    consumer = KafkaConsumer(
        bootstrap_servers=settings.redpanda_bootstrap_servers,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="bronze-consumer",
    )
    consumer.subscribe([settings.topic_events, settings.topic_orders])

    logger.info("Subscribed to topics: %s, %s", settings.topic_events, settings.topic_orders)

    try:
        while not _shutdown_requested:
            records = consumer.poll(timeout_ms=1000)
            if not records:
                continue

            for tp, messages in records.items():
                for msg in messages:
                    try:
                        if tp.topic == settings.topic_events:
                            _process_event(conn, msg.value)
                        elif tp.topic == settings.topic_orders:
                            _process_order(conn, msg.value)
                    except Exception:
                        logger.exception(
                            "Failed to process message from %s [offset %d]",
                            tp.topic,
                            msg.offset,
                        )

            consumer.commit()
    except Exception:
        logger.exception("Unexpected error in consumer loop")
    finally:
        consumer.close()
        conn.close()
        logger.info("Consumer stopped")


if __name__ == "__main__":
    main()
