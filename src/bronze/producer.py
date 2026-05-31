"""
Bronze layer: Real-time data producer.

Simulates realistic user events and orders, publishing them to Redpanda topics
for downstream consumption by the Bronze consumer. This is the entry point for
the entire data pipeline — without this, nothing flows.

Production concerns addressed:
  - Structured logging with correlation IDs via the event/order ID
  - Graceful shutdown on SIGINT/SIGTERM
  - Configurable via env vars through Settings
  - Retry logic for Kafka producer failures
  - Rate limiting to prevent overwhelming downstream consumers
  - Memory-safe: no unbounded buffering, flushes periodically
"""

import json
import logging
import random
import signal
import sys
import time
from typing import Any, NoReturn

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from bronze.config import settings
from bronze.models import Event, EventType, Order, OrderItem

logger = logging.getLogger(__name__)

# Realistic page paths for event simulation
PAGES: list[str] = [
    "/home",
    "/products",
    "/products/electronics",
    "/product/123",
    "/product/456",
    "/cart",
    "/checkout",
    "/about",
    "/contact",
    "/blog",
]

# Referrers with None weighted higher to simulate direct traffic
REFERRERS: list[str | None] = [
    "https://google.com",
    "https://twitter.com",
    "https://facebook.com",
    "https://linkedin.com",
    None,
    None,
    None,
]

USER_IDS: list[str] = [f"user_{i:04d}" for i in range(1, 51)]
SESSION_IDS: list[str] = [f"sess_{i:06d}" for i in range(1, 501)]

PRODUCTS: list[tuple[str, str, float]] = [
    ("prod_001", "Wireless Mouse", 29.99),
    ("prod_002", "Mechanical Keyboard", 89.99),
    ("prod_003", "USB-C Hub 7-in-1", 49.99),
    ("prod_004", "27-inch 4K Monitor", 299.99),
    ("prod_005", "HD Webcam 1080p", 79.99),
    ("prod_006", "Noise Canceling Headphones", 199.99),
    ("prod_007", "Adjustable Laptop Stand", 39.99),
    ("prod_008", "LED Desk Lamp", 24.99),
    ("prod_009", "External SSD 1TB", 109.99),
    ("prod_010", "Ergonomic Foot Rest", 34.99),
]

# Base metadata templates per event type — randomized at generation time
METADATA_TEMPLATES: dict[EventType, dict[str, Any]] = {
    EventType.page_view: {"browser": "Chrome", "os": "macOS", "viewport": "1920x1080"},
    EventType.click: {"element": "button", "selector": "#add-to-cart"},
    EventType.purchase: {"payment_method": "credit_card", "coupon_applied": False},
    EventType.signup: {"method": "email", "newsletter_optin": True},
    EventType.error: {"error_code": 500, "error_message": "Internal Server Error"},
}

# Global shutdown flag for signal handling
_shutdown_requested: bool = False


def _handle_signal(signum: int, _frame: Any) -> None:
    """Set shutdown flag on SIGINT/SIGTERM so main loop can exit cleanly."""
    global _shutdown_requested
    logger.info("Received signal %d — initiating graceful shutdown", signum)
    _shutdown_requested = True


def generate_event() -> Event:
    """
    Generate a single realistic event with randomized metadata.

    Metadata fields vary by event type to simulate real-world diversity:
      - page_view: includes page dwell time
      - click: random element type and CSS selector
      - purchase: varied payment methods, occasional coupon usage
      - error: realistic HTTP error codes with messages
    """
    event_type = random.choice(list(EventType))
    meta = dict(METADATA_TEMPLATES[event_type])

    if event_type == EventType.page_view:
        meta["duration_seconds"] = random.randint(5, 300)
    elif event_type == EventType.click:
        meta["element"] = random.choice(["button", "link", "image", "form"])
        meta["selector"] = random.choice(["#add-to-cart", "#checkout", "#search", "#nav-home"])
    elif event_type == EventType.purchase:
        meta["payment_method"] = random.choice(["credit_card", "paypal", "apple_pay"])
        meta["coupon_applied"] = random.random() < 0.2
    elif event_type == EventType.error:
        error_map = {400: "Bad Request", 404: "Not Found", 500: "Internal Server Error", 503: "Service Unavailable"}
        code = random.choice(list(error_map.keys()))
        meta["error_code"] = code
        meta["error_message"] = error_map[code]

    return Event(
        event_type=event_type,
        user_id=random.choice(USER_IDS),
        session_id=random.choice(SESSION_IDS),
        page=random.choice(PAGES),
        referrer=random.choice(REFERRERS),
        metadata=meta,
    )


def generate_order() -> Order:
    """
    Generate a multi-item order from randomly selected products.

    Each order contains 1-4 line items with varied quantities.
    Total is calculated from line items (not random) to ensure data integrity
    for downstream validation in the Silver layer.
    """
    num_items = random.randint(1, 4)

    selected = random.sample(PRODUCTS, num_items)
    items = [
        OrderItem(
            product_id=prod_id,
            product_name=prod_name,
            quantity=random.randint(1, 3),
            unit_price=unit_price,
        )
        for prod_id, prod_name, unit_price in selected
    ]

    total = round(sum(it.quantity * it.unit_price for it in items), 2)
    return Order(
        user_id=random.choice(USER_IDS),
        items=items,
        total_amount=total,
        currency=random.choice(["USD", "USD", "USD", "EUR"]),
        status=random.choice(["pending", "pending", "confirmed", "shipped"]),
        shipping_address=(
            f"{random.randint(1, 9999)} {random.choice(['Main', 'Oak', 'Elm', 'Park'])} St, "
            f"{random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston'])}, "
            f"{random.choice(['NY', 'CA', 'IL', 'TX'])} {random.randint(10000, 99999)}"
        ),
    )


def _create_producer() -> KafkaProducer:
    """
    Create and return a configured KafkaProducer with retry logic.

    Raises:
        NoBrokersAvailable: If Redpanda is not reachable after retries.
    """
    max_retries = 3
    retry_delay = 2

    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=settings.redpanda_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                # Production settings: acks=all ensures no data loss
                acks="all",
                retries=5,
                linger_ms=10,
                batch_size=16384,
            )
            logger.info("Connected to Redpanda at %s", settings.redpanda_bootstrap_servers)
            return producer
        except NoBrokersAvailable:
            if attempt < max_retries:
                logger.warning("Redpanda not available (attempt %d/%d) — retrying in %ds", attempt, max_retries, retry_delay)
                time.sleep(retry_delay)
            else:
                raise


def main() -> None:
    """
    Main entry point. Sets up logging, signal handlers, producer, and runs
    the generation loop until interrupted.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    logger.info("Starting Bronze producer — generating data to Redpanda")

    try:
        producer = _create_producer()
    except NoBrokersAvailable:
        logger.error("Cannot connect to Redpanda at %s. Is it running?", settings.redpanda_bootstrap_servers)
        sys.exit(1)

    # Track message counts for periodic logging
    message_count = 0
    last_log_time = time.time()
    log_interval = 60  # Log stats every 60 seconds

    try:
        while not _shutdown_requested:
            # 70% chance of event, 30% chance of order
            if random.random() < 0.7:
                event = generate_event()
                data = event.model_dump(mode="json")
                future = producer.send(settings.topic_events, value=data)
                message_count += 1
                logger.info("Produced event [%s] %s", event.event_type.value, event.event_id)
            else:
                order = generate_order()
                data = order.model_dump(mode="json")
                future = producer.send(settings.topic_orders, value=data)
                message_count += 1
                logger.info("Produced order %s ($%.2f, %d items)", order.order_id, order.total_amount, len(order.items))

            # Periodically flush and log throughput stats
            elapsed = time.time() - last_log_time
            if elapsed >= log_interval:
                producer.flush()
                rate = message_count / elapsed
                logger.info("Producer stats: %d messages in %.0fs (%.1f msg/s)", message_count, elapsed, rate)
                message_count = 0
                last_log_time = time.time()

            time.sleep(random.uniform(0.5, 3.0))

    except KeyboardInterrupt:
        logger.info("Shutdown requested — closing producer")
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer stopped — %d pending messages flushed", message_count)


if __name__ == "__main__":
    main()
