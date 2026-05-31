from __future__ import annotations

from collections import Counter

from bronze.models import Event, EventType, Order
from bronze.producer import (
    METADATA_TEMPLATES,
    PAGES,
    PRODUCTS,
    SESSION_IDS,
    USER_IDS,
    generate_event,
    generate_order,
)


class TestGenerateEvent:
    """Tests for the generate_event producer function."""

    def test_generate_event_returns_valid_event(self) -> None:
        """Verify generate_event returns a valid Event model instance."""
        event = generate_event()
        assert isinstance(event, Event)

    def test_generate_event_has_all_required_fields_populated(self) -> None:
        """Verify every required field is non-None after generation."""
        event = generate_event()
        assert event.event_id is not None
        assert event.event_type is not None
        assert event.user_id is not None
        assert event.session_id is not None
        assert event.page is not None
        assert event.timestamp is not None

    def test_generate_event_user_id_from_valid_pool(self) -> None:
        """Verify generated user_id comes from the predefined USER_IDS pool."""
        event = generate_event()
        assert event.user_id in USER_IDS

    def test_generate_event_session_id_from_valid_pool(self) -> None:
        """Verify generated session_id comes from the predefined SESSION_IDS pool."""
        event = generate_event()
        assert event.session_id in SESSION_IDS

    def test_generate_event_page_from_valid_pool(self) -> None:
        """Verify generated page comes from the predefined PAGES pool."""
        event = generate_event()
        assert event.page in PAGES

    def test_generate_event_referrer_may_be_none(self) -> None:
        """Verify referrer can be None (direct traffic simulation)."""
        # Generate multiple events to encounter None referrers
        referrers = {generate_event().referrer for _ in range(100)}
        assert None in referrers

    def test_generate_event_event_type_is_valid(self) -> None:
        """Verify generated event_type is a valid EventType enum member."""
        event = generate_event()
        assert isinstance(event.event_type, EventType)
        assert event.event_type in EventType

    def test_generate_event_metadata_contains_template_keys(self) -> None:
        """Verify metadata for each event type contains keys from its template."""
        type_counts: Counter[EventType] = Counter()
        for _ in range(500):
            event = generate_event()
            template = METADATA_TEMPLATES.get(event.event_type, {})
            for key in template:
                assert key in event.metadata, f"Missing metadata key '{key}' for {event.event_type}"
            type_counts[event.event_type] += 1
        # Verify all event types were generated
        assert len(type_counts) == len(EventType), f"Not all event types generated: {type_counts}"


class TestGenerateOrder:
    """Tests for the generate_order producer function."""

    def test_generate_order_returns_valid_order(self) -> None:
        """Verify generate_order returns a valid Order model instance."""
        order = generate_order()
        assert isinstance(order, Order)

    def test_generate_order_has_all_required_fields(self) -> None:
        """Verify every required field is populated after generation."""
        order = generate_order()
        assert order.order_id is not None
        assert order.user_id is not None
        assert order.items is not None
        assert order.total_amount > 0
        assert order.currency is not None
        assert order.status is not None

    def test_generate_order_total_matches_items_sum(self) -> None:
        """Verify calculated total_amount matches sum of item quantity * unit_price."""
        order = generate_order()
        expected_total = round(sum(it.quantity * it.unit_price for it in order.items), 2)
        assert order.total_amount == expected_total

    def test_generate_order_has_between_one_and_four_items(self) -> None:
        """Verify order contains between 1 and 4 line items."""
        order = generate_order()
        assert 1 <= len(order.items) <= 4

    def test_generate_order_each_item_has_positive_quantity(self) -> None:
        """Verify every OrderItem has a positive quantity."""
        order = generate_order()
        for item in order.items:
            assert item.quantity > 0

    def test_generate_order_user_id_from_valid_pool(self) -> None:
        """Verify generated user_id comes from the predefined USER_IDS pool."""
        order = generate_order()
        assert order.user_id in USER_IDS

    def test_generate_order_currency_is_usd_or_eur(self) -> None:
        """Verify currency is one of the allowed values."""
        order = generate_order()
        assert order.currency in {"USD", "EUR"}

    def test_generate_order_status_is_valid(self) -> None:
        """Verify status is one of the allowed values."""
        order = generate_order()
        assert order.status in {"pending", "confirmed", "shipped"}

    def test_generate_order_product_ids_from_valid_pool(self) -> None:
        """Verify all item product_ids come from the PRODUCTS pool."""
        order = generate_order()
        valid_ids = {p[0] for p in PRODUCTS}
        for item in order.items:
            assert item.product_id in valid_ids
