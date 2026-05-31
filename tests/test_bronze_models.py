from __future__ import annotations

import json
from datetime import datetime
from uuid import UUID

import pytest
from pydantic import ValidationError

from bronze.models import Event, EventType, Order, OrderItem


class TestEventModel:
    """Tests for the Bronze Event Pydantic model."""

    def test_event_creation_with_all_field_types(self, sample_event: Event) -> None:
        """Verify an Event can be created with all field types populated correctly."""
        event = sample_event
        assert isinstance(event.event_id, str)
        assert isinstance(event.event_type, EventType)
        assert isinstance(event.user_id, str)
        assert isinstance(event.session_id, str)
        assert isinstance(event.page, str)
        assert isinstance(event.referrer, str)
        assert isinstance(event.metadata, dict)
        assert isinstance(event.timestamp, datetime)

    def test_event_serialization_to_json(self, sample_event: Event) -> None:
        """Verify Event serializes to JSON-compatible dict via model_dump(mode='json')."""
        data = sample_event.model_dump(mode="json")
        assert isinstance(data, dict)
        assert data["event_type"] == "page_view"
        assert data["user_id"] == "user_0001"
        assert data["page"] == "/home"
        assert isinstance(data["timestamp"], str)
        # Ensure the JSON output is actually serializable
        json_str = json.dumps(data)
        parsed = json.loads(json_str)
        assert parsed["event_type"] == "page_view"

    def test_event_deserialization_from_json(self, sample_event: Event) -> None:
        """Verify an Event round-trips through JSON serialization/deserialization."""
        data = sample_event.model_dump(mode="json")
        restored = Event(**data)
        assert restored.event_id == sample_event.event_id
        assert restored.event_type == sample_event.event_type
        assert restored.user_id == sample_event.user_id
        assert restored.page == sample_event.page

    def test_event_type_enum_has_all_expected_values(self) -> None:
        """Verify EventType enum has all expected event types."""
        expected = {"page_view", "click", "purchase", "signup", "error"}
        actual = {e.value for e in EventType}
        assert actual == expected

    def test_event_auto_generates_event_id_and_timestamp(self) -> None:
        """Verify Event auto-generates event_id (UUID) and timestamp when not provided."""
        event = Event(
            event_type=EventType.click,
            user_id="user_0001",
            session_id="sess_000001",
            page="/cart",
        )
        assert event.event_id is not None
        assert isinstance(event.event_id, str)
        # Verify it looks like a UUID
        UUID(event.event_id)
        assert event.timestamp is not None
        assert isinstance(event.timestamp, datetime)

    def test_invalid_event_type_raises_validation_error(self) -> None:
        """Verify creating an Event with an invalid EventType string raises ValidationError."""
        with pytest.raises(ValidationError):
            Event(
                event_type="invalid_type",  # type: ignore[arg-type]
                user_id="user_0001",
                session_id="sess_000001",
                page="/home",
            )

    def test_event_creation_without_event_id(self) -> None:
        """Verify creating an Event without event_id auto-generates one."""
        event = Event(
            event_type=EventType.signup,
            user_id="user_0001",
            session_id="sess_000001",
            page="/signup",
        )
        assert event.event_id is not None
        assert len(event.event_id) > 0


class TestOrderModel:
    """Tests for the Bronze Order Pydantic model."""

    def test_order_creation_with_items(self) -> None:
        """Verify an Order can be created with multiple OrderItems."""
        items = [
            OrderItem(
                product_id="prod_001", product_name="Wireless Mouse",
                quantity=2, unit_price=29.99,
            ),
            OrderItem(
                product_id="prod_002", product_name="Mechanical Keyboard",
                quantity=1, unit_price=89.99,
            ),
        ]
        order = Order(
            user_id="user_0001",
            items=items,
            total_amount=149.97,
            currency="USD",
            status="pending",
        )
        assert len(order.items) == 2
        assert order.total_amount == 149.97

    def test_order_total_amount_matches_item_totals(self, sample_order: Order) -> None:
        """Verify the total_amount field matches the computed sum of item totals."""
        items_total = round(sum(it.quantity * it.unit_price for it in sample_order.items), 2)
        assert sample_order.total_amount == items_total

    def test_empty_items_list_is_allowed(self) -> None:
        """Verify an Order with empty items list is valid (downstream validates)."""
        order = Order(user_id="user_0001", items=[], total_amount=10.0)
        assert len(order.items) == 0

    def test_negative_quantity_raises_validation_error(self) -> None:
        """Verify OrderItem with negative quantity raises ValidationError (Field(gt=0))."""
        with pytest.raises(ValidationError):
            OrderItem(
                product_id="prod_001", product_name="Wireless Mouse",
                quantity=-1, unit_price=29.99,
            )

    def test_zero_quantity_raises_validation_error(self) -> None:
        """Verify zero quantity raises ValidationError (Field(gt=0))."""
        with pytest.raises(ValidationError):
            OrderItem(
                product_id="prod_001", product_name="Wireless Mouse",
                quantity=0, unit_price=29.99,
            )

    def test_negative_unit_price_raises_validation_error(self) -> None:
        """Verify negative unit_price raises ValidationError (Field(gt=0))."""
        with pytest.raises(ValidationError):
            OrderItem(
                product_id="prod_001", product_name="Wireless Mouse",
                quantity=1, unit_price=-10.0,
            )

    def test_order_defaults_currency_and_status(self) -> None:
        """Verify Order sets default currency='USD' and status='pending'."""
        items = [
            OrderItem(
                product_id="prod_001", product_name="Wireless Mouse",
                quantity=1, unit_price=29.99,
            ),
        ]
        order = Order(user_id="user_0001", items=items, total_amount=29.99)
        assert order.currency == "USD"
        assert order.status == "pending"
