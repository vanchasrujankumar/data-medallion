from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import duckdb
import polars as pl
import pytest
from fastapi.testclient import TestClient

from bronze.models import Event, EventType, Order, OrderItem
from gold.analytics import AnalyticsEngine


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="Run integration tests (require Redpanda + MinIO)",
    )


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "integration: mark test as integration (requires external services)",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if not config.getoption("--run-integration"):
        skip_marker = pytest.mark.skip(reason="need --run-integration option to run")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_marker)


@pytest.fixture
def test_settings(tmp_path: Path) -> dict[str, str]:
    """Settings override dict pointing to a test DuckDB path in tmp_path."""
    return {
        "duckdb_path": str(tmp_path / "test_medallion.duckdb"),
        "redpanda_bootstrap_servers": "localhost:9092",
        "minio_endpoint": "localhost:9000",
        "minio_access_key": "minioadmin",
        "minio_secret_key": "minioadmin",
        "minio_bucket": "test-medallion",
        "iceberg_warehouse": "s3://test-medallion/iceberg/",
        "topic_events": "test.raw.events",
        "topic_orders": "test.raw.orders",
    }


@pytest.fixture
def sample_event() -> Event:
    """Create a valid Event model instance with all fields populated."""
    return Event(
        event_type=EventType.page_view,
        user_id="user_0001",
        session_id="sess_000001",
        page="/home",
        referrer="https://google.com",
        metadata={"browser": "Chrome", "os": "macOS"},
    )


@pytest.fixture
def sample_order() -> Order:
    """Create a valid Order model instance with 2 items."""
    items = [
        OrderItem(
            product_id="prod_001",
            product_name="Wireless Mouse",
            quantity=2,
            unit_price=29.99,
        ),
        OrderItem(
            product_id="prod_002",
            product_name="Mechanical Keyboard",
            quantity=1,
            unit_price=89.99,
        ),
    ]
    total = round(sum(it.quantity * it.unit_price for it in items), 2)
    return Order(
        user_id="user_0001",
        items=items,
        total_amount=total,
        currency="USD",
        status="pending",
        shipping_address="123 Main St, New York, NY 10001",
    )


@pytest.fixture
def invalid_event() -> dict[str, Any]:
    """Event data dict missing required fields to trigger validation errors."""
    return {
        "user_id": "user_0001",
        "session_id": "sess_000001",
    }


@pytest.fixture
def duckdb_connection() -> duckdb.DuckDBPyConnection:
    """Create a temporary in-memory DuckDB connection."""
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def analytics_engine(duckdb_connection: duckdb.DuckDBPyConnection) -> AnalyticsEngine:
    """Create an AnalyticsEngine with test connection."""
    return AnalyticsEngine(duckdb_connection)


@pytest.fixture
def test_client() -> TestClient:
    """Create a FastAPI TestClient with mocked database and engine dependencies."""
    mock_conn = MagicMock()

    with patch("duckdb.connect", return_value=mock_conn):
        from gold.serve import app, get_engine

        mock_engine = MagicMock(spec=AnalyticsEngine)

        mock_engine.daily_active_users.return_value = pl.DataFrame(
            {
                "event_date": [datetime(2024, 1, 1).date()],
                "active_users": [100],
                "total_events": [500],
                "avg_conversion_rate": [0.05],
            }
        )

        mock_engine.top_products.return_value = pl.DataFrame(
            {
                "product_id": ["prod_001"],
                "product_name": ["Wireless Mouse"],
                "total_revenue": [2999.0],
                "order_count": [100],
            }
        )

        mock_engine.user_segment_report.return_value = pl.DataFrame(
            {
                "segment_name": ["Champions"],
                "user_count": [50],
                "avg_spend": [750.50],
                "avg_recency_days": [3],
            }
        )

        mock_engine.conversion_funnel.return_value = pl.DataFrame(
            {
                "step_name": ["page_view"],
                "users": [1000],
                "count": [5000],
            }
        )

        mock_engine.anomaly_detection.return_value = pl.DataFrame(
            {
                "metric": ["dau"],
                "date": [datetime(2024, 1, 1).date()],
                "value": [150.0],
                "z_score": [1.5],
                "is_anomaly": [False],
            }
        )

        async def override_get_engine() -> AnalyticsEngine:
            return mock_engine

        app.dependency_overrides[get_engine] = override_get_engine

        with TestClient(app) as client:
            yield client

        app.dependency_overrides.clear()
