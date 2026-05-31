from __future__ import annotations

from datetime import date, datetime, timedelta
from uuid import uuid4

import duckdb
import pytest

from gold.analytics import AnalyticsEngine


@pytest.fixture
def integration_duckdb() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with real test data for integration testing."""
    conn = duckdb.connect(":memory:")

    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    conn.execute("CREATE SCHEMA IF NOT EXISTS main_silver")
    conn.execute("CREATE SCHEMA IF NOT EXISTS main_gold")

    conn.execute(
        """
        CREATE TABLE bronze.events (
            event_id      VARCHAR,
            event_type    VARCHAR,
            user_id       VARCHAR,
            session_id    VARCHAR,
            page          VARCHAR,
            referrer      VARCHAR,
            metadata      VARCHAR,
            "timestamp"   TIMESTAMP
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE bronze.orders (
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

    base_time = datetime(2024, 1, 1, 12, 0, 0)
    users = [f"user_{i:04d}" for i in range(1, 6)]
    sessions = [f"sess_{i:06d}" for i in range(1, 11)]
    event_types = ["page_view", "click", "purchase", "signup", "error"]
    pages = ["/home", "/products", "/cart", "/checkout", "/about"]

    for day_offset in range(7):
        ts = base_time + timedelta(days=day_offset)
        for user_id in users:
            for _ in range(5):
                conn.execute(
                    """
                    INSERT INTO bronze.events
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        str(uuid4()),
                        event_types[_ % len(event_types)],
                        user_id,
                        sessions[hash(user_id + str(ts)) % len(sessions)],
                        pages[_ % len(pages)],
                        "https://google.com" if _ % 3 == 0 else None,
                        '{"browser": "Chrome"}',
                        ts.isoformat(),
                    ],
                )

    conn.execute(
        """
        INSERT INTO bronze.orders VALUES
        ('ord_001', 'user_0001',
         '{"product_id":"prod_001","product_name":"Wireless Mouse",'
         '"quantity":2,"unit_price":29.99}',
         59.98, 'USD', 'pending', '123 Main St', '2024-01-01'::TIMESTAMP)
        """
    )
    conn.execute(
        """
        INSERT INTO bronze.orders VALUES
         ('ord_002', 'user_0002',
         '{"product_id":"prod_002","product_name":"Mechanical Keyboard",'
         '"quantity":1,"unit_price":89.99}',
         89.99, 'USD', 'confirmed', '456 Oak St', '2024-01-02'::TIMESTAMP)
        """
    )
    conn.execute(
        """
        INSERT INTO bronze.orders VALUES
        ('ord_003', 'user_0001',
         '[{"product_id":"prod_003","product_name":"USB-C Hub","quantity":1,"unit_price":49.99}]',
         49.99, 'EUR', 'shipped', '123 Main St', '2024-01-03'::TIMESTAMP)
        """
    )

    conn.execute(
        """
        CREATE TABLE main_silver.clean_events AS
        SELECT
            MD5(event_id) AS event_key,
            event_id,
            user_id,
            session_id,
            event_type,
            page,
            referrer,
            "timestamp" AS event_timestamp,
            TRY_CAST(metadata AS JSON) AS metadata_json,
            CURRENT_TIMESTAMP AS ingested_at
        FROM bronze.events
        WHERE event_type IN ('page_view', 'click', 'purchase', 'signup', 'error')
        """
    )

    conn.execute(
        """
        CREATE TABLE main_silver.validate_orders AS
        SELECT
            MD5(order_id) AS order_key,
            order_id,
            user_id,
            total_amount,
            total_amount AS validated_total,
            currency,
            created_at,
            CURRENT_TIMESTAMP AS validated_at,
            status,
            shipping_address
        FROM bronze.orders
        """
    )

    conn.execute(
        """
        CREATE TABLE main_gold.daily_kpis AS
        SELECT
            CAST(event_timestamp AS DATE) AS event_date,
            event_type,
            COUNT(DISTINCT user_id) AS daily_active_users,
            COUNT(*) AS total_events,
            0.0 AS conversion_rate
        FROM main_silver.clean_events
        GROUP BY 1, 2
        """
    )

    conn.execute(
        """
        CREATE TABLE main_gold.customer_360 AS
        SELECT
            user_id,
            MIN(event_timestamp) AS first_seen,
            MAX(event_timestamp) AS last_seen,
            CAST(0 AS INTEGER) AS days_since_last_activity,
            CAST(0 AS BIGINT) AS total_orders,
            CAST(0 AS BIGINT) AS valid_orders,
            0.0 AS total_spend,
            0.0 AS avg_order_value,
            MAX(event_type) AS most_common_event_type,
            COUNT(*) AS total_events
        FROM main_silver.clean_events
        GROUP BY user_id
        """
    )

    yield conn
    conn.close()


@pytest.fixture
def integration_engine(integration_duckdb: duckdb.DuckDBPyConnection) -> AnalyticsEngine:
    """Create an AnalyticsEngine backed by the integration test database."""
    return AnalyticsEngine(integration_duckdb)


@pytest.mark.integration
class TestIntegrationDataPipeline:
    """Integration smoke tests that exercise the full data pipeline locally."""

    def test_bronze_events_have_data(self, integration_duckdb: duckdb.DuckDBPyConnection) -> None:
        """Verify the bronze.events table has been populated with test data."""
        count = integration_duckdb.execute("SELECT COUNT(*) FROM bronze.events").fetchone()[0]
        assert count > 0, "bronze.events should contain test data"

    def test_bronze_orders_have_data(self, integration_duckdb: duckdb.DuckDBPyConnection) -> None:
        """Verify the bronze.orders table has been populated with test data."""
        count = integration_duckdb.execute("SELECT COUNT(*) FROM bronze.orders").fetchone()[0]
        assert count > 0, "bronze.orders should contain test data"

    def test_silver_clean_events_exists(
        self, integration_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """Verify main_silver.clean_events view is queryable."""
        count = integration_duckdb.execute(
            "SELECT COUNT(*) FROM main_silver.clean_events"
        ).fetchone()[0]
        assert count > 0

    def test_silver_validate_orders_exists(
        self, integration_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """Verify main_silver.validate_orders view is queryable."""
        count = integration_duckdb.execute(
            "SELECT COUNT(*) FROM main_silver.validate_orders"
        ).fetchone()[0]
        assert count > 0

    def test_gold_daily_kpis_returns_rows(self, integration_engine: AnalyticsEngine) -> None:
        """Verify daily_active_users returns data for the test date range."""
        result = integration_engine.daily_active_users(date(2024, 1, 1), date(2024, 1, 7))
        assert len(result) > 0
        expected_columns = {"event_date", "active_users", "total_events", "avg_conversion_rate"}
        assert set(result.columns) == expected_columns

    def test_gold_top_products_returns_rows(self, integration_engine: AnalyticsEngine) -> None:
        """Verify top_products returns data from the test orders."""
        result = integration_engine.top_products(n=10)
        assert len(result) > 0
        expected_columns = {"product_id", "product_name", "total_revenue", "order_count"}
        assert set(result.columns) == expected_columns

    def test_gold_user_segments_returns_rows(self, integration_engine: AnalyticsEngine) -> None:
        """Verify user_segment_report returns segment data."""
        result = integration_engine.user_segment_report()
        assert len(result) > 0
        expected_columns = {"segment_name", "user_count", "avg_spend", "avg_recency_days"}
        assert set(result.columns) == expected_columns

    def test_gold_conversion_funnel_returns_rows(self, integration_engine: AnalyticsEngine) -> None:
        """Verify conversion_funnel returns funnel step data."""
        result = integration_engine.conversion_funnel(date(2024, 1, 1), date(2024, 1, 7))
        assert len(result) > 0
        expected_columns = {"step_name", "users", "count"}
        assert set(result.columns) == expected_columns

    def test_gold_anomaly_detection_returns_rows(self, integration_engine: AnalyticsEngine) -> None:
        """Verify anomaly_detection returns results for dau metric."""
        result = integration_engine.anomaly_detection(metric="dau", lookback_days=30)
        assert len(result) > 0
        expected_columns = {"metric", "date", "value", "z_score", "is_anomaly"}
        assert set(result.columns) == expected_columns
