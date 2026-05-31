from __future__ import annotations

from datetime import date
from unittest.mock import patch

import polars as pl

from gold.analytics import AnalyticsEngine


class TestAnalyticsEngineInit:
    """Tests for AnalyticsEngine instantiation."""

    def test_analytics_engine_can_be_instantiated(self, analytics_engine: AnalyticsEngine) -> None:
        """Verify AnalyticsEngine can be created with a DuckDB connection."""
        assert isinstance(analytics_engine, AnalyticsEngine)
        assert analytics_engine._conn is not None
        assert analytics_engine._cache == {}


class TestAnalyticsEngineCache:
    """Tests for AnalyticsEngine caching behavior."""

    def test_analytics_engine_cache_works(self, analytics_engine: AnalyticsEngine) -> None:
        """Verify calling same method twice returns cached result on second call."""
        mock_data = pl.DataFrame({
            "event_date": [date(2024, 1, 1)],
            "active_users": [100],
            "total_events": [500],
            "avg_conversion_rate": [0.05],
        })

        with patch.object(
            analytics_engine, "_query", return_value=mock_data
        ) as mock_query:
            result1 = analytics_engine.daily_active_users(
                date(2024, 1, 1), date(2024, 1, 31)
            )
            result2 = analytics_engine.daily_active_users(
                date(2024, 1, 1), date(2024, 1, 31)
            )

            mock_query.assert_called_once()
            assert result1 is result2

    def test_analytics_engine_cache_key_differs_by_arguments(
        self, analytics_engine: AnalyticsEngine
    ) -> None:
        """Verify different arguments produce different cache keys and call _query twice."""
        mock_data = pl.DataFrame({
            "event_date": [date(2024, 1, 1)],
            "active_users": [100],
            "total_events": [500],
            "avg_conversion_rate": [0.05],
        })

        with patch.object(
            analytics_engine, "_query", return_value=mock_data
        ) as mock_query:
            analytics_engine.daily_active_users(
                date(2024, 1, 1), date(2024, 1, 7)
            )
            analytics_engine.daily_active_users(
                date(2024, 1, 8), date(2024, 1, 14)
            )

            assert mock_query.call_count == 2


class TestDailyActiveUsers:
    """Tests for the daily_active_users analytics method."""

    def test_daily_active_users_returns_expected_schema(
        self, analytics_engine: AnalyticsEngine
    ) -> None:
        """Verify daily_active_users returns DataFrame with correct columns."""
        mock_data = pl.DataFrame(
            {
                "event_date": [date(2024, 1, 1), date(2024, 1, 2)],
                "active_users": [100, 120],
                "total_events": [500, 600],
                "avg_conversion_rate": [0.05, 0.04],
            }
        )

        with patch.object(analytics_engine, "_query", return_value=mock_data):
            result = analytics_engine.daily_active_users(
                date(2024, 1, 1), date(2024, 1, 31)
            )

            expected = {"event_date", "active_users", "total_events",
                        "avg_conversion_rate"}
            assert set(result.columns) == expected
            assert len(result) == 2

    def test_daily_active_users_returns_empty_for_no_data(
        self, analytics_engine: AnalyticsEngine
    ) -> None:
        """Verify daily_active_users handles empty result set."""
        schema = {
            "event_date": pl.Date,
            "active_users": pl.Int64,
            "total_events": pl.Int64,
            "avg_conversion_rate": pl.Float64,
        }
        mock_data = pl.DataFrame(schema=schema)

        with patch.object(analytics_engine, "_query", return_value=mock_data):
            result = analytics_engine.daily_active_users(
                date(2024, 1, 1), date(2024, 1, 31)
            )

            assert len(result) == 0


class TestTopProducts:
    """Tests for the top_products analytics method."""

    def test_top_products_returns_expected_columns(
        self, analytics_engine: AnalyticsEngine
    ) -> None:
        """Verify top_products returns expected columns."""
        mock_data = pl.DataFrame(
            {
                "product_id": ["prod_001", "prod_002"],
                "product_name": ["Wireless Mouse", "Mechanical Keyboard"],
                "total_revenue": [2999.0, 8999.0],
                "order_count": [100, 100],
            }
        )

        with patch.object(analytics_engine, "_query", return_value=mock_data):
            result = analytics_engine.top_products(n=5)

            expected = {"product_id", "product_name",
                        "total_revenue", "order_count"}
            assert set(result.columns) == expected

    def test_top_products_respects_limit(self, analytics_engine: AnalyticsEngine) -> None:
        """Verify top_products passes the n parameter as SQL LIMIT."""
        mock_data = pl.DataFrame(
            {
                "product_id": ["prod_001"],
                "product_name": ["Wireless Mouse"],
                "total_revenue": [2999.0],
                "order_count": [100],
            }
        )

        with patch.object(analytics_engine, "_query", return_value=mock_data) as mock_query:
            result = analytics_engine.top_products(n=1)

            assert len(result) == 1
            call_sql = mock_query.call_args[0][0]
            assert "LIMIT 1" in call_sql


class TestUserSegmentReport:
    """Tests for the user_segment_report analytics method."""

    def test_user_segment_report_returns_expected_segments(
        self, analytics_engine: AnalyticsEngine
    ) -> None:
        """Verify user_segment_report returns DataFrame with segment columns."""
        mock_data = pl.DataFrame(
            {
                "segment_name": ["Champions", "Loyal", "Recent", "At Risk", "Dormant"],
                "user_count": [10, 25, 30, 20, 15],
                "avg_spend": [1000.0, 450.0, 150.0, 100.0, 0.0],
                "avg_recency_days": [2, 5, 10, 45, 120],
            }
        )

        with patch.object(analytics_engine, "_query", return_value=mock_data):
            result = analytics_engine.user_segment_report()

            expected = {"segment_name", "user_count",
                        "avg_spend", "avg_recency_days"}
            assert set(result.columns) == expected
            assert len(result) == 5


class TestConversionFunnel:
    """Tests for the conversion_funnel analytics method."""

    def test_conversion_funnel_returns_expected_funnel_steps(
        self, analytics_engine: AnalyticsEngine
    ) -> None:
        """Verify conversion_funnel returns DataFrame with step_name, users, count."""
        mock_data = pl.DataFrame(
            {
                "step_name": ["page_view", "click", "purchase"],
                "users": [1000, 500, 50],
                "count": [5000, 800, 50],
            }
        )

        with patch.object(analytics_engine, "_query", return_value=mock_data):
            result = analytics_engine.conversion_funnel(
                date(2024, 1, 1), date(2024, 1, 31)
            )

            expected = {"step_name", "users", "count"}
            assert set(result.columns) == expected
            assert len(result) == 3

    def test_conversion_funnel_steps_in_correct_order(
        self, analytics_engine: AnalyticsEngine
    ) -> None:
        """Verify conversion funnel returns steps ordered correctly."""
        mock_data = pl.DataFrame(
            {
                "step_name": ["page_view", "click", "purchase"],
                "users": [1000, 500, 50],
                "count": [5000, 800, 50],
            }
        )

        with patch.object(analytics_engine, "_query", return_value=mock_data):
            result = analytics_engine.conversion_funnel(
                date(2024, 1, 1), date(2024, 1, 31)
            )

            assert result["step_name"].to_list() == [
                "page_view", "click", "purchase"
            ]


class TestAnomalyDetection:
    """Tests for the anomaly_detection analytics method."""

    def test_anomaly_detection_returns_expected_fields(
        self, analytics_engine: AnalyticsEngine
    ) -> None:
        """Verify anomaly_detection returns expected fields."""
        mock_data = pl.DataFrame(
            {
                "metric": ["dau"],
                "date": [date(2024, 1, 1)],
                "value": [150.0],
                "z_score": [1.5],
                "is_anomaly": [False],
            }
        )

        with patch.object(analytics_engine, "_query", return_value=mock_data):
            result = analytics_engine.anomaly_detection(
                metric="dau", lookback_days=30
            )

            expected = {"metric", "date", "value", "z_score", "is_anomaly"}
            assert set(result.columns) == expected

    def test_anomaly_detection_detects_anomalies(self, analytics_engine: AnalyticsEngine) -> None:
        """Verify anomaly_detection correctly flags entries with is_anomaly=True."""
        mock_data = pl.DataFrame(
            {
                "metric": ["dau", "dau"],
                "date": [date(2024, 1, 1), date(2024, 1, 2)],
                "value": [150.0, 1500.0],
                "z_score": [0.5, 3.2],
                "is_anomaly": [False, True],
            }
        )

        with patch.object(analytics_engine, "_query", return_value=mock_data):
            result = analytics_engine.anomaly_detection(metric="dau", lookback_days=30)

            anomalies = result.filter(pl.col("is_anomaly") == True)  # noqa: E712
            assert len(anomalies) == 1
            assert anomalies["z_score"].to_list() == [3.2]
