from __future__ import annotations

import time
from datetime import date
from typing import Any

import duckdb
import polars as pl

from gold.config import settings


class AnalyticsEngine:
    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn
        self._cache: dict[str, tuple[float, pl.DataFrame]] = {}

    def _query(self, sql: str) -> pl.DataFrame:
        return self._conn.sql(sql).pl()

    def _cache_get(self, key: str) -> pl.DataFrame | None:
        if key in self._cache:
            cached_at, data = self._cache[key]
            if time.monotonic() - cached_at < settings.cache_ttl_seconds:
                return data
            del self._cache[key]
        return None

    def _cache_set(self, key: str, data: pl.DataFrame) -> None:
        self._cache[key] = (time.monotonic(), data)

    def daily_active_users(self, date_from: date, date_to: date) -> pl.DataFrame:
        cache_key = f"dau:{date_from}:{date_to}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        result = self._query(f"""
            SELECT
                event_date,
                SUM(daily_active_users) AS active_users,
                SUM(total_events) AS total_events,
                AVG(conversion_rate) AS avg_conversion_rate
            FROM main_gold.daily_kpis
            WHERE event_date BETWEEN '{date_from}' AND '{date_to}'
            GROUP BY event_date
            ORDER BY event_date
        """)
        self._cache_set(cache_key, result)
        return result

    def top_products(self, n: int = 10) -> pl.DataFrame:
        cache_key = f"top_products:{n}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        result = self._query(f"""
            SELECT
                json_extract_string(item, '$.product_id') AS product_id,
                json_extract_string(item, '$.product_name') AS product_name,
                SUM(
                    CAST(json_extract_string(item, '$.quantity') AS INTEGER)
                    * CAST(json_extract_string(item, '$.unit_price') AS DOUBLE)
                ) AS total_revenue,
                COUNT(DISTINCT o.order_id) AS order_count
            FROM main_silver.validate_orders o
            JOIN bronze.orders b ON o.order_id = b.order_id,
            UNNEST(json_transform(b.items, '["JSON"]')) AS t(item)
            WHERE o.status != 'validation_failed'
            GROUP BY product_id, product_name
            ORDER BY total_revenue DESC
            LIMIT {n}
        """)
        self._cache_set(cache_key, result)
        return result

    def user_segment_report(self) -> pl.DataFrame:
        cache_key = "user_segment_report"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        result = self._query("""
            SELECT
                CASE
                    WHEN days_since_last_activity <= 7 AND total_spend > 500 THEN 'Champions'
                    WHEN days_since_last_activity <= 7 AND valid_orders >= 3 THEN 'Loyal'
                    WHEN days_since_last_activity <= 14 THEN 'Recent'
                    WHEN days_since_last_activity <= 30 AND total_spend > 200 THEN 'Good'
                    WHEN days_since_last_activity <= 60 THEN 'At Risk'
                    ELSE 'Dormant'
                END AS segment_name,
                COUNT(*) AS user_count,
                ROUND(AVG(COALESCE(total_spend, 0)), 2) AS avg_spend,
                CAST(ROUND(AVG(COALESCE(days_since_last_activity, 999)), 0) AS INTEGER) AS avg_recency_days
            FROM main_gold.customer_360
            GROUP BY segment_name
            ORDER BY segment_name
        """)
        self._cache_set(cache_key, result)
        return result

    def conversion_funnel(self, date_from: date, date_to: date) -> pl.DataFrame:
        cache_key = f"funnel:{date_from}:{date_to}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        result = self._query(f"""
            SELECT
                event_type AS step_name,
                COUNT(DISTINCT user_id) AS users,
                COUNT(*) AS count
            FROM main_silver.clean_events
            WHERE event_timestamp >= '{date_from}'
              AND event_timestamp < CAST('{date_to}' AS DATE) + INTERVAL '1 day'
              AND event_type IN ('page_view', 'click', 'purchase')
            GROUP BY event_type
            ORDER BY CASE event_type
                WHEN 'page_view' THEN 1
                WHEN 'click' THEN 2
                WHEN 'purchase' THEN 3
            END
        """)
        self._cache_set(cache_key, result)
        return result

    def anomaly_detection(self, metric: str, lookback_days: int = 30) -> pl.DataFrame:
        cache_key = f"anomaly:{metric}:{lookback_days}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        value_expr = {
            "dau": "SUM(daily_active_users)",
            "conversion": "AVG(conversion_rate)",
            "events": "SUM(total_events)",
        }.get(metric, f"SUM(CAST({metric} AS DOUBLE))")

        result = self._query(f"""
            WITH daily_metrics AS (
                SELECT
                    event_date AS date,
                    {value_expr} AS value
                FROM main_gold.daily_kpis
                WHERE event_date >= CURRENT_DATE - INTERVAL '{lookback_days} days'
                GROUP BY event_date
            ),
            stats AS (
                SELECT
                    AVG(value) AS mean,
                    COALESCE(STDDEV_SAMP(value), 0) AS stddev
                FROM daily_metrics
            )
            SELECT
                '{metric}' AS metric,
                dm.date,
                dm.value,
                CASE
                    WHEN stats.stddev > 0
                    THEN ROUND((dm.value - stats.mean) / stats.stddev, 3)
                    ELSE 0
                END AS z_score,
                CASE
                    WHEN stats.stddev > 0
                        AND ABS((dm.value - stats.mean) / stats.stddev) > 2.0
                    THEN TRUE
                    ELSE FALSE
                END AS is_anomaly
            FROM daily_metrics dm, stats
            ORDER BY dm.date
        """)
        self._cache_set(cache_key, result)
        return result
