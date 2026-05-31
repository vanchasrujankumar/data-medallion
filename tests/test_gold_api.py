from __future__ import annotations

from fastapi.testclient import TestClient


class TestHealth:
    """Tests for the /health endpoint."""

    def test_get_health_returns_200_with_status_healthy(self, test_client: TestClient) -> None:
        """Verify GET /health returns 200 with {'status': 'healthy'}."""
        response = test_client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "healthy"}


class TestDauEndpoint:
    """Tests for the GET /api/v1/kpis/dau endpoint."""

    def test_get_dau_returns_expected_schema(self, test_client: TestClient) -> None:
        """Verify GET /api/v1/kpis/dau returns DAUResponse with expected fields."""
        response = test_client.get("/api/v1/kpis/dau?date_from=2024-01-01&date_to=2024-01-31")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "total_active_users" in data
        assert "date_from" in data
        assert "date_to" in data
        assert isinstance(data["data"], list)
        assert data["date_from"] == "2024-01-01"
        assert data["date_to"] == "2024-01-31"

    def test_get_dau_with_invalid_date_returns_422(self, test_client: TestClient) -> None:
        """Verify GET /api/v1/kpis/dau without required query params returns 422."""
        response = test_client.get("/api/v1/kpis/dau")
        assert response.status_code == 422

    def test_get_dau_with_bad_date_format_returns_422(self, test_client: TestClient) -> None:
        """Verify GET /api/v1/kpis/dau with non-date string returns 422."""
        response = test_client.get("/api/v1/kpis/dau?date_from=not-a-date&date_to=2024-01-31")
        assert response.status_code == 422


class TestTopProductsEndpoint:
    """Tests for the GET /api/v1/products/top endpoint."""

    def test_get_top_products_returns_expected_schema(self, test_client: TestClient) -> None:
        """Verify GET /api/v1/products/top returns TopProductsResponse with products and total."""
        response = test_client.get("/api/v1/products/top")
        assert response.status_code == 200
        data = response.json()
        assert "products" in data
        assert "total" in data
        assert isinstance(data["products"], list)
        assert data["total"] >= 0

    def test_get_top_products_with_custom_limit(self, test_client: TestClient) -> None:
        """Verify GET /api/v1/products/top accepts n parameter."""
        response = test_client.get("/api/v1/products/top?n=5")
        assert response.status_code == 200

    def test_get_top_products_with_invalid_limit_returns_422(self, test_client: TestClient) -> None:
        """Verify GET /api/v1/products/top with n=0 returns 422 (ge=1)."""
        response = test_client.get("/api/v1/products/top?n=0")
        assert response.status_code == 422


class TestUserSegmentsEndpoint:
    """Tests for the GET /api/v1/users/segments endpoint."""

    def test_get_user_segments_returns_expected_schema(self, test_client: TestClient) -> None:
        """Verify GET /api/v1/users/segments returns SegmentReport with segments and total_users."""
        response = test_client.get("/api/v1/users/segments")
        assert response.status_code == 200
        data = response.json()
        assert "segments" in data
        assert "total_users" in data
        assert isinstance(data["segments"], list)


class TestConversionFunnelEndpoint:
    """Tests for the GET /api/v1/funnel/conversion endpoint."""

    def test_get_conversion_funnel_returns_expected_schema(self, test_client: TestClient) -> None:
        """Verify GET /api/v1/funnel/conversion returns FunnelResponse."""
        response = test_client.get(
            "/api/v1/funnel/conversion?date_from=2024-01-01&date_to=2024-01-31"
        )
        assert response.status_code == 200
        data = response.json()
        assert "funnel" in data
        assert "date_from" in data
        assert "date_to" in data
        assert isinstance(data["funnel"], list)

    def test_get_conversion_funnel_without_dates_returns_422(self, test_client: TestClient) -> None:
        """Verify GET /api/v1/funnel/conversion without date params returns 422."""
        response = test_client.get("/api/v1/funnel/conversion")
        assert response.status_code == 422


class TestAnomaliesEndpoint:
    """Tests for the GET /api/v1/anomalies endpoint."""

    def test_get_anomalies_returns_expected_schema(self, test_client: TestClient) -> None:
        """Verify GET /api/v1/anomalies returns AnomalyResponse with expected fields."""
        response = test_client.get("/api/v1/anomalies?metric=dau&lookback=30")
        assert response.status_code == 200
        data = response.json()
        assert "anomalies" in data
        assert "metric" in data
        assert "lookback_days" in data
        assert "total_anomalies" in data
        assert isinstance(data["anomalies"], list)

    def test_get_anomalies_defaults_metric_and_lookback(self, test_client: TestClient) -> None:
        """Verify GET /api/v1/anomalies uses default metric='dau' and lookback=30."""
        response = test_client.get("/api/v1/anomalies")
        assert response.status_code == 200
        data = response.json()
        assert data["metric"] == "dau"
        assert data["lookback_days"] == 30

    def test_get_anomalies_with_invalid_lookback_returns_422(self, test_client: TestClient) -> None:
        """Verify GET /api/v1/anomalies with lookback=0 returns 422 (ge=1)."""
        response = test_client.get("/api/v1/anomalies?lookback=0")
        assert response.status_code == 422

    def test_get_anomalies_lookback_too_large_returns_422(self, test_client: TestClient) -> None:
        """Verify GET /api/v1/anomalies with lookback=999 returns 422 (le=365)."""
        response = test_client.get("/api/v1/anomalies?lookback=999")
        assert response.status_code == 422


class TestUnknownEndpoint:
    """Tests for undefined routes."""

    def test_unknown_endpoint_returns_404(self, test_client: TestClient) -> None:
        """Verify hitting an undefined route returns 404."""
        response = test_client.get("/api/v1/nonexistent")
        assert response.status_code == 404
