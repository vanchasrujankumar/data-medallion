from __future__ import annotations

from pathlib import Path

import yaml

SILVER_DIR = Path(__file__).resolve().parent.parent / "src" / "silver"
MODELS_DIR = SILVER_DIR / "models"


class TestSourcesYml:
    """Tests for sources.yml."""

    def test_sources_yml_is_valid_yaml(self) -> None:
        """Verify sources.yml loads without YAML parse errors."""
        path = MODELS_DIR / "sources.yml"
        assert path.exists(), f"sources.yml not found at {path}"
        with open(path) as f:
            data = yaml.safe_load(f)
        assert data is not None

    def test_sources_yml_has_bronze_source(self) -> None:
        """Verify sources.yml defines the 'bronze' source with events and orders tables."""
        path = MODELS_DIR / "sources.yml"
        with open(path) as f:
            data = yaml.safe_load(f)
        sources = data["sources"]
        bronze = next(s for s in sources if s["name"] == "bronze")
        assert bronze["schema"] == "bronze"
        table_names = {t["name"] for t in bronze["tables"]}
        assert "events" in table_names
        assert "orders" in table_names


class TestDbtProjectYml:
    """Tests for dbt_project.yml."""

    def test_dbt_project_yml_is_valid_yaml(self) -> None:
        """Verify dbt_project.yml loads without YAML parse errors."""
        path = SILVER_DIR / "dbt_project.yml"
        assert path.exists(), f"dbt_project.yml not found at {path}"
        with open(path) as f:
            data = yaml.safe_load(f)
        assert data is not None

    def test_dbt_project_yml_has_correct_name(self) -> None:
        """Verify dbt_project.yml has project name 'medallion'."""
        path = SILVER_DIR / "dbt_project.yml"
        with open(path) as f:
            data = yaml.safe_load(f)
        assert data["name"] == "medallion"

    def test_dbt_project_yml_has_correct_schema_routing(self) -> None:
        """Verify bronze_to_silver models route to 'silver' schema and silver_to_gold to 'gold'."""
        path = SILVER_DIR / "dbt_project.yml"
        with open(path) as f:
            data = yaml.safe_load(f)
        models = data["models"]["medallion"]
        assert models["bronze_to_silver"]["+schema"] == "silver"
        assert models["bronze_to_silver"]["+materialized"] == "table"
        assert models["silver_to_gold"]["+schema"] == "gold"
        assert models["silver_to_gold"]["+materialized"] == "table"

    def test_dbt_project_yml_has_required_config_version(self) -> None:
        """Verify dbt_project.yml uses config-version 2."""
        path = SILVER_DIR / "dbt_project.yml"
        with open(path) as f:
            data = yaml.safe_load(f)
        assert data["config-version"] == 2


class TestProfilesYml:
    """Tests for profiles.yml."""

    def test_profiles_yml_is_valid_yaml(self) -> None:
        """Verify profiles.yml loads without YAML parse errors."""
        path = SILVER_DIR / "profiles.yml"
        assert path.exists(), f"profiles.yml not found at {path}"
        with open(path) as f:
            data = yaml.safe_load(f)
        assert data is not None

    def test_profiles_yml_has_required_fields(self) -> None:
        """Verify profiles.yml defines a 'medallion' profile with DuckDB output."""
        path = SILVER_DIR / "profiles.yml"
        with open(path) as f:
            data = yaml.safe_load(f)
        profile = data["medallion"]
        assert profile["target"] == "dev"
        output = profile["outputs"]["dev"]
        assert output["type"] == "duckdb"
        assert "path" in output
        assert "extensions" in output
        assert "iceberg" in output["extensions"]
        assert "parquet" in output["extensions"]
        assert "settings" in output

    def test_profiles_yml_has_minio_settings(self) -> None:
        """Verify profiles.yml contains S3/MinIO connection settings."""
        path = SILVER_DIR / "profiles.yml"
        with open(path) as f:
            data = yaml.safe_load(f)
        settings = data["medallion"]["outputs"]["dev"]["settings"]
        assert "s3_endpoint" in settings
        assert "s3_access_key_id" in settings
        assert "s3_secret_access_key" in settings
        assert "s3_use_ssl" in settings
        assert "s3_url_style" in settings


class TestSqlModels:
    """Tests for dbt SQL model files."""

    def test_clean_events_sql_contains_dedup_keywords(self) -> None:
        """Verify clean_events.sql uses ROW_NUMBER with PARTITION BY for dedup."""
        path = MODELS_DIR / "bronze_to_silver" / "clean_events.sql"
        sql = path.read_text()
        assert "ROW_NUMBER() OVER" in sql
        assert "PARTITION BY event_id" in sql
        assert "rn = 1" in sql

    def test_clean_events_sql_contains_cast(self) -> None:
        """Verify clean_events.sql uses CAST for timestamp and JSON."""
        path = MODELS_DIR / "bronze_to_silver" / "clean_events.sql"
        sql = path.read_text()
        assert "CAST(timestamp AS TIMESTAMP)" in sql
        assert "TRY_CAST(metadata AS JSON)" in sql

    def test_validate_orders_sql_contains_dedup_keywords(self) -> None:
        """Verify validate_orders.sql uses ROW_NUMBER with PARTITION BY for dedup."""
        path = MODELS_DIR / "bronze_to_silver" / "validate_orders.sql"
        sql = path.read_text()
        assert "ROW_NUMBER() OVER" in sql
        assert "PARTITION BY order_id" in sql
        assert "rn = 1" in sql

    def test_validate_orders_sql_contains_validation_failed(self) -> None:
        """Verify validate_orders.sql has validation_failed status logic."""
        path = MODELS_DIR / "bronze_to_silver" / "validate_orders.sql"
        sql = path.read_text()
        assert "validation_failed" in sql

    def test_validate_orders_sql_unnests_items_json(self) -> None:
        """Verify validate_orders.sql uses UNNEST on items JSON array."""
        path = MODELS_DIR / "bronze_to_silver" / "validate_orders.sql"
        sql = path.read_text()
        assert "UNNEST" in sql
        assert "json_transform(items" in sql

    def test_customer_360_sql_uses_refs(self) -> None:
        """Verify customer_360.sql references clean_events and validate_orders."""
        path = MODELS_DIR / "silver_to_gold" / "customer_360.sql"
        sql = path.read_text()
        assert "ref('clean_events')" in sql
        assert "ref('validate_orders')" in sql

    def test_daily_kpis_sql_uses_ref(self) -> None:
        """Verify daily_kpis.sql references clean_events."""
        path = MODELS_DIR / "silver_to_gold" / "daily_kpis.sql"
        sql = path.read_text()
        assert "ref('clean_events')" in sql

    def test_daily_kpis_sql_has_window_function(self) -> None:
        """Verify daily_kpis.sql calculates conversion_rate."""
        path = MODELS_DIR / "silver_to_gold" / "daily_kpis.sql"
        sql = path.read_text()
        assert "conversion_rate" in sql

    def test_all_sql_model_files_have_select(self) -> None:
        """Verify every SQL model file contains a SELECT statement."""
        sql_files = list(MODELS_DIR.rglob("*.sql"))
        assert len(sql_files) >= 4, f"Expected at least 4 SQL model files, found {len(sql_files)}"
        for path in sql_files:
            sql = path.read_text()
            assert "SELECT" in sql, f"{path.name} missing SELECT"
