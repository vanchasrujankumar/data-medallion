from pydantic_settings import BaseSettings


class GoldSettings(BaseSettings):
    duckdb_path: str = "data/medallion.duckdb"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    cache_ttl_seconds: int = 300

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "env_prefix": "GOLD_"}


settings = GoldSettings()
