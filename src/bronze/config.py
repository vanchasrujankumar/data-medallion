from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    redpanda_bootstrap_servers: str = "localhost:9092"
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "medallion"
    duckdb_path: str = "data/medallion.duckdb"
    iceberg_warehouse: str = "s3://medallion/iceberg/"
    topic_events: str = "raw.events"
    topic_orders: str = "raw.orders"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
