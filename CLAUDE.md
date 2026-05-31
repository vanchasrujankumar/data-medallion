# Data Medallion — Real-Time ELT Pipeline

## Project Context

Real-time medallion-architecture data pipeline: Bronze (raw) → Silver (cleaned) → Gold (aggregated).

**Stack:** Redpanda → DuckDB/Iceberg → dbt → Polars → FastAPI
**Orchestration:** Airflow
**Infra:** Docker Compose (local dev), MinIO (S3 storage)

## Architecture

```
Data Sources → Redpanda → Bronze (raw Iceberg) → dbt → Silver (cleaned) → dbt/Polars → Gold (aggregated) → FastAPI
```

## Key Files

| File | Purpose |
|------|---------|
| `.github/AGENTS.md` | AI agent guidelines (Karpathy principles) |
| `opencode.jsonc` | Local AI coding configuration |
| `src/bronze/` | Real-time producer/consumer |
| `src/silver/` | dbt transformations |
| `src/gold/` | Polars analytics + FastAPI API |
| `airflow/dags/` | Pipeline orchestration DAG |
| `renovate.json` | Auto-dependency updates |
| `.github/whitesource.yml` | CVE/SCA scanning |

## Development

- `make up` — start all services
- `make produce` — simulate real-time data
- `make test` — run unit tests
- Open [/api/v1](http://localhost:8000/api/v1/kpis/dau) to check data
- Open [Airflow](http://localhost:8080) to trigger pipeline (admin/admin)
