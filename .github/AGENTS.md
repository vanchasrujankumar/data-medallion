## Agent Guidelines for Data Medallion

Always apply **karpathy-guidelines** skill when coding.

### Before Implementing
- Check existing code structure and patterns first
- State assumptions about the data flow
- If a simpler approach exists, say so

### Code Standards
- Python 3.12+, type hints everywhere
- Use `pydantic` for data models
- Prefer `pathlib` over `os.path`
- All SQL in dbt models, not inline
- Async for I/O (Redpanda, HTTP)
- Tests for each layer (Bronze, Silver, Gold)

### Testing
- Bronze: test producer serialization + consumer parsing
- Silver: test dbt model outputs with known inputs
- Gold: test Polars aggregations produce correct metrics
- Docker tests must pass before commit

### Verifying Success
- `make lint` — ruff clean
- `make typecheck` — mypy clean
- `make test` — all tests passing
- `make build` — Docker builds
