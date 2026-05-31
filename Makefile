.PHONY: up down logs build test test-docker lint typecheck produce clean shell

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

build:
	docker compose build

test:
	uv run pytest tests/ -v --tb=short

test-docker:
	docker compose run --rm app uv run pytest tests/ -v --tb=short

lint:
	uv run ruff check src/ tests/ --fix

typecheck:
	uv run mypy src/ tests/

produce:
	docker compose exec app uv run python -m src.bronze.producer

producer:
	docker compose exec -d app uv run python -m src.bronze.producer

consumer:
	docker compose exec -d app uv run python -m src.bronze.consumer

api:
	docker compose exec -d app uv run uvicorn gold.serve:app --host 0.0.0.0 --port 8000 --reload

clean:
	rm -rf data/ .mypy_cache/ .pytest_cache/ __pycache__/
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

shell:
	docker compose exec app /bin/bash
