.PHONY: dev down storage-up storage-setup storage-smoke storage-reset query format lint test

storage-up:
	docker compose up -d --wait

storage-setup: storage-up
	uv run --env-file .env python -m jp_weather_etl.storage.setup

storage-smoke: storage-setup
	uv run --env-file .env python -m jp_weather_etl.storage.smoke

storage-reset:
	docker compose down -v

dev: storage-setup
	uv run --env-file .env dg dev

down:
	docker compose down

query:
	uv run --env-file .env python -m jp_weather_etl.queries.analytics "$(SQL)"

format:
	uv run ruff format src tests

lint:
	uv run ruff check src tests

test:
	uv run pytest
