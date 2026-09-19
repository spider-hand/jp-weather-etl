.PHONY: dev down storage-up storage-setup storage-smoke storage-reset query format lint test

storage-up:
	docker compose up -d --wait

storage-setup: storage-up
	uv run --env-file .env python -m storage.setup

storage-smoke: storage-setup
	uv run --env-file .env python -m storage.smoke

storage-reset:
	docker compose down -v

dev: storage-setup
	uv run --env-file .env dg dev

down:
	docker compose down

query:
	uv run --env-file .env python -m queries.analytics "$(SQL)"

format:
	uv run ruff format storage queries pipeline

lint:
	uv run ruff check storage queries pipeline

test:
	uv run pytest
