.PHONY: dev ui down storage-up storage-setup storage-smoke storage-reset query format lint test

storage-up:
	docker compose up -d --wait

storage-setup: storage-up
	uv run --project pipeline --env-file .env python -m pipeline.storage.setup

storage-smoke: storage-setup
	uv run --project pipeline --env-file .env python -m pipeline.storage.smoke

storage-reset:
	docker compose down -v

dev: storage-setup
	cd pipeline && uv run --env-file ../.env dg dev

down:
	docker compose down

ui:
	uv run --project ui streamlit run ui/main.py

query:
	uv run --project pipeline --env-file .env python -m pipeline.analytics "$(SQL)"

format:
	uv run --project pipeline ruff format pipeline

lint:
	uv run --project pipeline ruff check pipeline

test:
	uv run --project pipeline pytest
