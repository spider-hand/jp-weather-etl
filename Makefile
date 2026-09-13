.PHONY: dev down storage-up storage-setup storage-smoke storage-reset test

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

test:
	uv run --project pipeline pytest
