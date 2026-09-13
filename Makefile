.PHONY: dev down storage-up storage-setup storage-smoke storage-reset

storage-up:
	docker compose up -d --wait

storage-setup: storage-up
	uv run --project storage --env-file .env python storage/setup.py

storage-smoke: storage-setup
	uv run --project storage --env-file .env python storage/smoke.py

storage-reset:
	docker compose down -v

dev: storage-setup
	cd pipeline && uv run --env-file ../.env dg dev

down:
	docker compose down
