.PHONY: up down migrate seed smoke logs fmt lint

up:
	docker compose up -d --build
	sleep 2

migrate:
	docker compose exec api python -m backend.scripts.migrate

seed:
	docker compose exec api python scripts/seed_mock_data.py

smoke:
	python3 -m pytest --cov=backend --cov-report=term-missing -q

logs:
	docker compose logs -f --tail=200

down:
	docker compose down -v
