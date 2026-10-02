.PHONY: install lock up down logs config migrate test test-integration

install:
	uv sync --frozen --extra dev

lock:
	uv lock
	uv export --frozen --no-dev --no-emit-project --format requirements-txt --output-file requirements.lock --quiet

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f adapter consumer api

config:
	docker compose config --quiet

migrate:
	docker compose run --rm migrate

test:
	uv run --frozen --extra dev pytest -m 'not integration'

test-integration:
	uv run --frozen --extra dev pytest -m integration

.PHONY: verify verify-tests verify-broker
verify:
	bash scripts/verify_pipeline.sh

verify-tests:
	bash scripts/verify_tests.sh

verify-broker:
	bash scripts/verify_redpanda.sh

# Inspect only by default. SCOPE=pipeline also resets adapter checkpoints.
SCOPE ?= sink
.PHONY: db-clean-check db-clean-execute
db-clean-check:
	docker compose run --rm --no-deps --build api python -m app.db.clean --scope $(SCOPE)

db-clean-execute:
	docker compose stop adapter consumer
	docker compose run --rm --no-deps --build api python -m app.db.clean --scope $(SCOPE) --execute
