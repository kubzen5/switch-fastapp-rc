#!/usr/bin/env bash
set -euo pipefail
review_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$review_root"
review_compose=(docker compose -p "switch-regression-$$" -f tests/integration.compose.yml)
cleanup() {
    review_exit=$?
    trap - EXIT
    "${review_compose[@]}" down --volumes >/dev/null 2>&1 || true
    exit "$review_exit"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
"${review_compose[@]}" build consumer
"${review_compose[@]}" up -d --wait db broker
"${review_compose[@]}" run --rm --no-deps -T consumer python -m app.db.migrate
"${review_compose[@]}" exec -T broker rpk topic create regression.orders --partitions 3 -X brokers=localhost:9092
"${review_compose[@]}" up -d --wait consumer
export TEST_POSTGRES_HOST=localhost TEST_POSTGRES_PORT="${REVIEW_TEST_DB_PORT:-55434}"
export TEST_POSTGRES_DB=postgres TEST_POSTGRES_USER=postgres TEST_POSTGRES_PASSWORD=isolated-test-only
export TEST_KAFKA_BOOTSTRAP_SERVERS="localhost:${REVIEW_TEST_KAFKA_PORT:-29093}" TEST_KAFKA_TOPIC=regression.orders
.venv/bin/python -m pytest -q --junitxml=docs/review-tests.xml
