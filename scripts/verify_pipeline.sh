#!/usr/bin/env bash
# Real Snowflake -> Redpanda -> PostgreSQL -> HTTP; keeps the existing stack intact.
set -euo pipefail
review_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
export REVIEW_ROOT="$review_root"
cd "$review_root"
review_id="$(date -u +%Y%m%d%H%M%S)-$$"
review_project="switch-review-$review_id"
export REVIEW_EVIDENCE_DIR SOURCE_NAME KAFKA_TOPIC KAFKA_GROUP_ID SNOWFLAKE_TABLE
export CONSUMER_EXCLUSIVE=true
REVIEW_EVIDENCE_DIR="$review_root/docs/review-runs/$review_id"
mkdir -p "$REVIEW_EVIDENCE_DIR"
chmod 777 "$REVIEW_EVIDENCE_DIR"
SOURCE_NAME="review.$review_id"
KAFKA_TOPIC="review.$review_id.orders"
KAFKA_GROUP_ID="review.$review_id.materializer"
SNOWFLAKE_TABLE="ORDERS_REVIEW_${review_id//-/_}"
review_compose=(docker compose --env-file "$review_root/.env" -p "$review_project"
    -f "$review_root/docker-compose.yml" -f "$review_root/tests/pipeline-review.override.yml")
cleanup() {
    review_exit=$?
    trap - EXIT
    # Stop the entire isolated project, even after a failed phase or SIGINT.
    # Keep volumes and Snowflake journal for investigation; never reset sources.
    "${review_compose[@]}" down >/dev/null 2>&1 || true
    echo "Evidence: $REVIEW_EVIDENCE_DIR (exit=$review_exit)"
    echo "Retained volumes belong to $review_project; Snowflake table: $SNOWFLAKE_TABLE"
    exit "$review_exit"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
phase() {
    echo "Running: $1"
    "${review_compose[@]}" run --rm --no-deps -T adapter \
        python /workspace/scripts/verify_pipeline.py "$1" \
        >"$REVIEW_EVIDENCE_DIR/$1.log" 2>&1 || {
            tail -n 12 "$REVIEW_EVIDENCE_DIR/$1.log"
            return 1
        }
    tail -n 1 "$REVIEW_EVIDENCE_DIR/$1.log"
}
"${review_compose[@]}" config --quiet
"${review_compose[@]}" up -d --build --wait
"${review_compose[@]}" stop adapter
phase prepare
phase initial
phase cursor-collision
phase no-change
phase mutate
phase incremental
"${review_compose[@]}" restart adapter consumer
phase restart
phase exclusion
"${review_compose[@]}" stop adapter
phase mutate-outage
"${review_compose[@]}" stop -t 5 redpanda
phase outage
"${review_compose[@]}" up -d --wait redpanda
phase recovery
phase quarantine
"${review_compose[@]}" stop adapter consumer
phase replay
"${review_compose[@]}" up -d adapter consumer
phase final
"${review_compose[@]}" logs --no-color adapter consumer >"$REVIEW_EVIDENCE_DIR/workers.log"
"${review_compose[@]}" run --rm --no-deps -T adapter \
    python /workspace/scripts/verify_pipeline.py summary
echo "All nine pipeline stages passed."
