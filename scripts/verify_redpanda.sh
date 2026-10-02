#!/usr/bin/env bash
# Real broker/checkpoint/materializer verification on a disposable Compose project.
set -euo pipefail
review_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
export REVIEW_SIGNAL_DIR
REVIEW_SIGNAL_DIR=$(mktemp -d /tmp/switch-redpanda-review.XXXXXX)
chmod 777 "$REVIEW_SIGNAL_DIR"
review_project="switch-redpanda-review-$$"
review_compose=(docker compose -p "$review_project" -f "$review_root/tests/redpanda-review.compose.yml")
cleanup() {
    "${review_compose[@]}" down --volumes >/dev/null
    echo "Review artifacts: $REVIEW_SIGNAL_DIR"
}
trap cleanup EXIT
phase() {
    "${review_compose[@]}" run --rm runner python /workspace/scripts/verify_redpanda.py "$1"
}
"${review_compose[@]}" build runner
"${review_compose[@]}" up -d --wait db broker
phase setup
"${review_compose[@]}" stop -t 5 broker
phase before
"${review_compose[@]}" start broker
phase resume-before
phase partial >"$REVIEW_SIGNAL_DIR/partial.log" 2>&1 &
review_partial_pid=$!
for ((review_wait=0; review_wait<60; review_wait++)); do
    if [[ -f "$REVIEW_SIGNAL_DIR/first-ack" ]]; then break; fi
    sleep 1
done
[[ -f "$REVIEW_SIGNAL_DIR/first-ack" ]]
"${review_compose[@]}" stop -t 5 broker
touch "$REVIEW_SIGNAL_DIR/broker-stopped"
wait "$review_partial_pid"
cat "$REVIEW_SIGNAL_DIR/partial.log"
"${review_compose[@]}" start broker
phase resume-partial
phase lost-ack
review_crash_code=0
phase crash || review_crash_code=$?
[[ "$review_crash_code" == 77 ]]
phase resume-crash
phase report | tee "$REVIEW_SIGNAL_DIR/report.log"
