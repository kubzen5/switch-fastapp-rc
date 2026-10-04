#!/usr/bin/env bash
# Replay the configured demo without clearing the sink or changing source data.
set -euo pipefail
replay_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$replay_root"
replay_id="$(date -u +%Y%m%d%H%M%S)-$$"
replay_evidence="$replay_root/docs/review-runs/replay-$replay_id"
mkdir -p "$replay_evidence"
chmod 700 "$replay_evidence"

# Restore only workers which were running before this command.
replay_running=()
replay_services=$(docker compose ps --status running --services)
while IFS= read -r replay_service; do
    case "$replay_service" in
        adapter|consumer) replay_running+=("$replay_service") ;;
    esac
done <<< "$replay_services"

restore_workers() {
    replay_exit=$?
    trap - EXIT
    if ((${#replay_running[@]})); then
        docker compose up -d --no-deps "${replay_running[@]}" || replay_exit=1
    fi
    echo "Replay evidence: $replay_evidence/replay.json"
    exit "$replay_exit"
}
trap restore_workers EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

docker compose config --quiet
docker compose stop adapter consumer
# The configured compose environment supplies credentials. Use the host UID
# so the one-shot process can write to a private evidence directory.
docker compose run --rm --no-deps -T \
    --user "$(id -u):$(id -g)" --workdir /tmp \
    --volume "$replay_root:/workspace:ro" \
    --volume "$replay_evidence:/evidence" \
    --env PYTHONPATH=/workspace --env CONSUMER_EXCLUSIVE=true \
    adapter python /workspace/scripts/verify_pipeline.py replay
