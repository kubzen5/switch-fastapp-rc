#!/usr/bin/env bash
set -euo pipefail
simulation_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$simulation_root"
# make up first to build the image containing app.adapter.simulate.
exec docker compose run --rm --no-deps -T adapter \
    python -u -m app.adapter.simulate "$@"
