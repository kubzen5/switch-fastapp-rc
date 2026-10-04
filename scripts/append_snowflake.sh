#!/usr/bin/env bash
set -euo pipefail
append_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$append_root"
# Build the writer into the image; the existing adapter and consumer keep running.
exec docker compose run --rm --no-deps --build -T adapter \
    python -m app.adapter.demo append "$@"
