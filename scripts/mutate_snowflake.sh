#!/usr/bin/env bash
set -euo pipefail
docker compose run --rm adapter python -m app.adapter.demo mutate
