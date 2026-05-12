#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:5000}"

curl -sS "${BASE_URL}/healthz" | jq .
