#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-https://127.0.0.1:5000}"

curl -sSk "${BASE_URL}/healthz" | jq .
