#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:5000}"
OUT_FILE="${OUT_FILE:-/tmp/node1_job_id.txt}"
JOB_ID="${1:-${JOB_ID:-}}"
STATUS="${2:-${STATUS:-processing}}"

if [[ -z "${JOB_ID}" ]] && [[ -f "${OUT_FILE}" ]]; then
  JOB_ID="$(cat "${OUT_FILE}")"
fi

if [[ -z "${JOB_ID}" ]]; then
  echo "usage: $0 <job_id> [status]"
  echo "or set JOB_ID env, or run 02_enqueue.sh first"
  exit 1
fi

curl -sS -X POST "${BASE_URL}/api/v1/jobs/${JOB_ID}/status" \
  -H "Content-Type: application/json" \
  -d "{\"status\":\"${STATUS}\"}" | jq .
