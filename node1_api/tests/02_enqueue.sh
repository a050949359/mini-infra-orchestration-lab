#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-https://127.0.0.1:5000}"
OUT_FILE="${OUT_FILE:-/tmp/node1_job_id.txt}"

RESPONSE="$(curl -sSk -X POST "${BASE_URL}/api/v1/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "type": "process_data",
    "payload": {
      "user_id": 123,
      "action": "resize_image",
      "data": "base64_or_url"
    },
    "priority": 1
  }')"

echo "${RESPONSE}" | jq .
JOB_ID="$(echo "${RESPONSE}" | jq -r '.job_id')"

if [[ -z "${JOB_ID}" || "${JOB_ID}" == "null" ]]; then
  echo "enqueue failed: no job_id"
  exit 1
fi

echo "${JOB_ID}" > "${OUT_FILE}"
echo "saved job_id to ${OUT_FILE}"
