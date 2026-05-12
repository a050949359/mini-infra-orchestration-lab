#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"${SCRIPT_DIR}/01_health.sh"
"${SCRIPT_DIR}/02_enqueue.sh"
"${SCRIPT_DIR}/03_get_job.sh"
"${SCRIPT_DIR}/04_update_status.sh"
"${SCRIPT_DIR}/03_get_job.sh"
