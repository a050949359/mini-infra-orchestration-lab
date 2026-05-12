#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${SCRIPT_DIR}/bin"
OUT_FILE="${OUT_DIR}/mini-orch-worker"

mkdir -p "${OUT_DIR}"
cd "${SCRIPT_DIR}"

echo "[build] verify dependencies (read-only module mode)"
go mod download

echo "[build] build linux/amd64 worker binary"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -mod=readonly -o "${OUT_FILE}" ./cmd/worker

echo "[build] output: ${OUT_FILE}"
