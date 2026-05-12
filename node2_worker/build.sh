#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${SCRIPT_DIR}/bin"
OUT_FILE="${OUT_DIR}/mini-orch-worker"

mkdir -p "${OUT_DIR}"
cd "${SCRIPT_DIR}"

echo "[build] go mod tidy"
go mod tidy

echo "[build] build linux/amd64 worker binary"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o "${OUT_FILE}" ./cmd/worker

echo "[build] output: ${OUT_FILE}"
