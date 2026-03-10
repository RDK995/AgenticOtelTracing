#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  source "$ROOT_DIR/.env"
  set +a
fi

OTEL_POSTGRES_DSN="${OTEL_POSTGRES_DSN:-postgresql://${POSTGRES_USER:-otel_user}:${POSTGRES_PASSWORD:-otel_password}@localhost:${POSTGRES_PORT:-5432}/${POSTGRES_DB:-otel_observability}}"
OTEL_INGEST_HOST="${OTEL_INGEST_HOST:-127.0.0.1}"
OTEL_INGEST_PORT="${OTEL_INGEST_PORT:-4318}"
OTEL_INGEST_TRACES_PATH="${OTEL_INGEST_TRACES_PATH:-/v1/traces}"
OTEL_INGEST_LOG_LEVEL="${OTEL_INGEST_LOG_LEVEL:-INFO}"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"

if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="${PYTHON_BIN_FALLBACK:-python}"
fi

if command -v lsof >/dev/null 2>&1; then
  existing_pid="$(lsof -tiTCP:"$OTEL_INGEST_PORT" -sTCP:LISTEN | head -n 1 || true)"
  if [[ -n "$existing_pid" ]]; then
    echo "OTEL ingest port $OTEL_INGEST_PORT is already in use (PID: $existing_pid)." >&2
    echo "Stop it with: ./scripts/postgres/stop_otel_ingest.sh" >&2
    exit 0
  fi
fi

"$PYTHON_BIN" -m uk_resell_adk.otel_ingest \
  --host "$OTEL_INGEST_HOST" \
  --port "$OTEL_INGEST_PORT" \
  --path "$OTEL_INGEST_TRACES_PATH" \
  --db-dsn "$OTEL_POSTGRES_DSN" \
  --log-level "$OTEL_INGEST_LOG_LEVEL"
