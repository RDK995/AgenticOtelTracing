#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  source "$ROOT_DIR/.env"
  set +a
fi

OTEL_INGEST_PORT="${OTEL_INGEST_PORT:-4318}"

if ! command -v lsof >/dev/null 2>&1; then
  echo "Error: lsof is required to stop ingest by port." >&2
  exit 1
fi

pids="$(lsof -tiTCP:"$OTEL_INGEST_PORT" -sTCP:LISTEN || true)"
if [[ -z "$pids" ]]; then
  echo "No listener found on port $OTEL_INGEST_PORT."
  exit 0
fi

echo "Stopping OTEL ingest process(es) on port $OTEL_INGEST_PORT: $pids"
kill $pids
echo "Done."
