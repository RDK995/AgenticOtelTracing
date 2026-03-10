#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  source "$ROOT_DIR/.env"
  set +a
fi

POSTGRES_CONTAINER_NAME="${POSTGRES_CONTAINER_NAME:-otel-postgres}"
POSTGRES_DB="${POSTGRES_DB:-otel_observability}"
POSTGRES_USER="${POSTGRES_USER:-otel_user}"
SCHEMA_NAME="${SCHEMA_NAME:-otel}"
AUTO_YES="${1:-}"

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker is not installed." >&2
  exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -Fxq "$POSTGRES_CONTAINER_NAME"; then
  echo "Error: Postgres container '$POSTGRES_CONTAINER_NAME' is not running." >&2
  exit 1
fi

if [[ "$AUTO_YES" != "--yes" ]]; then
  echo "This will DELETE all data in schema '$SCHEMA_NAME' for database '$POSTGRES_DB'."
  printf "Type 'yes' to continue: "
  read -r confirmation
  if [[ "$confirmation" != "yes" ]]; then
    echo "Cancelled."
    exit 0
  fi
fi

docker exec -i "$POSTGRES_CONTAINER_NAME" psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<SQL
TRUNCATE TABLE
  ${SCHEMA_NAME}.export_attempts,
  ${SCHEMA_NAME}.span_links,
  ${SCHEMA_NAME}.span_events,
  ${SCHEMA_NAME}.spans,
  ${SCHEMA_NAME}.traces
RESTART IDENTITY;

DELETE FROM ${SCHEMA_NAME}.export_watermarks;

INSERT INTO ${SCHEMA_NAME}.export_watermarks (destination)
VALUES ('langfuse'), ('dynatrace'), ('cloud_trace')
ON CONFLICT (destination) DO NOTHING;
SQL

echo "Cleared schema '$SCHEMA_NAME' trace data and reset exporter watermarks."
