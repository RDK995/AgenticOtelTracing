#!/usr/bin/env bash
set -euo pipefail

POSTGRES_CONTAINER_NAME="${POSTGRES_CONTAINER_NAME:-otel-postgres}"
POSTGRES_DB="${POSTGRES_DB:-otel_observability}"
POSTGRES_USER="${POSTGRES_USER:-otel_user}"

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker is not installed." >&2
  exit 1
fi

if [[ -t 0 && -t 1 ]]; then
  exec docker exec -it "$POSTGRES_CONTAINER_NAME" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" "$@"
fi

exec docker exec "$POSTGRES_CONTAINER_NAME" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" "$@"
