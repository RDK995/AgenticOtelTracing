#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

POSTGRES_CONTAINER_NAME="${POSTGRES_CONTAINER_NAME:-otel-postgres}"
POSTGRES_IMAGE="${POSTGRES_IMAGE:-postgres:16}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_DB="${POSTGRES_DB:-otel_observability}"
POSTGRES_USER="${POSTGRES_USER:-otel_user}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-otel_password}"
POSTGRES_VOLUME="${POSTGRES_VOLUME:-otel-postgres-data}"
POSTGRES_INIT_DIR="${POSTGRES_INIT_DIR:-$ROOT_DIR/infra/postgres/init}"

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker is not installed." >&2
  exit 1
fi

if [[ ! -d "$POSTGRES_INIT_DIR" ]]; then
  echo "Error: init directory not found: $POSTGRES_INIT_DIR" >&2
  exit 1
fi

if docker ps -a --format '{{.Names}}' | grep -Fxq "$POSTGRES_CONTAINER_NAME"; then
  if docker ps --format '{{.Names}}' | grep -Fxq "$POSTGRES_CONTAINER_NAME"; then
    echo "Postgres container '$POSTGRES_CONTAINER_NAME' is already running."
    exit 0
  fi
  echo "Starting existing container '$POSTGRES_CONTAINER_NAME'..."
  docker start "$POSTGRES_CONTAINER_NAME" >/dev/null
else
  echo "Creating volume '$POSTGRES_VOLUME' (if missing)..."
  docker volume create "$POSTGRES_VOLUME" >/dev/null

  echo "Starting new Postgres container '$POSTGRES_CONTAINER_NAME'..."
  docker run -d \
    --name "$POSTGRES_CONTAINER_NAME" \
    -e POSTGRES_DB="$POSTGRES_DB" \
    -e POSTGRES_USER="$POSTGRES_USER" \
    -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
    -p "$POSTGRES_PORT:5432" \
    -v "$POSTGRES_VOLUME:/var/lib/postgresql/data" \
    -v "$POSTGRES_INIT_DIR:/docker-entrypoint-initdb.d:ro" \
    "$POSTGRES_IMAGE" >/dev/null
fi

echo "Waiting for Postgres to become ready..."
for _ in $(seq 1 60); do
  if docker exec "$POSTGRES_CONTAINER_NAME" pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; then
    echo "Postgres is ready."
    echo "Connection string:"
    echo "postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@localhost:$POSTGRES_PORT/$POSTGRES_DB"
    exit 0
  fi
  sleep 1
done

echo "Error: Postgres did not become ready in time." >&2
docker logs "$POSTGRES_CONTAINER_NAME" --tail 80 >&2 || true
exit 1
