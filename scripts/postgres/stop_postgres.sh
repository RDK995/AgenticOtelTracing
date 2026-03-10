#!/usr/bin/env bash
set -euo pipefail

POSTGRES_CONTAINER_NAME="${POSTGRES_CONTAINER_NAME:-otel-postgres}"
POSTGRES_VOLUME="${POSTGRES_VOLUME:-otel-postgres-data}"
WIPE_DATA="${1:-}"

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker is not installed." >&2
  exit 1
fi

if docker ps -a --format '{{.Names}}' | grep -Fxq "$POSTGRES_CONTAINER_NAME"; then
  echo "Stopping and removing container '$POSTGRES_CONTAINER_NAME'..."
  docker rm -f "$POSTGRES_CONTAINER_NAME" >/dev/null
else
  echo "Container '$POSTGRES_CONTAINER_NAME' does not exist."
fi

if [[ "$WIPE_DATA" == "--wipe-data" ]]; then
  echo "Removing Docker volume '$POSTGRES_VOLUME'..."
  docker volume rm "$POSTGRES_VOLUME" >/dev/null || true
fi

echo "Done."
