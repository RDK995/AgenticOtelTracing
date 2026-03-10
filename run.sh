#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"
export PYTHONPATH="$ROOT_DIR/src${PYTHONPATH:+:$PYTHONPATH}"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="${PYTHON_BIN_FALLBACK:-python}"
fi

if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  source "$ROOT_DIR/.env"
  set +a
fi

_truthy() {
  local raw="${1:-}"
  local raw_lc
  raw_lc="$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')"
  case "$raw_lc" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

_require_python_311() {
  if "$PYTHON_BIN" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)'; then
    return 0
  fi
  echo "Error: Python >=3.11 is required. Current interpreter: $("$PYTHON_BIN" -c 'import sys; print(sys.executable)') ($("$PYTHON_BIN" -V 2>&1))." >&2
  exit 1
}

_require_python_module() {
  local module_name="$1"
  if "$PYTHON_BIN" -c "import ${module_name}" >/dev/null 2>&1; then
    return 0
  fi
  echo "Error: Python module '${module_name}' is required but not installed in $("$PYTHON_BIN" -c 'import sys; print(sys.executable)')." >&2
  exit 1
}

_require_python_311

if _truthy "${ENABLE_LANGFUSE_TRACING:-true}" \
  && [[ -n "${LANGFUSE_PUBLIC_KEY:-}" ]] \
  && [[ -n "${LANGFUSE_SECRET_KEY:-}" ]]; then
  _require_python_module "langfuse"
  export LANGFUSE_USER_ID="${LANGFUSE_USER_ID:-${USER:-unknown-user}}"
  export LANGFUSE_SESSION_ID="${LANGFUSE_SESSION_ID:-uk-resell-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
fi

usage() {
  cat <<'USAGE'
Usage:
  ./run.sh [mode] [args...]

Modes:
  local   Run local dry-run entrypoint (default)
  adk     Run ADK web with uk_resell_adk.app:root_agent

Examples:
  ./run.sh
  ./run.sh local --json
  ./run.sh adk
USAGE
}

mode="local"
if [[ $# -gt 0 ]]; then
  case "$1" in
    local|adk)
      mode="$1"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
  esac
fi

if [[ "$mode" == "local" ]]; then
  "$PYTHON_BIN" -m uk_resell_adk.main "$@"
  exit 0
fi

if ! command -v adk >/dev/null 2>&1; then
  echo "Error: adk CLI not found. Install/configure ADK CLI first." >&2
  exit 1
fi

adk web uk_resell_adk.app:root_agent "$@"
