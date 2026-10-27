#!/usr/bin/env bash
# Sourced by the other scripts: project paths, .env loading, small helpers.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/build"
RUN_DIR="$ROOT/run"          # pid files
LOG_DIR="$ROOT/logs"
mkdir -p "$RUN_DIR" "$LOG_DIR"

if [ -f "$ROOT/.env" ]; then set -a; . "$ROOT/.env"; set +a; fi
export NEXUS_MASTER_HOST="${NEXUS_MASTER_HOST:-127.0.0.1}"
export NEXUS_MASTER_PORT="${NEXUS_MASTER_PORT:-5050}"
export NEXUS_HTTP_PORT="${NEXUS_HTTP_PORT:-8080}"
API="http://127.0.0.1:${NEXUS_HTTP_PORT}"

die()  { echo "ERROR: $*" >&2; exit 1; }
need_binaries() { [ -x "$BIN/master" ] && [ -x "$BIN/worker" ] || die "not built yet - run ./scripts/build.sh"; }
need_db_env()   { [ -n "${NEXUS_DB_PASSWORD:-}" ] || die "NEXUS_DB_PASSWORD is not set. Copy .env.example to .env and edit it (or export the variables)."; }

# wait_for <description> <timeout-seconds> <command...>
wait_for() {
  local what="$1" t="$2"; shift 2
  local i=0
  while ! "$@" >/dev/null 2>&1; do
    i=$((i+1)); [ "$i" -gt $((t*5)) ] && { echo "timeout waiting for $what" >&2; return 1; }
    sleep 0.2
  done
}
master_up() { curl -fsS "$API/api/health" >/dev/null 2>&1; }

# json_get <json> <python-expression using d>  e.g. json_get "$x" "d['task_id']"
json_get() { python3 -c "import sys,json; d=json.loads(sys.argv[1]); print($2)" "$1"; }
