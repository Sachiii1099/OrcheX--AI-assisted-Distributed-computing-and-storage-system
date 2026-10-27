#!/usr/bin/env bash
# Start the Master in the FOREGROUND (interactive console on stdin, Ctrl+C to stop).
#   ./scripts/start_master.sh
# Dashboard: http://127.0.0.1:${NEXUS_HTTP_PORT:-8080}/
. "$(dirname "$0")/common.sh"
need_binaries; need_db_env
cd "$ROOT"
exec "$BIN/master"
