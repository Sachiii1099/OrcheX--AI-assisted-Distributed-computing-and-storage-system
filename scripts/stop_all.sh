#!/usr/bin/env bash
# Cleanly stop everything started by start_cluster.sh (Workers first, then the Master).
. "$(dirname "$0")/common.sh"
stop_pid() {
  local f="$1" name; name="$(basename "$f" .pid)"
  [ -f "$f" ] || return 0
  local pid; pid="$(cat "$f")"
  if kill -0 "$pid" 2>/dev/null; then
    kill -TERM "$pid" 2>/dev/null
    for _ in $(seq 1 50); do kill -0 "$pid" 2>/dev/null || break; sleep 0.2; done
    kill -0 "$pid" 2>/dev/null && { echo "force-killing $name"; kill -KILL "$pid" 2>/dev/null; } || echo "stopped $name"
  fi
  rm -f "$f"
}
for f in "$RUN_DIR"/worker_*.pid; do [ -e "$f" ] && stop_pid "$f"; done
stop_pid "$RUN_DIR/master.pid"
exit 0
