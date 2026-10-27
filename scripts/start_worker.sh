#!/usr/bin/env bash
# Start ONE Worker in the foreground.
#   ./scripts/start_worker.sh <WORKER_ID> [capacity]
#   capacity = how many tasks this Worker may run at the same time (default: number of CPU cores)
# Block storage of the Worker: ./worker_data/<WORKER_ID>/ (override with NEXUS_WORKER_STORAGE)
. "$(dirname "$0")/common.sh"
need_binaries
[ -n "${1:-}" ] || die "usage: $0 <WORKER_ID> [capacity]"
cd "$ROOT"
if [ -n "${2:-}" ]; then export NEXUS_WORKER_CAPACITY="$2"; fi
exec "$BIN/worker" "$1"
