#!/usr/bin/env bash
# Start a whole local cluster in the BACKGROUND: 1 Master + N Workers.
#   ./scripts/start_cluster.sh [-w N] [-c CAPACITY] [-a auto|none]
#     -w N          number of Workers W1..WN (default 3)
#     -a MODE       CPU affinity: "auto" (default when there are at least N logical CPUs) gives every Worker its
#                   OWN dedicated logical CPU (W1 -> first allowed CPU, W2 -> second ...); "none" = no pinning.
#                   Explicit CPU per Worker: NEXUS_CPU_MAP="2,4,6" ./scripts/start_cluster.sh   (W1->2, W2->4, W3->6)
#     -c CAPACITY   max concurrent tasks per Worker (default: 1 when pinned = one task per dedicated core;
#                   CPU cores / N, at least 2, when not pinned)
# Three separate settings (see README section 10 "Multi-core processing"): CPU affinity (which CPUs), executor threads
# (NEXUS_WORKER_THREADS) and capacity (max concurrent tasks). The Workers verify the pinning and report the real result.
# Logs: logs/master.log, logs/worker_<ID>.log   PIDs: run/*.pid   Stop with ./scripts/stop_all.sh
# NOTE: all Workers started here share THIS computer's RAM and CPU package - they are NOT separate machines.
. "$(dirname "$0")/common.sh"
need_binaries; need_db_env

N=3; CAP=""; AFF=""
while getopts "w:c:a:" o; do case $o in w) N="$OPTARG";; c) CAP="$OPTARG";; a) AFF="$OPTARG";; *) die "usage: $0 [-w N] [-c CAPACITY] [-a auto|none]";; esac; done
CORES="$(nproc 2>/dev/null || echo 2)"
if [ -z "$AFF" ]; then
  if [ -n "${NEXUS_CPU_MAP:-}" ]; then AFF="map"
  elif [ -n "${NEXUS_WORKER_CPU_AFFINITY:-}" ]; then AFF="env"
  elif [ "$CORES" -ge "$N" ]; then AFF="auto"
  else AFF="none"; echo "NOTE: only $CORES logical CPU(s) for $N Workers - not pinning (each Worker would have to share a core)."; fi
fi
case "$AFF" in auto|none|map|env) ;; *) die "-a must be auto or none";; esac
if [ -z "$CAP" ]; then
  if [ "$AFF" = "none" ]; then CAP=$(( CORES / N )); [ "$CAP" -lt 2 ] && CAP=2; else CAP=1; fi
fi
cd "$ROOT"

if master_up; then echo "Master already running."; else
  echo "Starting Master..."
  nohup "$BIN/master" < /dev/null > "$LOG_DIR/master.log" 2>&1 &
  echo $! > "$RUN_DIR/master.pid"
  wait_for "the Master's HTTP API" 30 master_up || { tail -20 "$LOG_DIR/master.log"; die "Master did not start (see logs/master.log)"; }
fi

IFS=',' read -r -a CPUMAP <<< "${NEXUS_CPU_MAP:-}"
for i in $(seq 1 "$N"); do
  id="W$i"
  if [ -f "$RUN_DIR/worker_$id.pid" ] && kill -0 "$(cat "$RUN_DIR/worker_$id.pid")" 2>/dev/null; then echo "Worker $id already running."; continue; fi
  case "$AFF" in
    auto) wa="auto" ;;
    map)  wa="${CPUMAP[$((i-1))]:-none}" ;;
    env)  wa="$NEXUS_WORKER_CPU_AFFINITY" ;;
    *)    wa="none" ;;
  esac
  NEXUS_WORKER_CPU_AFFINITY="$wa" NEXUS_WORKER_INDEX="$((i-1))" NEXUS_WORKER_CAPACITY="$CAP" \
    nohup "$BIN/worker" "$id" < /dev/null > "$LOG_DIR/worker_$id.log" 2>&1 &
  echo $! > "$RUN_DIR/worker_$id.pid"
done

healthy() { [ "$(curl -fsS "$API/api/overview" | python3 -c 'import sys,json; print(json.load(sys.stdin)["workers"]["healthy"])')" -ge "$N" ]; }
wait_for "$N Workers to register" 30 healthy || die "Workers did not register (see logs/worker_*.log)"
echo "Cluster is up: Master + $N Workers (max $CAP concurrent task(s) each, CPU affinity mode: $AFF)."
echo "What each Worker REALLY got (verified by the Worker, shown on the dashboard Workers tab):"
curl -fsS "$API/api/workers" | python3 -c '
import sys, json
for w in sorted(json.load(sys.stdin), key=lambda x: x["worker_id"]):
    if w["healthy"]:
        print("  %-4s affinity=%-13s cpus=%-6s capacity=%s threads=%s  %s" % (w["worker_id"], w["affinity_status"], w["cpu_affinity"] or "-", w["capacity"], w["pool_threads"], ("" if w["affinity_status"] in ("APPLIED","NOT_REQUESTED") else "<- " + (w["affinity_detail"] or ""))))'
echo "Dashboard: $API/"
