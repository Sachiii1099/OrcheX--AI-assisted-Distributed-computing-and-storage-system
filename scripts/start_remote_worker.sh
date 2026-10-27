#!/usr/bin/env bash
# Start ONE Worker on a computer that is NOT the Master computer (foreground).
#   ./scripts/start_remote_worker.sh <WORKER_ID> <MASTER_IP_OR_HOSTNAME> [master_port] [cpu_affinity]
# Example:  ./scripts/start_remote_worker.sh PC2-W1 192.168.1.20 5050 auto
# The Master address must be reachable from THIS computer. 127.0.0.1 / localhost would point at this
# computer itself, so it is refused here. Needs: the project built on this computer (./scripts/build.sh;
# libpq-dev is still required by CMake even though Workers never touch PostgreSQL).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
[ -x "$ROOT/build/worker" ] || { echo "build first: ./scripts/build.sh"; exit 1; }
ID="${1:-}"; HOST="${2:-}"; PORT="${3:-${NEXUS_MASTER_PORT:-5050}}"; AFF="${4:-${NEXUS_WORKER_CPU_AFFINITY:-none}}"
[ -n "$ID" ] && [ -n "$HOST" ] || { echo "usage: $0 <WORKER_ID> <MASTER_IP_OR_HOSTNAME> [port] [cpu_affinity]"; exit 2; }
case "$HOST" in
  127.*|localhost|::1) echo "ERROR: '$HOST' is this computer, not the Master. Use the Master computer's LAN IP (run 'hostname -I' there)."; exit 2;;
esac
echo "Checking that $HOST:$PORT is reachable ..."
if ! timeout 5 bash -c "exec 3<>/dev/tcp/$HOST/$PORT" 2>/dev/null; then
  echo "ERROR: cannot open a TCP connection to $HOST:$PORT. Check: Master running? NEXUS_MASTER_BIND=0.0.0.0? firewall/port $PORT open?"
  echo "       (WSL2 note: the Master's Windows host must forward the port, see docs/RUN_GUIDE_WINDOWS_WSL.md section 'Workers on other computers')"
  exit 1
fi
cd "$ROOT"
export NEXUS_WORKER_CPU_AFFINITY="$AFF"
exec "$ROOT/build/worker" "$ID" "$HOST" "$PORT"
