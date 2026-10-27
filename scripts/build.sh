#!/usr/bin/env bash
# Configure + compile NEXUS. Binaries end up in build/master, build/worker, build/nexus_ml
# (and the test programs). Extra CMake flags can be passed through, e.g.
#   ./scripts/build.sh -DPostgreSQL_INCLUDE_DIR=/usr/include/postgresql
#   ./scripts/build.sh --clean            delete build/ first (a from-scratch build)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
if [ "${1:-}" = "--clean" ]; then shift; rm -rf "$ROOT/build"; fi
cmake -S "$ROOT" -B "$ROOT/build" -DCMAKE_BUILD_TYPE=Release "$@"
cmake --build "$ROOT/build" -j"$(nproc 2>/dev/null || echo 2)"
echo
echo "Built: $ROOT/build/master  $ROOT/build/worker  $ROOT/build/nexus_ml"
