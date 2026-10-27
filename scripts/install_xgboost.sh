#!/usr/bin/env bash
# Builds the official XGBoost library (https://github.com/dmlc/xgboost, pinned release tag) from
# source into ./third_party/xgboost-install. No sudo, no Python and no pip are needed - NEXUS uses
# XGBoost through its C API (libxgboost.so). Takes about 5-15 minutes the first time.
#
#   ./scripts/install_xgboost.sh            build + install (skipped if already installed)
#   ./scripts/install_xgboost.sh --force    rebuild from scratch
#   XGBOOST_TAG=v2.1.4 JOBS=4 ./scripts/install_xgboost.sh
#
# Afterwards re-run ./scripts/build.sh : CMake finds the library and compiles NEXUS with XGBoost.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TAG="${XGBOOST_TAG:-v2.1.4}"
SRC="$ROOT/third_party/xgboost-src"
PREFIX="$ROOT/third_party/xgboost-install"
JOBS="${JOBS:-$(nproc 2>/dev/null || echo 2)}"

if [ "${1:-}" = "--force" ]; then rm -rf "$SRC" "$PREFIX"; fi
if [ -e "$PREFIX/lib/libxgboost.so" ]; then
  echo "XGBoost already installed in $PREFIX (use --force to rebuild)"; exit 0
fi
for t in git cmake g++ make; do command -v "$t" >/dev/null || { echo "missing tool: $t (sudo apt install git cmake build-essential)"; exit 1; }; done

mkdir -p "$ROOT/third_party"
if [ ! -d "$SRC/.git" ]; then
  echo "==> Cloning dmlc/xgboost $TAG (needs internet access to github.com)"
  git clone --depth 1 --branch "$TAG" --recursive --shallow-submodules https://github.com/dmlc/xgboost.git "$SRC"
fi
echo "==> Configuring"
mkdir -p "$SRC/build"
# CMAKE_POLICY_VERSION_MINIMUM lets CMake 4.x accept XGBoost's older cmake_minimum_required
cmake -S "$SRC" -B "$SRC/build" -DCMAKE_BUILD_TYPE=Release -DUSE_OPENMP=ON \
      -DCMAKE_INSTALL_PREFIX="$PREFIX" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 >"$SRC/build/configure.log" 2>&1 \
  || { tail -20 "$SRC/build/configure.log"; exit 1; }
echo "==> Building with $JOBS jobs (this is the slow part)"
cmake --build "$SRC/build" -j"$JOBS"
cmake --install "$SRC/build"
echo "XGBoost $TAG installed in $PREFIX"
echo "Next: ./scripts/build.sh"
