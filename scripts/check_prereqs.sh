#!/usr/bin/env bash
# Checks (and explains how to install) everything NEXUS needs. Changes nothing.
ok=0
chk() {  # chk <label> <command-that-succeeds-when-present> <install hint>
  if eval "$2" >/dev/null 2>&1; then printf '  [ok]      %s\n' "$1"; else printf '  [MISSING] %s   ->  %s\n' "$1" "$3"; ok=1; fi
}
echo "NEXUS prerequisites (Ubuntu / Debian / WSL):"
chk "Linux or WSL (needs /proc)"           "[ -r /proc/stat ]"                         "run inside WSL: wsl --install -d Ubuntu"
chk "g++ with C++17"                        "echo 'int main(){}' | g++ -std=c++17 -x c++ - -o /dev/null" "sudo apt install build-essential"
chk "CMake >= 3.10"                         "cmake --version"                           "sudo apt install cmake"
chk "libpq headers (libpq-dev)"             "[ -e /usr/include/postgresql/libpq-fe.h ] || [ -e /usr/include/libpq-fe.h ]" "sudo apt install libpq-dev"
chk "psql client"                           "command -v psql"                           "sudo apt install postgresql-client"
chk "PostgreSQL server (system or private)" "command -v pg_ctl || ls /usr/lib/postgresql/*/bin/initdb" "sudo apt install postgresql"
chk "python3 (tests + CLI helpers)"         "command -v python3"                        "sudo apt install python3"
chk "curl"                                  "command -v curl"                           "sudo apt install curl"
chk "git (to download XGBoost)"                "command -v git"                            "sudo apt install git"
XR="$(cd "$(dirname "$0")/.." && pwd)/third_party/xgboost-install"
if [ -e "$XR/lib/libxgboost.so" ] || [ -e /usr/local/lib/libxgboost.so ]; then echo "  [ok]      XGBoost library"; else echo "  [todo]    XGBoost library not built  ->  ./scripts/install_xgboost.sh   (without it NEXUS runs with Random Forest only)"; fi
command -v zip >/dev/null 2>&1 && echo "  [ok]      zip (optional, only for packaging)" || echo "  [optional] zip not installed (only needed to package the project)"
echo "Configuration:"
if [ -f "$(dirname "$0")/../.env" ]; then echo "  [ok]      .env present"; else echo "  [todo]    cp .env.example .env   and set NEXUS_DB_PASSWORD"; fi
if [ -n "${NEXUS_DB_PASSWORD:-}" ] || grep -q '^NEXUS_DB_PASSWORD=.\+' "$(dirname "$0")/../.env" 2>/dev/null; then echo "  [ok]      NEXUS_DB_PASSWORD is set"; else echo "  [todo]    NEXUS_DB_PASSWORD is not set"; fi
[ $ok -eq 0 ] && echo "All required tools found." || echo "Install the missing items above, then re-run."
exit $ok
