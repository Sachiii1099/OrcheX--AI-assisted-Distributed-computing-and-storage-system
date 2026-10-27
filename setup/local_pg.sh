#!/usr/bin/env bash
# Private, rootless PostgreSQL cluster for NEXUS (no sudo, no system service needed).
# Handy on WSL/lab machines where you cannot configure the system PostgreSQL.
#
#   ./setup/local_pg.sh start     create the cluster on first use, start it, create role + databases + schema
#   ./setup/local_pg.sh stop
#   ./setup/local_pg.sh status
#
# Data lives in ./.pgdata (git-ignored). Listens on 127.0.0.1:${NEXUS_DB_PORT:-54329} only.
# Needs the PostgreSQL *server* binaries (apt: postgresql) but does not use the system cluster.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
if [ -f "$ROOT/.env" ]; then set -a; . "$ROOT/.env"; set +a; fi

PGDATA="${NEXUS_PGDATA:-$ROOT/.pgdata}"
PORT="${NEXUS_DB_PORT:-54329}"
SOCKDIR="$PGDATA/sock"
BIN=""
for d in /usr/lib/postgresql/*/bin; do [ -x "$d/initdb" ] && BIN="$d"; done      # newest installed version
[ -n "$BIN" ] || { echo "PostgreSQL server binaries not found. Install: sudo apt install postgresql"; exit 1; }

case "${1:-}" in
  start)
    : "${NEXUS_DB_PASSWORD:?Set NEXUS_DB_PASSWORD (see .env.example)}"
    if [ ! -f "$PGDATA/PG_VERSION" ]; then
      echo "==> Initialising private cluster in $PGDATA"
      "$BIN/initdb" -D "$PGDATA" -A trust --auth-host=scram-sha-256 -E UTF8 >/dev/null
      mkdir -p "$SOCKDIR"
      { echo "port = $PORT"; echo "listen_addresses = '127.0.0.1'"; echo "unix_socket_directories = '$SOCKDIR'"; } >> "$PGDATA/postgresql.conf"
    fi
    mkdir -p "$SOCKDIR"
    if ! "$BIN/pg_ctl" -D "$PGDATA" status >/dev/null 2>&1; then
      echo "==> Starting PostgreSQL on 127.0.0.1:$PORT"
      "$BIN/pg_ctl" -D "$PGDATA" -l "$PGDATA/server.log" -w start >/dev/null
    fi
    export NEXUS_DB_HOST=127.0.0.1 NEXUS_DB_PORT="$PORT"
    export PSQL_ADMIN="psql -h $SOCKDIR -p $PORT -U $(id -un)"
    "$ROOT/setup/setup.sh" --test-db
    echo
    echo "Use these in every terminal:  export NEXUS_DB_HOST=127.0.0.1 NEXUS_DB_PORT=$PORT"
    ;;
  stop)
    "$BIN/pg_ctl" -D "$PGDATA" -m fast stop
    ;;
  status)
    "$BIN/pg_ctl" -D "$PGDATA" status
    ;;
  *)
    echo "usage: $0 start|stop|status"; exit 2 ;;
esac
