#!/usr/bin/env bash
# Creates the PostgreSQL role + database for NEXUS and loads schema/seed. Idempotent.
#   ./setup/setup.sh              database "nexus"
#   ./setup/setup.sh --test-db    also database "nexus_test" (used by ./scripts/run_tests.sh)
# Reads NEXUS_DB_* from the environment (or from ./.env if present).
# Admin access: by default runs `sudo -u postgres psql`. Override, e.g.:
#   PSQL_ADMIN="psql -h localhost -U postgres" ./setup/setup.sh
# No sudo / system PostgreSQL?  Use  ./setup/local_pg.sh start  (private cluster in ./.pgdata).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
if [ -f "$ROOT/.env" ]; then set -a; . "$ROOT/.env"; set +a; fi

DB_HOST="${NEXUS_DB_HOST:-localhost}"
DB_PORT="${NEXUS_DB_PORT:-5432}"
DB_NAME="${NEXUS_DB_NAME:-nexus}"
DB_USER="${NEXUS_DB_USER:-nexus}"
: "${NEXUS_DB_PASSWORD:?Set NEXUS_DB_PASSWORD (see .env.example)}"
PSQL_ADMIN="${PSQL_ADMIN:-sudo -u postgres psql -p $DB_PORT}"
DATABASES=("$DB_NAME")
[ "${1:-}" = "--test-db" ] && DATABASES+=("${DB_NAME}_test")

echo "==> Creating role '$DB_USER' (if missing)"
$PSQL_ADMIN -v ON_ERROR_STOP=1 -v u="$DB_USER" -v pw="$NEXUS_DB_PASSWORD" -d postgres <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'u', :'pw')
  WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'u') \gexec
SELECT format('ALTER ROLE %I PASSWORD %L', :'u', :'pw') \gexec
SQL

export PGPASSWORD="$NEXUS_DB_PASSWORD"
for d in "${DATABASES[@]}"; do
  echo "==> Creating database '$d' (if missing)"
  $PSQL_ADMIN -v ON_ERROR_STOP=1 -v u="$DB_USER" -v d="$d" -d postgres <<'SQL'
SELECT format('CREATE DATABASE %I OWNER %I', :'d', :'u')
  WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'d') \gexec
SQL
  echo "==> Applying schema.sql and seed.sql to '$d' as '$DB_USER'"
  PSQL_APP="psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $d -v ON_ERROR_STOP=1 -q"
  $PSQL_APP -f "$ROOT/database/schema.sql"
  $PSQL_APP -f "$ROOT/database/seed.sql"
done
echo "==> Tables in '$DB_NAME':"
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -q -c '\dt'
echo "Done. Database '$DB_NAME' is ready."
