#!/usr/bin/env bash
# Runs every automated test and prints an honest summary.
#   ./scripts/run_tests.sh              unit tests + database tests + end-to-end integration test
#   ./scripts/run_tests.sh --unit       only the unit tests (no PostgreSQL needed)
#
# Database-backed tests use the database  ${NEXUS_DB_NAME:-nexus}_test  (they WIPE it).
# Create it once with:  ./setup/setup.sh --test-db     (or ./setup/local_pg.sh start)
# Tests that cannot run are reported as SKIPPED with the reason - never as passed.
. "$(dirname "$0")/common.sh"
[ -x "$BIN/master" ] || "$ROOT/scripts/build.sh" || die "build failed"
cd "$BIN"

declare -A RESULT
run() {   # run <name> <command...>
  local name="$1"; shift
  echo; echo "################ $name"
  "$@"; local rc=$?
  case $rc in 0) RESULT[$name]="PASSED";; 77) RESULT[$name]="SKIPPED (see reason above)";; *) RESULT[$name]="FAILED (exit $rc)";; esac
}

# ---- unit tests (pure C++, no database) ----
for t in test_message test_scheduler test_worker_exec test_storage test_ml test_http test_affinity; do run "$t" "./$t"; done

[ "${1:-}" = "--unit" ] && DB_TESTS=0 || DB_TESTS=1
if [ "$DB_TESTS" = 1 ]; then
  export NEXUS_DB_NAME="${NEXUS_DB_NAME:-nexus}"
  case "$NEXUS_DB_NAME" in *_test) ;; *) export NEXUS_DB_NAME="${NEXUS_DB_NAME}_test";; esac
  if [ -z "${NEXUS_DB_PASSWORD:-}" ]; then
    echo; echo "SKIPPED database + integration tests: NEXUS_DB_PASSWORD is not set (copy .env.example to .env)."
    RESULT[test_db]="SKIPPED (NEXUS_DB_PASSWORD not set)"; RESULT[integration_test]="SKIPPED (NEXUS_DB_PASSWORD not set)"
  elif ! PGPASSWORD="$NEXUS_DB_PASSWORD" psql -h "${NEXUS_DB_HOST:-localhost}" -p "${NEXUS_DB_PORT:-5432}" -U "${NEXUS_DB_USER:-nexus}" -d "$NEXUS_DB_NAME" -c 'select 1' >/dev/null 2>&1; then
    echo; echo "SKIPPED database + integration tests: cannot connect to database '$NEXUS_DB_NAME'. Run ./setup/setup.sh --test-db first."
    RESULT[test_db]="SKIPPED (database '$NEXUS_DB_NAME' unreachable)"; RESULT[integration_test]="SKIPPED (database '$NEXUS_DB_NAME' unreachable)"
  else
    "$BIN/master" --init-db >/dev/null 2>&1 || die "could not apply the schema to $NEXUS_DB_NAME"
    run test_db "./test_db"
    run integration_test python3 "$ROOT/tests/integration_test.py"
  fi
fi

echo; echo "==================== TEST SUMMARY ===================="
rc=0
for k in test_message test_scheduler test_worker_exec test_storage test_ml test_http test_affinity test_db integration_test; do
  [ -n "${RESULT[$k]:-}" ] || continue
  printf '  %-18s %s\n' "$k" "${RESULT[$k]}"
  case "${RESULT[$k]}" in FAILED*) rc=1;; esac
done
exit $rc
