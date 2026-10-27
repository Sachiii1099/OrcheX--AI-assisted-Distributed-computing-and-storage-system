#!/usr/bin/env bash
# Train / evaluate the task-duration model from the execution history in PostgreSQL.
#   ./scripts/train_model.sh [xgboost|random_forest]   train (default xgboost; through the running Master if there is one, so it is loaded at once)
#   ./scripts/train_model.sh --offline   train with the stand-alone tool (Master restart needed to load the model)
#   ./scripts/train_model.sh --status    list the latest training runs
# The model is trained ONLY on real rows of execution_history. With fewer than NEXUS_ML_MIN_SAMPLES (default 30)
# completed executions nothing is trained and the status is INSUFFICIENT_DATA - run more tasks first.
. "$(dirname "$0")/common.sh"
[ -x "$BIN/nexus_ml" ] || die "not built yet - run ./scripts/build.sh"
need_db_env
cd "$ROOT"
case "${1:-}" in
  --status)  exec "$BIN/nexus_ml" status ;;
  --offline) exec "$BIN/nexus_ml" train ;;
  xgboost|random_forest) if master_up; then "$ROOT/scripts/nexus_cli.sh" train "$1"; else exec "$BIN/nexus_ml" train "$1"; fi ;;
  *) if master_up; then "$ROOT/scripts/nexus_cli.sh" train; else exec "$BIN/nexus_ml" train; fi ;;
esac
