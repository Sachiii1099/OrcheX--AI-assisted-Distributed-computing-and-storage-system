# Database schema (PostgreSQL)

Source of truth: `database/schema.sql` (+ `seed.sql` for the static `task_types` reference rows). Both files are **idempotent** (`CREATE TABLE IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`, `ON CONFLICT`) and never drop data, so running them again is safe and upgrades an older database in place.
Create a clean database: `./setup/setup.sh [--test-db]` (system PostgreSQL) or `./setup/local_pg.sh start` (private cluster). Re-apply: `./build/master --init-db`. No fake data is seeded; the system starts empty.
All application queries are parameterised (`$1…`); multi-statement changes use `Database::transact` or single CTE statements.

## Tables

| Table | Purpose | Key columns | Lifecycle |
|---|---|---|---|
| `workers` | registry of Workers | `worker_id` (PK), `hostname`, `ip_address`, `cpu_cores`, `memory_total_mb`, `capacity` (max concurrent tasks), **`pool_threads`, `logical_cpus`, `cpu_affinity`, `affinity_status`, `affinity_detail`**, live `cpu/ram/disk_usage`, `status` (ACTIVE/INACTIVE), `last_heartbeat` | upserted on registration; INACTIVE on timeout / Master start; the dashboard computes health from `last_heartbeat`, not from `status` |
| `resource_metrics` | history of Worker CPU/RAM/disk/running tasks | `worker_id` → workers, `timestamp`, `cpu_usage`, `ram_usage`, `disk_usage`, `running_tasks` | one row per RESOURCE_UPDATE; feeds the charts |
| `jobs` | a user-visible unit of work | `job_id`, `job_type` (task type or `MAPREDUCE_WORDCOUNT`), `status`, `priority`, `params`, `result`, `dataset_id` → datasets | PENDING/RUNNING → COMPLETED/FAILED |
| `tasks` | one schedulable task | `task_id`, `job_id` → jobs, `task_type`, `role` (STANDALONE/MAP/REDUCE), `partition_idx`, `block_id`, `input_bytes`, `priority`, `scale`, `status`, `assigned_worker`, `attempts`, `max_attempts`, `predicted_seconds`, `last_error`, `created_at/assigned_at/started_at/completed_at` | PENDING → ASSIGNED → RUNNING → COMPLETED, or back to PENDING on retry, FAILED when attempts are exhausted |
| `execution_history` | **one row per attempt**, measured on the Worker | `task_id`, `worker_id`, `task_type`, `execution_time`, `status`, `attempt`, `scale`, `input_bytes`, `worker_cpu_at_assign`, `worker_ram_at_assign`, `running_at_assign`, `worker_capacity`, `concurrent_at_start`, `started_at`, `finished_at`, `scheduler_policy`, `predicted_seconds`, `error` | append-only; the **training set** of the ML model; keeps failed attempts (retry history is never lost) |
| `scheduler_config` | persisted settings | `key='policy'`, `value` | updated by the GUI/API |
| `scheduling_decisions` | every assignment and why | `task_id`, `attempt`, `policy` (active), `effective_policy` (differs on AI fallback), `worker_id`, `score`, `predicted_seconds`, `fallback`, `reason`, `candidates` (JSON: metrics of every Worker at decision time) | append-only |
| `datasets` | uploaded files | `dataset_id`, `name`, `original_size`, `block_size`, `block_count`, `replication` (requested), `text_aligned`, `sha256` (whole file), `status` | UPLOADING → AVAILABLE / FAILED |
| `blocks` | blocks of a dataset | `block_id` (`ds<N>_b<i>`), `dataset_id`, `block_index`, `size_bytes`, `checksum` (SHA-256) | created in the same transaction as the replica rows |
| `block_replicas` | where each block physically is | PK (`block_id`, `worker_id`), `status` (HEALTHY/CORRUPT/MISSING), `stored_at`, `last_verified_at` | inserted only after the Worker acknowledged the physical write; updated by verify / repair |
| `job_stages` | MapReduce stages with timing | `job_id`, `stage_name` (SPLIT/MAP/SHUFFLE/REDUCE/FINALIZE), `status`, `task_count`, `started_at`, `completed_at` | one row per stage |
| `mr_intermediate` | map output per (map task, partition) | `job_id`, `map_task_id`, `partition_idx`, `data` | written when a map task completes |
| `mr_reduce_outputs` | marks stored reduce partitions | `job_id`, `partition_idx`, `task_id`, `words` | job finishes when all exist |
| `mr_results` | final word counts | `job_id`, `word`, `count` | written at FINALIZE |
| `ml_training_runs` | every training attempt | `run_id`, `status` (TRAINED/NOT_ACTIVATED/INSUFFICIENT_DATA/FAILED), **`model_type`** (xgboost/random_forest), `n_samples/n_train/n_test`, `mae`, `rmse`, `baseline_mae/rmse`, **`compare_model/mae/rmse`**, `n_trees` (= boosting rounds for XGBoost), `max_depth`, **`hyperparams`**, `model_path`, `message` | append-only |
| `ml_predictions` | predicted vs actual durations | `task_id`, `attempt`, `worker_id`, `run_id` → ml_training_runs, `predicted_seconds`, `actual_seconds` (filled on completion) | |
| `benchmark_runs` | one benchmark experiment | `benchmark_id`, `name`, `status` (RUNNING/COMPLETED/FAILED/CANCELLED), `workload` (JSON), `policies`, `n_workers`, `worker_ids`, `model_kind`, `model_run_id`, `note`, `started_at`, `finished_at` | RUNNING rows left by a crashed Master are marked FAILED at start |
| `benchmark_results` | measured result of one policy run | `benchmark_id`, `repeat_idx`, `policy`, `status`, `tasks_total/completed/failed`, `retries`, `makespan_s`, `avg_exec_s`, `avg_wait_s`, `throughput_tps`, `avg_cpu`, `avg_ram`, `local_tasks`, `block_tasks`, `ai_predictions`, `ai_fallbacks`, `pred_mae`, `pred_rmse`, `worker_stats` (JSON), `task_ids` | one row per (policy, repeat); every value computed from the task tables |
| `task_types` | static reference | `task_type`, `description` | seeded by `seed.sql` |

## Relationships

```
workers 1──* resource_metrics          jobs 1──* tasks 1──* execution_history *──1 workers
workers 1──* block_replicas *──1 blocks *──1 datasets        tasks 1──* scheduling_decisions *──1 workers
jobs 1──* job_stages   jobs 1──* mr_intermediate *──1 tasks   jobs 1──* mr_reduce_outputs   jobs 1──* mr_results
tasks 1──* ml_predictions *──0..1 ml_training_runs            benchmark_runs 1──* benchmark_results (task_ids → tasks)
```

## Retry and failure history

A retry never overwrites history: each attempt is its own `execution_history` row (status COMPLETED/FAILED, error text, Worker), each assignment its own `scheduling_decisions` row, and `tasks.attempts` counts them. A permanently failed task keeps `last_error`.

## Inspect

```bash
export PGPASSWORD="$NEXUS_DB_PASSWORD"
psql -h "$NEXUS_DB_HOST" -p "$NEXUS_DB_PORT" -U "$NEXUS_DB_USER" -d "$NEXUS_DB_NAME" -c '\dt'
psql ... -c "select policy, count(*) from scheduling_decisions group by 1;"
psql ... -c "select benchmark_id, policy, makespan_s, throughput_tps from benchmark_results order by 1,2;"
```
