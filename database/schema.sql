-- NEXUS PostgreSQL schema. IDEMPOTENT: safe to run any number of times, and it
-- upgrades a database created by an earlier NEXUS version (CREATE ... IF NOT
-- EXISTS, then ALTER ... ADD COLUMN IF NOT EXISTS).
--
-- PostgreSQL stores METADATA and results: worker state, metrics, jobs, tasks,
-- scheduling decisions, execution history, dataset/block/replica catalogue and
-- ML runs. The bytes of every storage block live in the Workers' directories,
-- never in PostgreSQL.

SET client_min_messages = warning;      -- silence "already exists, skipping" notices

-- =========================================================== cluster ========
CREATE TABLE IF NOT EXISTS workers (
    worker_id       VARCHAR(64) PRIMARY KEY,
    hostname        VARCHAR(255),
    ip_address      VARCHAR(64),
    cpu_cores       INTEGER,
    memory_total_mb BIGINT,
    cpu_usage       DOUBLE PRECISION NOT NULL DEFAULT 0,   -- percent (whole host)
    ram_usage       DOUBLE PRECISION NOT NULL DEFAULT 0,   -- percent (whole host)
    disk_usage      DOUBLE PRECISION NOT NULL DEFAULT 0,   -- percent (storage filesystem)
    status          VARCHAR(16) NOT NULL DEFAULT 'INACTIVE'
                    CHECK (status IN ('ACTIVE', 'INACTIVE')),
    registered_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat  TIMESTAMPTZ
);
ALTER TABLE workers ADD COLUMN IF NOT EXISTS capacity         INTEGER NOT NULL DEFAULT 1;
ALTER TABLE workers ADD COLUMN IF NOT EXISTS running_tasks    INTEGER NOT NULL DEFAULT 0;
ALTER TABLE workers ADD COLUMN IF NOT EXISTS ram_available_mb BIGINT;
ALTER TABLE workers ADD COLUMN IF NOT EXISTS process_rss_mb   DOUBLE PRECISION;
ALTER TABLE workers ADD COLUMN IF NOT EXISTS load1            DOUBLE PRECISION;
ALTER TABLE workers ADD COLUMN IF NOT EXISTS storage_bytes    BIGINT NOT NULL DEFAULT 0;
ALTER TABLE workers ADD COLUMN IF NOT EXISTS last_resource_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS jobs (
    job_id       SERIAL PRIMARY KEY,
    job_type     VARCHAR(64) NOT NULL,
    status       VARCHAR(16) NOT NULL DEFAULT 'PENDING'
                 CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED')),
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS priority   INTEGER NOT NULL DEFAULT 5;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS params     TEXT;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS result     TEXT;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS dataset_id INTEGER;

CREATE TABLE IF NOT EXISTS tasks (
    task_id         SERIAL PRIMARY KEY,
    job_id          INTEGER NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    task_type       VARCHAR(64) NOT NULL,
    params          TEXT,
    assigned_worker VARCHAR(64) REFERENCES workers(worker_id),
    status          VARCHAR(16) NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING', 'ASSIGNED', 'RUNNING', 'COMPLETED', 'FAILED')),
    result          TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    assigned_at     TIMESTAMPTZ,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ
);
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS priority          INTEGER NOT NULL DEFAULT 5
                                                             CHECK (priority BETWEEN 0 AND 9);
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS scale             INTEGER NOT NULL DEFAULT 1;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS attempts          INTEGER NOT NULL DEFAULT 0;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS max_attempts      INTEGER NOT NULL DEFAULT 3;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS role              VARCHAR(16) NOT NULL DEFAULT 'STANDALONE'
                                                             CHECK (role IN ('STANDALONE', 'MAP', 'REDUCE'));
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS partition_idx     INTEGER;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS block_id          VARCHAR(64);
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS input_bytes       BIGINT NOT NULL DEFAULT 0;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS predicted_seconds DOUBLE PRECISION;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS last_error        TEXT;

CREATE TABLE IF NOT EXISTS resource_metrics (
    metric_id  BIGSERIAL PRIMARY KEY,
    worker_id  VARCHAR(64) NOT NULL REFERENCES workers(worker_id) ON DELETE CASCADE,
    "timestamp" TIMESTAMPTZ NOT NULL DEFAULT now(),
    cpu_usage  DOUBLE PRECISION,
    ram_usage  DOUBLE PRECISION,
    disk_usage DOUBLE PRECISION,
    status     VARCHAR(16)
);
ALTER TABLE resource_metrics ADD COLUMN IF NOT EXISTS running_tasks    INTEGER;
ALTER TABLE resource_metrics ADD COLUMN IF NOT EXISTS ram_available_mb BIGINT;

-- One row per finished task attempt. The "*_at_assign" columns and scale /
-- input_bytes are exactly the features that exist when the scheduler decides, so
-- this table is the (real) training set of the execution-time model.
CREATE TABLE IF NOT EXISTS execution_history (
    execution_id         SERIAL PRIMARY KEY,
    task_id              INTEGER NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    worker_id            VARCHAR(64) NOT NULL REFERENCES workers(worker_id),
    task_type            VARCHAR(64),
    execution_time       DOUBLE PRECISION,              -- seconds, measured on the Worker
    status               VARCHAR(16) NOT NULL,          -- COMPLETED / FAILED
    worker_cpu_at_assign DOUBLE PRECISION,
    worker_ram_at_assign DOUBLE PRECISION,
    scheduler_policy     VARCHAR(32),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE execution_history ADD COLUMN IF NOT EXISTS attempt             INTEGER;
ALTER TABLE execution_history ADD COLUMN IF NOT EXISTS scale               INTEGER;
ALTER TABLE execution_history ADD COLUMN IF NOT EXISTS input_bytes         BIGINT;
ALTER TABLE execution_history ADD COLUMN IF NOT EXISTS running_at_assign   INTEGER;
ALTER TABLE execution_history ADD COLUMN IF NOT EXISTS worker_capacity     INTEGER;
ALTER TABLE execution_history ADD COLUMN IF NOT EXISTS started_at          TIMESTAMPTZ;
ALTER TABLE execution_history ADD COLUMN IF NOT EXISTS finished_at         TIMESTAMPTZ;
ALTER TABLE execution_history ADD COLUMN IF NOT EXISTS concurrent_at_start INTEGER;
ALTER TABLE execution_history ADD COLUMN IF NOT EXISTS predicted_seconds   DOUBLE PRECISION;
ALTER TABLE execution_history ADD COLUMN IF NOT EXISTS error               TEXT;

-- ======================================================== scheduling ========
CREATE TABLE IF NOT EXISTS scheduler_config (
    key        VARCHAR(64) PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Every assignment, with the metrics the decision was based on.
CREATE TABLE IF NOT EXISTS scheduling_decisions (
    decision_id       BIGSERIAL PRIMARY KEY,
    task_id           INTEGER NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    attempt           INTEGER NOT NULL DEFAULT 1,
    policy            VARCHAR(32) NOT NULL,        -- policy that was ACTIVE
    effective_policy  VARCHAR(32) NOT NULL,        -- policy actually used (differs on AI fallback)
    worker_id         VARCHAR(64) NOT NULL REFERENCES workers(worker_id),
    score             DOUBLE PRECISION,
    predicted_seconds DOUBLE PRECISION,
    fallback          BOOLEAN NOT NULL DEFAULT FALSE,
    reason            TEXT,
    candidates        TEXT,                        -- per-worker metrics at decision time (JSON text)
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ========================================================= storage ==========
CREATE TABLE IF NOT EXISTS datasets (
    dataset_id    SERIAL PRIMARY KEY,
    name          VARCHAR(255) NOT NULL,
    original_size BIGINT NOT NULL CHECK (original_size > 0),
    block_size    INTEGER NOT NULL CHECK (block_size > 0),
    block_count   INTEGER NOT NULL DEFAULT 0,
    replication   INTEGER NOT NULL CHECK (replication >= 1),   -- requested replication factor
    text_aligned  BOOLEAN NOT NULL DEFAULT FALSE,
    sha256        CHAR(64) NOT NULL,                           -- of the whole original file
    status        VARCHAR(16) NOT NULL DEFAULT 'UPLOADING'
                  CHECK (status IN ('UPLOADING', 'AVAILABLE', 'FAILED')),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS blocks (
    block_id    VARCHAR(64) PRIMARY KEY,                       -- "ds<dataset>_b<index>"
    dataset_id  INTEGER NOT NULL REFERENCES datasets(dataset_id) ON DELETE CASCADE,
    block_index INTEGER NOT NULL CHECK (block_index >= 0),
    size_bytes  INTEGER NOT NULL CHECK (size_bytes > 0),
    checksum    CHAR(64) NOT NULL,                             -- SHA-256 of the block bytes
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (dataset_id, block_index)
);

CREATE TABLE IF NOT EXISTS block_replicas (
    block_id         VARCHAR(64) NOT NULL REFERENCES blocks(block_id) ON DELETE CASCADE,
    worker_id        VARCHAR(64) NOT NULL REFERENCES workers(worker_id),
    status           VARCHAR(16) NOT NULL DEFAULT 'HEALTHY'
                     CHECK (status IN ('HEALTHY', 'CORRUPT', 'MISSING')),
    stored_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_verified_at TIMESTAMPTZ,
    PRIMARY KEY (block_id, worker_id)
);

ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_dataset_fk;
ALTER TABLE jobs ADD CONSTRAINT jobs_dataset_fk FOREIGN KEY (dataset_id)
    REFERENCES datasets(dataset_id) ON DELETE SET NULL;

-- ======================================================= mapreduce ==========
CREATE TABLE IF NOT EXISTS job_stages (
    stage_id        SERIAL PRIMARY KEY,
    job_id          INTEGER NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    stage_name      VARCHAR(16) NOT NULL CHECK (stage_name IN ('SPLIT', 'MAP', 'SHUFFLE', 'REDUCE', 'FINALIZE')),
    status          VARCHAR(16) NOT NULL DEFAULT 'RUNNING'
                    CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED')),
    task_count      INTEGER NOT NULL DEFAULT 0,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ,
    UNIQUE (job_id, stage_name)
);

-- Map output, one row per (map task, reduce partition). Flows through the Master.
CREATE TABLE IF NOT EXISTS mr_intermediate (
    job_id        INTEGER NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    map_task_id   INTEGER NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    partition_idx INTEGER NOT NULL,
    data          TEXT NOT NULL,                               -- "word<TAB>count" lines
    PRIMARY KEY (job_id, map_task_id, partition_idx)
);

-- One row per reduce partition whose output has been stored in mr_results. The job is
-- finalised only when all of them exist (a task is COMPLETED slightly before its output is stored).
CREATE TABLE IF NOT EXISTS mr_reduce_outputs (
    job_id        INTEGER NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    partition_idx INTEGER NOT NULL,
    task_id       INTEGER NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    words         INTEGER NOT NULL,
    stored_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, partition_idx)
);

CREATE TABLE IF NOT EXISTS mr_results (
    job_id INTEGER NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    word   TEXT NOT NULL,
    count  BIGINT NOT NULL,
    PRIMARY KEY (job_id, word)
);

-- ============================================================== ML ==========
CREATE TABLE IF NOT EXISTS ml_training_runs (
    run_id        SERIAL PRIMARY KEY,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ,
    status        VARCHAR(32) NOT NULL
                  CHECK (status IN ('TRAINED', 'NOT_ACTIVATED', 'INSUFFICIENT_DATA', 'FAILED')),
    n_samples     INTEGER NOT NULL DEFAULT 0,
    n_train       INTEGER NOT NULL DEFAULT 0,
    n_test        INTEGER NOT NULL DEFAULT 0,
    mae           DOUBLE PRECISION,                            -- seconds, on the held-out test split
    rmse          DOUBLE PRECISION,
    baseline_mae  DOUBLE PRECISION,                            -- "always predict the training mean"
    baseline_rmse DOUBLE PRECISION,
    n_trees       INTEGER,
    max_depth     INTEGER,
    model_path    TEXT,
    message       TEXT
);

CREATE TABLE IF NOT EXISTS ml_predictions (
    prediction_id     BIGSERIAL PRIMARY KEY,
    task_id           INTEGER NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    attempt           INTEGER NOT NULL DEFAULT 1,
    worker_id         VARCHAR(64) NOT NULL REFERENCES workers(worker_id),
    run_id            INTEGER REFERENCES ml_training_runs(run_id) ON DELETE SET NULL,
    predicted_seconds DOUBLE PRECISION NOT NULL,
    actual_seconds    DOUBLE PRECISION,                        -- filled in when the task finishes
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ========================================================= indexes ==========
CREATE INDEX IF NOT EXISTS idx_workers_status       ON workers(status);
CREATE INDEX IF NOT EXISTS idx_tasks_job            ON tasks(job_id);
CREATE INDEX IF NOT EXISTS idx_tasks_worker_status  ON tasks(assigned_worker, status);
CREATE INDEX IF NOT EXISTS idx_tasks_status         ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_metrics_worker_time  ON resource_metrics(worker_id, "timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_exec_worker          ON execution_history(worker_id);
CREATE INDEX IF NOT EXISTS idx_exec_task_type       ON execution_history(task_type);
CREATE INDEX IF NOT EXISTS idx_exec_task            ON execution_history(task_id);
CREATE INDEX IF NOT EXISTS idx_decisions_task       ON scheduling_decisions(task_id);
CREATE INDEX IF NOT EXISTS idx_blocks_dataset       ON blocks(dataset_id);
CREATE INDEX IF NOT EXISTS idx_replicas_worker      ON block_replicas(worker_id);
CREATE INDEX IF NOT EXISTS idx_pred_task            ON ml_predictions(task_id);

-- ================================================ v2 additions (idempotent) ====
-- Worker CPU allocation: affinity (which CPUs), pool_threads (executor threads) and capacity
-- (max concurrent tasks) are three separate settings; the Worker reports what it really applied.
ALTER TABLE workers ADD COLUMN IF NOT EXISTS pool_threads     INTEGER;
ALTER TABLE workers ADD COLUMN IF NOT EXISTS logical_cpus     INTEGER;
ALTER TABLE workers ADD COLUMN IF NOT EXISTS cpu_affinity     TEXT;          -- e.g. '2' or '0-1'; '' = not pinned
ALTER TABLE workers ADD COLUMN IF NOT EXISTS affinity_status  VARCHAR(16);   -- NOT_REQUESTED | APPLIED | PARTIAL | FAILED
ALTER TABLE workers ADD COLUMN IF NOT EXISTS affinity_detail  TEXT;

-- ML: which algorithm produced a run, its hyper-parameters, and the other algorithm on the same split.
ALTER TABLE ml_training_runs ADD COLUMN IF NOT EXISTS model_type    VARCHAR(32) NOT NULL DEFAULT 'random_forest';
ALTER TABLE ml_training_runs ADD COLUMN IF NOT EXISTS hyperparams   TEXT;
ALTER TABLE ml_training_runs ADD COLUMN IF NOT EXISTS compare_model VARCHAR(32);
ALTER TABLE ml_training_runs ADD COLUMN IF NOT EXISTS compare_mae   DOUBLE PRECISION;
ALTER TABLE ml_training_runs ADD COLUMN IF NOT EXISTS compare_rmse  DOUBLE PRECISION;

-- Scheduling benchmarks: the same workload run once per policy, results measured from the DB.
CREATE TABLE IF NOT EXISTS benchmark_runs (
    benchmark_id  SERIAL PRIMARY KEY,
    name          VARCHAR(128) NOT NULL,
    status        VARCHAR(16) NOT NULL DEFAULT 'RUNNING'
                  CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED')),
    workload      TEXT NOT NULL,                 -- JSON: type, count, scale, priorities, block dataset ...
    policies      TEXT NOT NULL,                 -- comma separated, in execution order
    n_workers     INTEGER,
    worker_ids    TEXT,
    model_kind    VARCHAR(32),                   -- model available to AI_ASSISTED at start (xgboost | random_forest | none)
    model_run_id  INTEGER,
    note          TEXT,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS benchmark_results (
    result_id       SERIAL PRIMARY KEY,
    benchmark_id    INTEGER NOT NULL REFERENCES benchmark_runs(benchmark_id) ON DELETE CASCADE,
    repeat_idx      INTEGER NOT NULL DEFAULT 1,
    policy          VARCHAR(32) NOT NULL,
    status          VARCHAR(16) NOT NULL,        -- COMPLETED | PARTIAL | TIMEOUT | FAILED
    tasks_total     INTEGER NOT NULL DEFAULT 0,
    tasks_completed INTEGER NOT NULL DEFAULT 0,
    tasks_failed    INTEGER NOT NULL DEFAULT 0,
    retries         INTEGER NOT NULL DEFAULT 0,  -- sum(attempts - 1)
    makespan_s      DOUBLE PRECISION,            -- last completion - first submission
    avg_exec_s      DOUBLE PRECISION,
    avg_wait_s      DOUBLE PRECISION,            -- submission -> start of execution
    throughput_tps  DOUBLE PRECISION,            -- completed tasks / makespan
    avg_cpu         DOUBLE PRECISION,            -- worker CPU% sampled at assignment
    avg_ram         DOUBLE PRECISION,
    local_tasks     INTEGER NOT NULL DEFAULT 0,  -- block tasks that ran on a replica holder
    block_tasks     INTEGER NOT NULL DEFAULT 0,
    ai_predictions  INTEGER NOT NULL DEFAULT 0,
    ai_fallbacks    INTEGER NOT NULL DEFAULT 0,
    pred_mae        DOUBLE PRECISION,            -- |predicted - actual| over this run (AI_ASSISTED only)
    pred_rmse       DOUBLE PRECISION,
    worker_stats    TEXT,                        -- JSON: per worker tasks, busy seconds, utilization
    task_ids        TEXT,                        -- comma separated
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    error           TEXT
);
CREATE INDEX IF NOT EXISTS idx_bench_results_run ON benchmark_results(benchmark_id);
