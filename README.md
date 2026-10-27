# NEXUS – AI-Assisted, Resource-Aware Distributed Data Processing and Storage System

A Hadoop-inspired educational system in **C++17** (POSIX sockets, `std::thread`, libpq) with a **PostgreSQL** metadata store, a
zero-dependency **web dashboard**, **XGBoost** task-duration prediction (the real `dmlc/xgboost` library through its C API; a
from-scratch Random Forest is kept as a separately labelled alternative), and shell/Python tooling.

| Area | What is implemented (all of it runs against real processes – see [docs/FINAL_TEST_REPORT.md](docs/FINAL_TEST_REPORT.md)) |
|---|---|
| Master–Worker over TCP | registration with unique ids, heartbeats, OS resource reports, health from heartbeat age, task assignment/results, safe handling of crashes, timeouts and malformed messages; configurable Master bind address (`NEXUS_MASTER_BIND`) and Worker target (`NEXUS_MASTER_HOST`) |
| Multi-core processing | **three separate settings**: CPU affinity (`NEXUS_WORKER_CPU_AFFINITY`, Linux `sched_setaffinity`, verified by reading the mask back), executor threads (`NEXUS_WORKER_THREADS`) and max concurrent tasks (`NEXUS_WORKER_CAPACITY`); the real result is shown on the dashboard |
| Distributed storage | files are cut into checksummed blocks, replicated on **distinct** Workers, bytes stored on the Workers' disks, metadata in PostgreSQL; verify / repair / download / reconstruct |
| MapReduce | word count over the stored blocks: SPLIT → MAP (data-local) → SHUFFLE → REDUCE → FINALIZE, every stage timed |
| Scheduling | **FCFS, Round Robin, Priority, Resource-Aware, AI-Assisted (XGBoost)**, switchable at run time from the GUI; every decision (with the metrics it used) is stored |
| Algorithm comparison | **Benchmarks tab**: the same seeded workload is run once per policy; makespan, waiting time, throughput, retries, utilisation, locality, predicted-vs-actual are *measured* and stored in PostgreSQL |
| AI | XGBoost regression on the cluster's own `execution_history`; held-out MAE/RMSE next to a mean-duration baseline and Random Forest; honest `INSUFFICIENT_DATA`; deterministic fallback; background auto-retrain |
| Dashboard | cluster, workers (incl. CPU pinning), tasks, scheduling, benchmarks, storage, MapReduce, resource charts, concurrency timeline, model status, logs |

> **Workers on one computer share that computer's CPU cores and RAM.** Separate Worker *processes* give parallelism but **not** physical-machine
> fault tolerance. The multi-machine path (`NEXUS_MASTER_HOST`, `scripts/start_remote_worker.sh`) uses the same TCP code but was **not tested on real
> second hardware** – see [docs/FINAL_TEST_REPORT.md](docs/FINAL_TEST_REPORT.md) §Not verified.

## Documentation

| Document | Content |
|---|---|
| [docs/RUN_GUIDE_WINDOWS_WSL.md](docs/RUN_GUIDE_WINDOWS_WSL.md) | **start here**: copy-paste setup and run guide for Windows + WSL/Ubuntu + VS Code |
| [docs/PROJECT_OVERVIEW.md](docs/PROJECT_OVERVIEW.md) | problem, goals, features, limitations, technology |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | components, message flow, data flow, failure handling |
| [docs/FILE_BY_FILE_GUIDE.md](docs/FILE_BY_FILE_GUIDE.md) | every source file and its key functions |
| [docs/SCHEDULING_ALGORITHMS.md](docs/SCHEDULING_ALGORITHMS.md) | the five policies, pseudocode, benchmark methodology |
| [docs/DISTRIBUTED_STORAGE.md](docs/DISTRIBUTED_STORAGE.md) | blocks, checksums, replicas, locality, recovery, where files live |
| [docs/MACHINE_LEARNING.md](docs/MACHINE_LEARNING.md) | XGBoost model, features, split, metrics, persistence |
| [docs/DATABASE_SCHEMA.md](docs/DATABASE_SCHEMA.md) | every table |
| [docs/TESTING_AND_VALIDATION.md](docs/TESTING_AND_VALIDATION.md), [docs/FINAL_TEST_REPORT.md](docs/FINAL_TEST_REPORT.md) | what was tested, with real results; what was not |
| [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) | common errors and fixes |
| [docs/VIVA_GUIDE.md](docs/VIVA_GUIDE.md), [docs/IMPLEMENTATION_CHECKLIST.md](docs/IMPLEMENTATION_CHECKLIST.md) | viva concept map, requirement status |

---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------

## 1. Architecture

```
                      Browser  ──HTTP/JSON──┐
                curl / scripts/nexus_cli.sh ─┤
                                             ▼
 ┌────────────────────────────────── MASTER (one process) ────────────────────────────────────┐
 │  HTTP API + dashboard   (master/http_server, api)                                           │
 │  Console (stdin)                                                                           │
 │                                                                                            │
 │  TaskManager ── dispatch thread ── Scheduler(policy) ──► TASK_ASSIGN                       │
 │     │  guarded state machine           FCFS / RR / PRIORITY / RESOURCE_AWARE / AI          │
 │     ├─ MapReduce coordinator ◄── task events                      ▲ predictions            │
 │     ├─ StorageManager (blocks, replicas, verify, repair)      MlManager (XGBoost)          │
 │     └─ WorkerManager (registry, heartbeat age, RPC channel)                                │
 │  MasterServer: accept thread + one reader thread per Worker + heartbeat monitor            │
 └───────────────┬───────────────────────────────────────────────┬─────────────────────────────┘
                 │ TCP line protocol  TYPE|key=value|...          │ libpq (parameterised SQL)
        ┌────────┴─────────┬──────────────────┐                   ▼
        ▼                  ▼                  ▼             PostgreSQL  (metadata, history, decisions,
   WORKER W1          WORKER W2          WORKER W3           datasets/blocks/replicas, ML runs)
   ┌─────────────┐
   │ receive loop│ ← BLOCK_STORE / FETCH / VERIFY / DELETE  (small disk jobs, run inline)
   │ thread pool │ ← TASK_ASSIGN → up to `capacity` tasks truly in parallel
   │ heartbeat   │ → HEARTBEAT + RESOURCE_UPDATE (CPU, RAM, disk, load, tasks running)
   │ BlockStore  │ → worker_data/<ID>/blocks/<block_id>.blk   (the actual bytes)
   └─────────────┘
```
More detail (message table, state machine, ER overview, thread model): [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).
Viva / PBL concept map: [docs/VIVA_GUIDE.md](docs/VIVA_GUIDE.md). Requirement-by-requirement status: [docs/IMPLEMENTATION_CHECKLIST.md](docs/IMPLEMENTATION_CHECKLIST.md).

## 2. Dependencies (Ubuntu / Debian / WSL)

```bash
sudo apt update
sudo apt install -y build-essential cmake git libpq-dev postgresql postgresql-client python3 curl
./scripts/check_prereqs.sh        # tells you what is still missing
./scripts/install_xgboost.sh      # builds the official XGBoost library (needs internet, ~10 min, no sudo, no pip)
```
The only third-party libraries are libpq and XGBoost (built from the official source tag by `scripts/install_xgboost.sh` into `third_party/xgboost-install`). SHA-256, base64, JSON writer, HTTP server, thread pool and the Random Forest alternative are in this repository. Without XGBoost the system still builds and runs; the model status then says plainly "XGBoost not built in".
Python 3 (standard library only) is used by the tests and the CLI/demo helpers, not by the system itself.

**Windows:** run everything inside **WSL 2 (Ubuntu)**. The Worker refuses to start on non-Linux systems (it needs `/proc`), and the
sources use POSIX sockets, so a native Windows build is not supported. Keep the project in the Linux file system (`~/NEXUS`), not under `/mnt/c`
(slow, and file permissions of the scripts are lost).

## 3. Quick start (10 minutes)

```bash
unzip NEXUS_FINAL.zip -d ~ && cd ~/NEXUS            # 1. extract
./scripts/check_prereqs.sh                          # 2. check tools
cp .env.example .env && nano .env                   # 3. set NEXUS_DB_PASSWORD (any password you choose)

# 4. database - choose ONE:
sudo service postgresql start && ./setup/setup.sh --test-db     #   a) system PostgreSQL (asks for your sudo password)
./setup/local_pg.sh start                                       #   b) no sudo: private cluster in ./.pgdata on port 54329
                                                                #      (then put NEXUS_DB_HOST=127.0.0.1 and NEXUS_DB_PORT=54329 in .env)

./scripts/install_xgboost.sh                         # 5a. one-time: build XGBoost (skip -> Random Forest only)
./scripts/build.sh                                  # 5. build (master, worker, nexus_ml, tests)
./scripts/run_tests.sh                              # 6. all automated tests
./scripts/run_demo.sh                               # 7. end-to-end demo (cluster stays up)
# 8. open http://127.0.0.1:8080/   (dashboard)       ./scripts/stop_all.sh to stop
```

## 4. Configuration (`.env`, no secrets in code)

Every script loads `.env`; for manual runs use `set -a; source .env; set +a`. All variables are documented in [.env.example](.env.example):

| Variable | Default | Meaning |
|---|---|---|
| `NEXUS_DB_HOST/PORT/NAME/USER/PASSWORD` | localhost / 5432 / nexus / nexus / *(required)* | PostgreSQL connection (Master and ML tool only) |
| `NEXUS_MASTER_HOST/PORT` | 127.0.0.1 / 5050 | where Workers connect (on another computer: the Master computer's LAN IP, never 127.0.0.1) |
| `NEXUS_MASTER_BIND` | 0.0.0.0 | interface of the Worker TCP port (127.0.0.1 = this computer only) |
| `NEXUS_HTTP_PORT`, `NEXUS_HTTP_BIND` | 8080, 127.0.0.1 | dashboard + API (no authentication → keep it on localhost) |
| `NEXUS_HEARTBEAT_SEC` | 3 | Worker heartbeat interval; a Worker is **dead after 3 missed beats** |
| `NEXUS_WORKER_CPU_AFFINITY` | none (`start_cluster.sh`: auto) | `none`, `auto`, or a CPU list (`2`, `0,2`, `1-3`): which logical CPUs the Worker process may use |
| `NEXUS_WORKER_THREADS` | = capacity | executor thread-pool size |
| `NEXUS_WORKER_CAPACITY` | pinned CPUs, else all CPUs | max tasks the Master may assign to one Worker at the same time |
| `NEXUS_WORKER_STORAGE` | `worker_data` | parent of `<WORKER_ID>/blocks/` |
| `NEXUS_SCHED_POLICY` | stored policy | forces a policy at Master start |
| `NEXUS_BLOCK_SIZE`, `NEXUS_REPLICATION`, `NEXUS_MAX_UPLOAD_MB` | 262144, 2, 64 | storage defaults / upload limit |
| `NEXUS_ML_MODEL_TYPE` | xgboost | `xgboost` or `random_forest` |
| `NEXUS_ML_MODEL`, `NEXUS_ML_MIN_SAMPLES` | models/duration_xgb.model (`_rf.model`), 30 | model file, minimum history |
| `NEXUS_ML_XGB_ROUNDS/_DEPTH/_ETA`, `NEXUS_ML_TREES/_MAX_DEPTH` | 150 / 4 / 0.1, 60 / 12 | XGBoost / Random Forest parameters |
| `NEXUS_ML_AUTO_RETRAIN_EVERY` | 100 | background retrain after this many new completed executions (0 = off) |

## 5. Database (PostgreSQL)

* **Create:** `./setup/setup.sh [--test-db]` (system PostgreSQL, uses `sudo -u postgres`; override with `PSQL_ADMIN="psql -h … -U postgres"`) or
  `./setup/local_pg.sh start` (rootless private cluster; `stop` / `status` also available).
* **Upgrade / re-apply:** `./build/master --init-db` – `database/schema.sql` + `seed.sql` are idempotent (`IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`), so a database from the older 40 % prototype is upgraded in place.
* The Master refuses to start with a clear message if PostgreSQL is unreachable or a table is missing.
* **Tables:** `workers`, `jobs`, `tasks`, `resource_metrics`, `execution_history`, `scheduler_config`, `scheduling_decisions`, `datasets`, `blocks`,
  `block_replicas`, `job_stages`, `mr_intermediate`, `mr_reduce_outputs`, `mr_results`, `ml_training_runs`, `ml_predictions`, `benchmark_runs`, `benchmark_results`, `task_types`.
  Primary/foreign keys with `ON DELETE CASCADE`, `CHECK` constraints (priority 0–9, valid status values, positive sizes), indexes on the hot paths.
* **Safety:** every statement is parameterised (`$1…`; an injection attempt is stored as data – tested). Multi-step changes are transactions (`Database::transact`)
  or single CTE statements; task transitions are guarded `UPDATE … WHERE status = <expected>`.

## 6. Build

```bash
./scripts/build.sh              # incremental
./scripts/build.sh --clean      # from scratch (what the final verification used: 0 warnings with -Wall -Wextra)
# equivalent: cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j
```
Products in `build/`: `master`, `worker`, `nexus_ml`, and the test programs.

## 7. Running

```bash
./scripts/start_cluster.sh -w 3 -c 4     # Master + 3 Workers in the background, 4 concurrent tasks each  (logs/, run/*.pid)
./scripts/stop_all.sh                    # clean shutdown (Workers first)

# or by hand, one terminal each:
./scripts/start_master.sh                # foreground; interactive console, dashboard on :8080
./scripts/start_worker.sh W1 4           # Worker id + capacity
./scripts/start_worker.sh W2 4
```
Master console commands (type `help`): `submit`, `burst`, `policy`, `workers`, `tasks`, `history`, `decisions`, `upload`, `datasets`, `dataset`, `verify`, `repair`,
`download`, `mapreduce`, `job`, `train`, `model`. CLI client for the API: `./scripts/nexus_cli.sh` (run without arguments for the list).

Worker ids must be unique among live Workers (1–32 chars of `[A-Za-z0-9_-]`); a second process with a live id is rejected.
If a Worker's id was used before, its block directory `worker_data/<ID>/` is reused, which is how stored data survives restarts.

## 8. Choosing the scheduling policy (no recompilation)

```bash
./scripts/nexus_cli.sh policy                 # show
./scripts/nexus_cli.sh policy ROUND_ROBIN     # set: FCFS | ROUND_ROBIN | PRIORITY | RESOURCE_AWARE | AI_ASSISTED
# Master console: policy PRIORITY      Dashboard: Scheduling tab      API: POST /api/policy  policy=PRIORITY
```
The choice is stored in PostgreSQL (`scheduler_config`) and survives restarts. All policies are **non-preemptive** (a running task is never interrupted).

| Policy | Order in which pending tasks are considered | Worker chosen | Tie-break |
|---|---|---|---|
| `FCFS` | submission order (task id) | first eligible Worker by id | lower task id / lower Worker id |
| `ROUND_ROBIN` | submission order | next eligible Worker after the last one used (cursor by id, wraps around); full / dead Workers are skipped | cursor |
| `PRIORITY` | **priority 9 → 0**, then submission order | least-occupied eligible Worker (running/capacity) | earlier submission; lower Worker id |
| `RESOURCE_AWARE` | submission order | lowest resource score (below) | lower Worker id |
| `AI_ASSISTED` | priority, then submission order | **shortest predicted duration** of the task on each Worker; **falls back to RESOURCE_AWARE** if no trained model / a prediction fails / metrics are stale (recorded as `fallback`) | resource score, then id |

A Worker is *eligible* only if it is **healthy** (connected **and** last heartbeat ≤ 3 × `NEXUS_HEARTBEAT_SEC` ago – the stored `ACTIVE` flag is never trusted on its own),
has a free slot (`running < capacity`), and – for tasks that read a block – holds a healthy replica of that block. A Worker that already failed a task is avoided for its retry if another Worker is available.
Priority scheduling has no aging, so a constant stream of high-priority work can starve low-priority tasks (documented limitation).

**Resource score** (lower is better, every term normalised to 0..1):

```
score = 0.35·cpu/100 + 0.20·ram/100 + 0.05·disk/100 + 0.30·(running/capacity) + 0.10·min(1, heartbeat_age/timeout) − 0.15·[Worker holds the task's data]
```
Metrics must be *usable*: finite, within 0–100 %, and reported within the last 3 heartbeat intervals. Otherwise the Worker is **rejected** for RESOURCE_AWARE / AI_ASSISTED.
Every assignment writes a `scheduling_decisions` row: active policy, effective policy, chosen Worker, score, predicted duration, fallback flag, the reason text and the
metrics of **every** Worker at that moment (JSON). View them in *Dashboard → Scheduling* (click a row) or `GET /api/decisions`.

## 9. Submitting tasks and priorities

| Type | What it really does |
|---|---|
| `CPU_BURN` (scale 1–100) | integer arithmetic on one core for scale × 100 ms – short, precisely tunable load for demos and for building model history |
| `WORD_COUNT` (1–20) | builds a 10M×scale-word corpus and counts word frequencies (hash map, memory heavy) |
| `PRIME_COUNT` (1–20) | sieve of Eratosthenes up to 150M×scale |
| `MATRIX_MULTIPLY` (1–20) | dense N×N multiplication, N = 1200·∛scale |
| `CHECKSUM` (1–20) | FNV-1a over 400M×scale generated bytes |
| `BLOCK_CHECKSUM`, `BLOCK_WORD_COUNT` | read a **stored block** from the Worker's own disk (SHA-256 / word count); only Workers holding a replica may run them |

```bash
./scripts/nexus_cli.sh submit CPU_BURN 5 9            # type, scale, priority (0-9, higher = more urgent)
./scripts/nexus_cli.sh submit BLOCK_CHECKSUM 1 5 ds3_b0
curl -X POST localhost:8080/api/tasks -d "type=WORD_COUNT&scale=2&priority=7"
```
Priority is stored per task and used by `PRIORITY` and `AI_ASSISTED`. **Failure handling:** a task that fails (or whose Worker dies / stops heartbeating) is retried on another Worker up to
3 attempts; each attempt is a row in `execution_history` (including the failed ones, with the reason). Deterministic errors (bad parameters) are not retried. A late result from an
old attempt is ignored. After a Master restart, interrupted tasks are re-queued (`recoverOnStart`).

## 10. Multi-core processing

Three different things control how much CPU a Worker uses – they are **separate settings**, shown separately on the dashboard:

| Setting | Meaning | Where it is enforced |
|---|---|---|
| `NEXUS_WORKER_CPU_AFFINITY` | *which* logical CPUs the Worker process may run on (`none`, `auto`, `2`, `0,2`, `1-3`) | Linux `sched_setaffinity` in `worker/cpu_affinity.cpp`, applied before any thread exists; the mask is **read back** with `sched_getaffinity` and only then reported |
| `NEXUS_WORKER_THREADS` | size of the executor thread pool (default = capacity) | `worker_client.cpp` (`ThreadPool`) |
| `NEXUS_WORKER_CAPACITY` | how many tasks the Master may assign at once (default = number of pinned CPUs, else all CPUs) | the Scheduler (never assigns beyond it) |

* `auto` gives Worker *k* its own dedicated CPU (the *k*-th CPU the process is allowed to use; the number at the end of the id, `W3` → index 2, or `NEXUS_WORKER_INDEX`).
  `./scripts/start_cluster.sh` uses `auto` (capacity 1 per Worker) when there are at least as many logical CPUs as Workers; `NEXUS_CPU_MAP=2,4,6` picks explicit CPUs.
* Invalid or unavailable CPU ids are **never silently accepted**: status `FAILED` (Worker stays unpinned, reason logged and shown) or `PARTIAL` (some ids ignored / a core shared). Capacity larger than the usable CPUs logs an oversubscription warning.
* Evidence: dashboard *Workers → CPU affinity*, `GET /api/workers`, `grep Cpus_allowed_list /proc/<worker pid>/status` (the integration test does exactly that for every thread of the process).
* Pinning does not reserve a CPU exclusively: the OS may still run other processes there. NEXUS only constrains the Worker.

Measured concurrency evidence: each execution row stores the Worker-side `started_at`/`finished_at` and `concurrent_at_start`; `GET /api/concurrency` and *Dashboard → Resources* compute the maximum overlap per Worker and draw the timeline.
Heartbeats and storage requests run on other threads, so they keep flowing while tasks run.

## 11. Distributed storage

```bash
./scripts/nexus_cli.sh upload data.txt 2 262144 1     # file, replication, block size, 1 = text-aligned (needed for MapReduce)
./scripts/nexus_cli.sh datasets ; ./scripts/nexus_cli.sh dataset 3     # list / inspect blocks + replicas + health
./scripts/nexus_cli.sh verify 3 ; ./scripts/nexus_cli.sh repair 3
./scripts/nexus_cli.sh download 3 out.txt             # reconstruct (every block checksum + whole-file SHA-256 checked)
./scripts/storage_demo.sh                              # upload → corrupt a replica on disk → detect → repair → download
```
* **Chunking:** configurable block size (16 B – 4 MiB). Text-aligned mode ends blocks at whitespace so no word is cut (required by MapReduce).
* **Placement:** each block goes to `replication` **distinct** Workers, emptiest first (ties by id). With fewer healthy Workers than the factor, the block is stored on as many as exist and
  reported as *under-replicated* (warning + `under_replicated_blocks`). If no Worker can take a block, the upload fails and is rolled back.
* **Where the bytes are:** `worker_data/<WORKER_ID>/blocks/<block_id>.blk` (id = `ds<dataset>_b<index>`, written atomically after the SHA-256 is checked). PostgreSQL stores only metadata
  (`datasets`, `blocks` with size/checksum, `block_replicas` with Worker + `HEALTHY/CORRUPT/MISSING`).
* **Retrieval:** the Master tries replicas one after another and validates each block's SHA-256; a corrupt copy is marked `CORRUPT` and skipped. If some block has no valid replica on a live Worker,
  the download fails with **HTTP 409** and the list of unavailable blocks – the file is never returned half-reconstructed.
* **Recovery:** `verify` makes every Worker re-hash its copy and updates replica health; `repair` copies a healthy replica to restore the replication factor (preferably in place on the damaged Worker).
  Repair is **on demand** (button / command) – there is no background re-replication daemon.
* **Persistence:** block files stay on the Workers' disks and metadata in PostgreSQL, so datasets survive restarts of the Master and of Workers (tested).
* **Security:** block ids and Worker ids are restricted to `[A-Za-z0-9_-]` before they touch the file system (no `/`, no `.`), uploads are stored under a sanitised base name, and the static file server
  serves only three fixed files (traversal attempts tested).
* **Limits:** the whole file is held in the Master's memory while it is split / reassembled (default upload limit 64 MB); there is no dataset deletion API; blocks are not encrypted; one Worker holds at most one copy of a block.

## 12. MapReduce word count

```bash
./scripts/nexus_cli.sh upload corpus.txt 2 100000 1        # text-aligned!
./scripts/nexus_cli.sh mapreduce 3 3 5                      # dataset id, reducers, priority   (or Dashboard → Storage → "Run word count")
./scripts/nexus_cli.sh job 12
```
1. **SPLIT** – the dataset's blocks are the input partitions. 2. **MAP** – one `BLOCK_WORD_COUNT` task per block, scheduled only on Workers holding a replica (data locality); each map task reads its block from
its own disk, counts words (lower-case `[a-z0-9]+`) and hash-partitions the result into R parts. 3. **SHUFFLE** – the Master gathers partition *i* of every map output (`mr_intermediate`).
4. **REDUCE** – R `REDUCE_WORD_COUNT` tasks (any Worker) merge their partition. 5. **FINALIZE** – counts go to `mr_results`, the job is `COMPLETED`.
Each stage has start/end times in `job_stages`. If a task fails permanently, the job is `FAILED` and its pending tasks are cancelled. The integration test compares the full result with Python's own count (exact match, also with one Worker killed).
*Limitation:* intermediate data travels through the Master and PostgreSQL (not Worker-to-Worker); only word count is implemented; no combiner.

## 13. AI-based task-time prediction (XGBoost)

* **Data:** only real completed rows of `execution_history`. Features (all known at scheduling time): task type (one-hot), log₂(scale), log₁₀(input bytes), Worker CPU %, RAM %, tasks already running, capacity, cores, occupancy.
  A per-type "historical mean duration" is deliberately **not** a feature – it would leak the training targets into the evaluation.
* **Model:** `reg:squarederror` gradient-boosted trees from the real **XGBoost 2.1.4** library (C API, `ml/xgb_model.cpp`; build it with `scripts/install_xgboost.sh`). A from-scratch Random Forest (`ml/random_forest.cpp`) remains available as a separate, honestly labelled model (`NEXUS_ML_MODEL_TYPE=random_forest`, or the *Train Random Forest* button). If XGBoost is not compiled in, training XGBoost **fails with a clear message** – it never falls back to a forest under the XGBoost name.
* **Evaluation:** seeded shuffle, 80 / 20 train / test split; fitted on the train rows only; **MAE and RMSE on the held-out rows** next to the "always predict the training mean" baseline **and** the other algorithm trained on the same split. Not better than the baseline → `NOT_ACTIVATED`, the scheduler keeps its fallback.
* **Honesty rules:** fewer than 30 completed executions (`NEXUS_ML_MIN_SAMPLES`) → `INSUFFICIENT_DATA`, **no model file is written, no number is invented**.
* **Train / retrain:** Dashboard → *AI model*, `./scripts/train_model.sh [xgboost|random_forest]`, `POST /api/ml/train` (`model_type=`), console `train`; plus a **background auto-retrain** every `NEXUS_ML_AUTO_RETRAIN_EVERY` (100) new completed executions that never blocks task dispatch.
  Model files: `models/duration_xgb.model` / `duration_rf.model`; runs in `ml_training_runs` (model type, hyper-parameters, comparison). The Master reloads the model at start.
* **How it changes scheduling:** with `AI_ASSISTED`, the model predicts, for each candidate Worker, how long *this* task would take given that Worker's current CPU/RAM/queue; the shortest predicted duration wins.
  Prediction and (later) actual duration are stored in `ml_predictions`. No model / failed prediction / stale metrics → `RESOURCE_AWARE` fallback, marked `fallback=true`.
* Details and measured numbers: [docs/MACHINE_LEARNING.md](docs/MACHINE_LEARNING.md). *The model is exactly as good as the history you give it.*

## 14. Operating-system resource monitoring

Each Worker samples the **real OS** every heartbeat: CPU % from the delta of `/proc/stat`, RAM from `/proc/meminfo` (`MemTotal − MemAvailable`), disk % from `statvfs()` of the storage directory, load average from `/proc/loadavg`,
its own resident memory from `/proc/self/statm`, plus running/queued tasks and bytes stored. Values are persisted in `resource_metrics` and the live row of `workers`, drawn in *Dashboard → Resources*.
CPU and RAM are **host-wide** (so Workers on one host report the same numbers). *Monitoring ≠ managing:* NEXUS observes these figures and uses them for decisions; it does not reserve, limit or allocate OS resources (no cgroups, no `nice`, no memory quotas) –
its "allocation" is choosing which Worker gets which task. On non-Linux platforms the Worker exits with an explicit message.

## 15. HTTP API (JSON; everything the dashboard shows comes from here)

| Method & path | Purpose |
|---|---|
| `GET /api/overview`, `/api/workers`, `/api/health` | cluster summary, Worker table (health from heartbeat age), liveness |
| `GET/POST /api/policy` | read / set the scheduling policy (`policy=NAME`) |
| `POST /api/tasks` (`type, scale, priority, block_id`), `GET /api/tasks?status=&limit=`, `GET /api/tasks/:id` | submit, list, inspect (decisions + attempts) |
| `GET /api/history`, `/api/decisions`, `/api/metrics?minutes=`, `/api/concurrency` | execution history, scheduling decisions, resource time series, measured concurrency |
| `GET /api/jobs?type=`, `/api/jobs/:id`, `/api/jobs/:id/results?limit=`, `POST /api/mapreduce` (`dataset_id, reducers, priority`) | jobs, stages, MapReduce results |
| `GET/POST /api/datasets` (raw body, `name, block_size, replication, text`), `GET /api/datasets/:id`, `POST …/verify`, `POST …/repair`, `GET …/download` | distributed storage |
| `GET /api/ml/status`, `POST /api/ml/train` (`model_type=xgboost` or `random_forest`) | model kind, XGBoost version, metrics, runs, predictions; (re)train |
| `POST /api/benchmarks` (`policies, type, count, scale_min, scale_max, priorities, seed, repeats, dataset_id, retrain_before`), `GET /api/benchmarks`, `GET /api/benchmarks/:id`, `POST /api/benchmarks/cancel` | start / list / read / cancel scheduling benchmarks (results are stored in PostgreSQL) |
| `GET /api/logs?after=` | recent Master log lines |

Errors are JSON `{"error": "..."}` with 400 / 404 / 405 / 409 / 413 / 500 status codes. **There is no authentication or TLS** – the API binds to 127.0.0.1 by default.

## 16. Dashboard

`http://127.0.0.1:8080/` – plain HTML/JS/CSS served by the Master (`dashboard/`), polling the API every 2 s. Tabs: *Overview* (KPIs, hosts, shared-CPU warning), *Workers* (status, CPU/RAM/disk bars, slots, heartbeat age, blocks),
*Tasks* (submit with priority, live queue, history), *Scheduling* (policy selector + decision log with per-Worker metrics), *Storage* (upload, block/replica map, verify/repair/download), *MapReduce* (jobs, stage timeline, top words),
*Benchmarks* (run all five policies on the same seeded workload, tables + bar charts, historical runs), *Resources* (CPU/RAM/tasks charts, concurrency table and Gantt timeline), *AI model* (status, MAE/RMSE vs baseline, runs, predicted vs actual), *Logs*. Loading, empty and "Master unreachable" states are handled.

## 17. Tests

```bash
./scripts/run_tests.sh          # unit tests + database tests + end-to-end integration test (needs the *_test database: ./setup/setup.sh --test-db)
./scripts/run_tests.sh --unit   # only the tests that need no PostgreSQL
cd build && ctest --output-on-failure
```
| Test program | Covers |
|---|---|
| `test_message` | message serialisation/parsing, malformed input, framing over a real socket (split writes, 5 MB payloads), SHA-256 known answers, base64, id/path helpers |
| `test_scheduler` | FCFS order, Round Robin rotation/skipping, Priority order + tie-break, resource-aware selection, stale/invalid metrics, capacity, locality, retry exclusion, AI selection and fallback |
| `test_worker_exec` | thread-pool limit and real parallel speed-up, task executor, block store (checksum, atomicity, **path traversal**), MapReduce map/partition/reduce = direct count |
| `test_storage` | chunking (binary + text-aligned), reconstruction, per-block checksums, replica placement on distinct Workers |
| `test_ml` | XGBoost + Random Forest accuracy, determinism, persistence, `INSUFFICIENT_DATA`, **no train/test leakage**, baseline comparison, model file handling |
| `test_affinity` | CPU-list parsing, `sched_setaffinity` applied **and read back**, invalid/partial/auto requests, deterministic benchmark workload, request validation |
| `test_http` | request parsing, routing, size limits, error codes against a live server |
| `test_db` *(needs `nexus_test` DB)* | idempotent schema, constraints, SQL-injection-as-data, transactions/rollback, persistence across connections, Worker registry (registration, **heartbeat expiry**, duplicate ids), full task life cycle with socket-connected fake Workers (FCFS / Priority / RR order, capacity, retry, permanent failure, stale/duplicate results, Worker loss, restart recovery) |
| `integration_test.py` *(needs `nexus_test` DB)* | real Master + 3 real Workers: registration, protocol abuse, all original workloads, concurrency, all policies, frozen/killed Workers, storage (placement, corruption, repair, unavailable replicas, restart), MapReduce vs Python truth, ML (insufficient data → train → AI scheduling), Master crash recovery, dashboard serving |

Tests that cannot run print **SKIPPED** with the reason (exit code 77) and are never counted as passed.

## 18. Troubleshooting

| Symptom | Fix |
|---|---|
| `Cannot connect to PostgreSQL: … connection refused` | `sudo service postgresql start` (WSL has no systemd by default) or `./setup/local_pg.sh start`; check `NEXUS_DB_HOST/PORT` in `.env` (54329 for the private cluster) |
| `password authentication failed` | `NEXUS_DB_PASSWORD` differs from the one used in `setup.sh`; rerun `./setup/setup.sh` (it resets the role's password) |
| `sudo: a password is required` in setup | use `./setup/local_pg.sh start` or `PSQL_ADMIN="psql -h localhost -U postgres" ./setup/setup.sh` |
| `Database schema is missing or outdated` | `./build/master --init-db` |
| CMake: `Could NOT find PostgreSQL` | `sudo apt install libpq-dev`; if headers are in a custom path: `./scripts/build.sh -DPostgreSQL_INCLUDE_DIR=/usr/include/postgresql` |
| `Cannot listen on port 5050` / HTTP port busy | another Master is running (`./scripts/stop_all.sh`, `ss -ltnp \| grep 5050`) or change `NEXUS_MASTER_PORT` / `NEXUS_HTTP_PORT` |
| Worker prints `Registration REJECTED … already connected` | that id is alive elsewhere; pick another id, or wait ~10 s after killing the old process |
| Worker loops on `Connect failed` | Master not running or wrong `NEXUS_MASTER_HOST/PORT`; across machines open the port in the firewall |
| Dashboard not reachable from Windows | WSL 2 forwards `localhost:8080` automatically; otherwise use the WSL IP and `NEXUS_HTTP_BIND=0.0.0.0` (the API has no authentication!) |
| Dashboard shows "Master unreachable" | the Master stopped; it retries every 2 s by itself |
| `scripts/*.sh: Permission denied` / `\r: command not found` | the project was unpacked on `/mnt/c` or edited on Windows: `chmod +x scripts/*.sh setup/*.sh` and `sed -i 's/\r$//' scripts/*.sh setup/*.sh`; best: keep it in `~` |
| MapReduce says "not text-aligned" | upload with `text=1` / tick *text-aligned* |
| AI policy shows `fallback` | no trained model yet – run ≥ 30 tasks, then `./scripts/train_model.sh` |
| Tests say SKIPPED | database `nexus_test` missing: `./setup/setup.sh --test-db` |

## 19. Known limitations (honest list)

* Everything was verified on **one machine** (WSL 2): Master, Workers and PostgreSQL on localhost, Workers pinned to distinct CPUs. Workers on **separate physical computers**, network partitions and clock skew between hosts (execution timestamps are taken on the Worker) were **not** tested.
* The Master is a single point of failure (no leader election). Its state is in PostgreSQL, so a restart recovers queues, policy, datasets and the model, and Workers reconnect by themselves – but nothing runs while it is down.
* The TCP protocol and the HTTP API have **no authentication or encryption**; use only on trusted networks / localhost.
* Scheduling is non-preemptive; Priority has no aging (starvation possible); a task that hangs on a *live* Worker has no timeout.
* Storage: whole-file in memory (≤ 64 MB by default), no dataset deletion, manual (not automatic) re-replication, no encryption, replicas of a Worker that is offline are simply unavailable until it returns.
* MapReduce: word count only; intermediate data goes through the Master/PostgreSQL; a Worker failing *during* a reduce task is handled by the normal retry, but no speculative execution.
* The built-in workloads are synthetic CPU/memory benchmarks (plus block reading); prediction quality depends entirely on the variety of your own execution history.
* CPU/RAM figures are host-wide, not per-process; disk % is that of the storage file system.
* Native Windows (outside WSL) is unsupported.

## 20. Project layout

```
common/    protocol.h message.{h,cpp} sha256.{h,cpp} chunker.h wordcount.h thread_pool.h util.h json.h log.h config.h
database/  schema.sql seed.sql database.{h,cpp}            (libpq layer, transactions, JSON rows)
master/    main.cpp master_server worker_manager scheduler task_manager storage_manager mapreduce ml_manager benchmark http_server api
worker/    main.cpp worker_client cpu_affinity resource_monitor task_executor block_store
ml/        xgb_model duration_model random_forest features.h trainer
tools/     nexus_ml.cpp                                   (train | status | predict)
dashboard/ index.html app.js style.css
tests/     test_*.cpp integration_test.py CMakeLists.txt
scripts/   install_xgboost start_remote_worker build start_master start_worker start_cluster stop_all run_tests run_demo train_model storage_demo nexus_cli check_prereqs common
setup/     setup.sh local_pg.sh
docs/      see the Documentation table at the top
```
