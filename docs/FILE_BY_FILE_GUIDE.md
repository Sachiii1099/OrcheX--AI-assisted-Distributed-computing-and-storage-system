# File-by-file guide

How to read this: each entry gives **purpose · inputs/outputs · key functions · interactions · how to test/debug**. Requirement tags in brackets refer to the project goals
(**[MW]** Master–Worker TCP, **[HB]** heartbeat/failure, **[MC]** multicore/affinity, **[ST]** storage, **[SCH]** scheduling, **[ML]** XGBoost, **[DB]** PostgreSQL, **[GUI]** dashboard, **[TST]** tests).

```
NEXUS/
├── CMakeLists.txt  .env.example  .gitignore  README.md  DEMO_CHECKLIST.md
├── common/    shared by Master and Worker (protocol, framing, helpers)
├── database/  schema.sql seed.sql database.{h,cpp}
├── master/    the Master process
├── worker/    the Worker process
├── ml/        features, XGBoost, Random Forest, training pipeline
├── tools/     nexus_ml.cpp (stand-alone train/status/predict)
├── dashboard/ index.html app.js style.css
├── tests/     unit tests + integration_test.py
├── scripts/   build / run / test / demo helpers (bash)
├── setup/     PostgreSQL setup scripts
└── docs/      this documentation
```

---------------------------------------------------------------------------------------------------

## 1. Build and configuration files

### `CMakeLists.txt`
Defines the static libraries `nexus_common`, `nexus_db`, `nexus_ml_lib`, `nexus_master_lib`, `nexus_worker_lib` and the executables `master`, `worker`, `nexus_ml`; adds `tests/`.
Finds libpq (required) and **XGBoost** (`NEXUS_XGBOOST_ROOT`, `third_party/xgboost-install`, `/usr/local`, `/usr`); on success defines `NEXUS_HAVE_XGBOOST` and sets the build RPATH so the binaries find `libxgboost.so`; otherwise prints a warning and builds Random-Forest-only. Debug: `cmake -S . -B build` and read the `XGBoost found:` line. [ML]

### `.env.example`
Template of every setting (database, ports, bind address, heartbeat, CPU affinity / threads / capacity, storage, ML). Copy to `.env` (git-ignored; never commit it). Scripts source it via `scripts/common.sh`. Contains **no real secret** – `NEXUS_DB_PASSWORD=change_me`. [DB]

### `.gitignore` – excludes `build/`, `.env`, `.pgdata/`, `worker_data/`, `logs/`, `run/`, `models/`, `exports/`, `third_party/`.

---------------------------------------------------------------------------------------------------

## 2. `common/` – shared code

| File | Purpose and key contents |
|---|---|
| `protocol.h` | Message-type constants (`REGISTER_WORKER`, `HEARTBEAT`, `TASK_ASSIGN`, `BLOCK_STORE`, …), defaults (ports, heartbeat, `HEARTBEAT_MISS_FACTOR = 3`), the list of task types and helpers `isKnownTaskType`, `isBlockTaskType`, `isUserSubmittable`, priority range 0–9. The single source of truth both processes include. [MW] |
| `message.h/.cpp` | `Message` (type + key/value fields; `set/get/getInt/getDouble`, `serialize()`, `parse()`; values percent-escaped so `\| = % \n` are safe) and `Connection` (RAII TCP socket; thread-safe `send()`, single-reader `receive(timeout)` returning 1/0/-1, `shutdown()`, `connectTo(host, port)`); `createListener(port, err, bindAddr)` = socket/bind/listen with the configurable bind address (`NEXUS_MASTER_BIND`). Lines are capped at 24 MB; garbage lines are skipped. Tested by `test_message`. [MW] |
| `sha256.h/.cpp` | Own SHA-256 (`sha256Hex`) and base64 (`base64Encode/Decode`) – used for block checksums and to ship block bytes in one text line. Known-answer tests. [ST] |
| `chunker.h` | `planChunks(data, blockSize, textAligned)` → `Chunk{index, offset, size, checksum}`; `chooseReplicaNodes(nodes, replication)` → distinct Workers, emptiest first. Pure functions, unit-tested by `test_storage`. [ST] |
| `wordcount.h` | MapReduce helpers: `countWords`, `fnv1a`, `partitionOf(word, reducers)`, `encodeMapOutput/splitMapOutput`, `mergeLines`, `formatLines`. Used by the Worker (map/reduce) and by tests as the independent truth. [ST] |
| `thread_pool.h` | Bounded `ThreadPool(threads, maxQueue)` with `submit()` returning false when full (the Worker rejects such a task as retryable). [MC] |
| `util.h` | `nowMs`, `trim`, `splitOn`, `upper`, `urlEncode/Decode`, `encodeParams/decodeParams` (task parameters), `isSafeId` (path-traversal guard), `sanitizeFileName`, `readFile/writeFile`, `fmtDouble`. |
| `json.h` | Minimal JSON writer (`json::Obj().add(...).raw(...).build()`); the API only emits JSON. |
| `log.h` | Thread-safe `log(tag, msg)`; keeps the last lines in memory for `/api/logs`. |
| `config.h` | `envOr`, `envIntOr` – environment-variable reading. |

---------------------------------------------------------------------------------------------------

## 3. `database/`

### `schema.sql`
Idempotent DDL for every table (see [DATABASE_SCHEMA.md](DATABASE_SCHEMA.md)). Section order: cluster → jobs/tasks → metrics/history → scheduling → storage → MapReduce → ML → indexes → **v2 additions** (worker CPU columns, ML model columns, benchmark tables). Apply: `setup/setup.sh`, `build/master --init-db`. Test: `test_db` ("schema is idempotent"). [DB]

### `seed.sql` – static `task_types` reference rows only. No fake workers or tasks.

### `database.h/.cpp`
`Database` = one libpq connection behind a mutex. Key functions: `connect`, `exec(sql, params)` (**always parameterised**; `kSqlNull` binds NULL), `transact(stmts)` (BEGIN/COMMIT, rollback on any failure), `runScript`, `schemaPresent` (checks every required table, incl. `benchmark_*`), `upsertWorker`, `recordHeartbeat`, `recordResources`, `setWorkerStatus`, `Config::fromEnv()` (reads `NEXUS_DB_*`; errors name the setting that is missing). `Result` offers typed accessors and `toJson()/rowJson()` used by the API. Used by every Master module. Test: `test_db`. [DB]

---------------------------------------------------------------------------------------------------

## 4. `master/` – the Master process

| File | Purpose, key functions, interactions |
|---|---|
| `main.cpp` | Start-up order: connect DB → check schema (`--init-db` applies it) → build `Scheduler`, `WorkerManager`, `StorageManager`, `MlManager`, `TaskManager`, `MapReduce`, `BenchmarkRunner` → load model/policy → recover interrupted tasks → start `MasterServer` (TCP), `TaskManager` dispatch thread, auto-retrain thread, `HttpServer`; then the interactive console (`help`, `workers`, `tasks`, `train`, `policy`, `quit` …). Prints clear errors for a missing database/password. |
| `master_server.h/.cpp` | TCP front door. `start()` (listener on `NEXUS_MASTER_BIND:PORT`, accept thread, monitor thread), `acceptLoop`, `handleConnection` (first message must be a valid `REGISTER_WORKER`; then dispatches `HEARTBEAT`, `RESOURCE_UPDATE` (range-validated), `TASK_*`, `BLOCK_REPLY`), `monitorLoop` (every 0.5 s: `expireStale()` → `TaskManager::onWorkerLost`). [MW][HB] |
| `worker_manager.h/.cpp` | In-memory registry. `registerWorker` (unique ids; stores reported capacity, threads, affinity status → `workers` table), `onHeartbeat`, `onResourceUpdate`, `onDisconnect`, `expireStale`, `snapshot`, `isHealthy` (age of last heartbeat), `call()/onReply()` = request/response channel used by storage (`BLOCK_*` RPC with timeout). [HB][MC] |
| `scheduler.h/.cpp` | **Pure decision logic** for the five policies: `order()`, `pick()` (eligibility → policy rule → `Decision` incl. candidate JSON), `isHealthy`, `metricsUsable`, `resourceScore`, `parsePolicy/policyName`. No sockets, no DB. See [SCHEDULING_ALGORITHMS.md](SCHEDULING_ALGORITHMS.md). Test: `test_scheduler` (67 checks). [SCH] |
| `task_manager.h/.cpp` | Task life cycle. `submit`, `createJob/addTask`, `dispatchLoop/dispatchOnce` (snapshot workers → `Scheduler::order/pick` → `assign()` = one CTE statement that claims the task, writes `scheduling_decisions` and `ml_predictions` → `TASK_ASSIGN`), `onTaskStarted/onTaskResult/onTaskFailure` (guarded by task id + attempt + worker), `handleFailure` (retry on another Worker with the failed one excluded, or FAIL after `max_attempts`), `onWorkerLost`, `recoverOnStart`, `pauseDispatch` (benchmark batching), `cancelAll`. Test: `test_db`, `integration_test.py` (recovery). [SCH][HB] |
| `storage_manager.h/.cpp` | Master side of the block store: `upload` (plan chunks → place on distinct Workers → **physical write acknowledged before metadata** → one transaction per block), `fetchBlock` (checksum-validated), `reconstruct`, `verify`, `repair`, `inspect`, `listJson`, `replicaHolders` (locality). [ST] |
| `mapreduce.h/.cpp` | Word-count coordinator: `start`, `onTaskEvent`, `advance` (stateless, idempotent; derives the next stage from PostgreSQL), `resume` after Master restart, `jobJson`. Stages recorded in `job_stages`. [ST] |
| `ml_manager.h/.cpp` | Owns the active `DurationModel`: `loadFromDisk`, `train(modelType)` (hot-swaps only if it beat the baseline), `predictor()` for the scheduler, `startAutoRetrain` (background thread), `statusJson` (model kind, XGBoost version, runs, live MAE/RMSE). [ML] |
| `benchmark.h/.cpp` | `BenchmarkRunner`: `parseSpec` (validation), `plan` (seeded deterministic workload), `start` (background thread), `runAll/runOne` (idle wait → policy → paused batch submit → wait → **measure from the DB** → store), `listJson/getJson`, `recoverOnStart`. Test: `test_affinity` (plan/validation) + integration `benchmark` section. [SCH][GUI] |
| `http_server.h/.cpp` | Small HTTP/1.1 server: `route(method, pattern, handler)` with `:param` captures, request size limits, error codes, static-file traversal protection. Test: `test_http`. |
| `api.h/.cpp` | All REST routes (overview, workers, policy, tasks, jobs, history, decisions, metrics, concurrency, datasets, ml, **benchmarks**, logs) and `applyPolicy/loadPolicy`. Every handler reads PostgreSQL or calls a Master component – no hard-coded data. [GUI] |
| `sql_util.h` | `NEXUS_TS(col)` macro: ISO-8601 UTC timestamp text for JSON. |

---------------------------------------------------------------------------------------------------

## 5. `worker/` – the Worker process

| File | Purpose, key functions |
|---|---|
| `main.cpp` | Parses id/host/port; **applies CPU affinity first** (`applyCpuAffinity`), then derives `capacity` (default = pinned CPUs) and `threads`; warns on oversubscription; reconnect loop. [MC] |
| `cpu_affinity.h/.cpp` | `parseCpuList`, `formatCpuList`, `currentAffinity` (`sched_getaffinity`), `workerIndexFromId`, `applyCpuAffinity(spec, index)` → `AffinityResult{status, cpus, allowed, detail}`: validates ids against the allowed set, calls `sched_setaffinity`, **reads the mask back**, returns `APPLIED / PARTIAL / FAILED / NOT_REQUESTED`. Test: `test_affinity` (134 checks) and `/proc/<pid>/status` in the integration test. [MC] |
| `worker_client.h/.cpp` | `connectAndRegister` (sends capacity, threads, affinity), `heartbeatLoop` (`HEARTBEAT` + `RESOURCE_UPDATE` each interval), `executeTask` (runs on the pool: `TASK_STARTED` → `TaskExecutor::run` → `TASK_RESULT/FAILURE` with measured times), `handleBlockRequest` (`BLOCK_STORE/FETCH/VERIFY/DELETE`), `run` (receive loop; rejects a task as retryable when the pool is full). |
| `task_executor.h/.cpp` | Real workloads: `wordCount`, `primeCount`, `matrixMultiply`, `checksum`, `cpuBurn`, block tasks `blockWordCount`, `blockChecksum`, `reduceWordCount`. Returns `TaskOutcome{ok, result, data, error, retryable, seconds}`. Test: `test_worker_exec`. |
| `block_store.h/.cpp` | Worker-side storage: `put` (checksum-verified, temp file + rename), `get`, `verify`, `remove`, `usedBytes`, `pathFor` (id validation). Files: `<storage>/<WORKER_ID>/blocks/<id>.blk`. [ST] |
| `resource_monitor.h/.cpp` | Real OS readings: CPU from `/proc/stat` deltas, RAM from `/proc/meminfo`, disk from `statvfs`, load and process RSS. It **monitors**, it does not limit. |

---------------------------------------------------------------------------------------------------

## 6. `ml/` and `tools/`

| File | Purpose, key contents |
|---|---|
| `features.h` | `TaskFeatures`, `featureNames()`, `featureVector()` – the 16 scheduling-time features. No post-execution information. |
| `xgb_model.h/.cpp` | `XgbRegressor`: `fit` (DMatrix → `XGBoosterUpdateOneIter` × rounds), `predict`, `serialize/deserialize` (XGBoost JSON model), `available()`, `libraryVersion()`; an honest stub when libxgboost is absent. [ML] |
| `random_forest.h/.cpp` | From-scratch Random Forest (`fit`, `predict`, text serialisation) + `meanAbsError`, `rootMeanSquaredError`. Kept as a separately labelled alternative. |
| `duration_model.h` | `DurationModel` – holds either an XGBoost or a Random Forest model; `kindName()` is always truthful. |
| `trainer.h/.cpp` | `TrainConfig::fromEnv`, `trainAndEvaluate` (split, fit, baseline, comparison, status), `trainFromDatabase` (SQL → rows → run record), `saveModelFile/loadModelFile`. Test: `test_ml` (126 checks incl. 3 XGBoost cases). |
| `tools/nexus_ml.cpp` | CLI: `train [xgboost\|random_forest]`, `status`, `predict …` (exit 3 = insufficient data). |

---------------------------------------------------------------------------------------------------

## 7. `dashboard/`

| File | Purpose |
|---|---|
| `index.html` | Tabs: Overview, Workers, Tasks, Scheduling, **Benchmarks**, Storage, MapReduce, Resources, AI model, Logs. |
| `app.js` | One loader per tab (`loadOverview`, `workersTable` + `affinityBadge`, `loadScheduling`, `loadBenchmarks` + `aggregate` + `barChart`, `loadStorage`, `loadMapReduce`, `loadResources` + `lineChart`/`gantt`, `loadMl`, `loadLogs`), `api()` fetch wrapper with "Master unreachable" handling, 2-second refresh. All numbers come from `/api/*`; the integration test asserts there is no `Math.random` and that the numbers equal PostgreSQL counts. |
| `style.css` | Light/dark theme, responsive layout. |

---------------------------------------------------------------------------------------------------

## 8. `tests/`

| File | What it checks | Run |
|---|---|---|
| `test_util.h` | Tiny `CHECK` / `CHECK_EQ` harness, `testResult()`. | – |
| `test_message.cpp` | framing, escaping, malformed input, sockets, SHA-256, base64. | `build/test_message` |
| `test_scheduler.cpp` | every policy, eligibility, fallback, retry exclusion. | `build/test_scheduler` |
| `test_worker_exec.cpp` | thread pool, executors, block store, map/reduce vs direct count. | `build/test_worker_exec` |
| `test_storage.cpp` | chunking, reconstruction, checksums, placement. | `build/test_storage` |
| `test_ml.cpp` | Random Forest and **XGBoost**: accuracy, determinism, persistence, leakage, insufficient data, baseline. Exit 77 (SKIPPED) if XGBoost is not compiled in. | `build/test_ml` |
| `test_affinity.cpp` | CPU-list parsing, **real `sched_setaffinity` + read-back**, invalid/partial/auto, benchmark plan determinism and validation. | `build/test_affinity` |
| `test_http.cpp` | HTTP parsing/routing/limits against a live server. | `build/test_http` |
| `test_db.cpp` | schema, constraints, injection-as-data, transactions, Worker registry, task life cycle with fake socket Workers. Wipes `*_test` DB. | `build/test_db` |
| `integration_test.py` | real Master + Workers + PostgreSQL, 14 sections (startup … ml, affinity, benchmark, persistence, dashboard). | `python3 tests/integration_test.py [--only a,b]` |
| `CMakeLists.txt` | registers the tests with ctest (exit 77 = skipped). | `ctest` |

---------------------------------------------------------------------------------------------------

## 9. `scripts/` and `setup/`

| Script | Purpose |
|---|---|
| `common.sh` | paths, `.env` loading, `wait_for`, `master_up`, helpers (sourced by the others). |
| `check_prereqs.sh` | read-only check of g++, CMake, libpq, psql, PostgreSQL server, python3, curl, git, XGBoost, `.env`. |
| `install_xgboost.sh` | clones `dmlc/xgboost` tag v2.1.4 and builds it into `third_party/xgboost-install` (no sudo/pip). |
| `build.sh` | CMake configure + build (`--clean`). |
| `start_master.sh` / `start_worker.sh` | foreground Master / one Worker. |
| `start_cluster.sh` | background Master + N Workers; `-a auto|none`, `NEXUS_CPU_MAP`; prints what affinity each Worker really got. |
| `start_remote_worker.sh` | Worker on **another computer**: refuses `127.0.0.1`/`localhost`, checks TCP reachability first. |
| `stop_all.sh` | clean shutdown (Workers first). |
| `run_tests.sh` | all unit + DB + integration tests with an honest PASSED / FAILED / SKIPPED summary. |
| `run_demo.sh`, `storage_demo.sh` | scripted end-to-end demos of the original features. |
| `train_model.sh`, `nexus_cli.sh` | train/inspect the model; command-line client of the REST API (incl. `bench`, `bench-show`). |
| `setup/setup.sh` | creates role + databases (system PostgreSQL via `sudo -u postgres`) and applies the schema. |
| `setup/local_pg.sh` | **no-sudo** private PostgreSQL cluster in `./.pgdata` on port 54329 (`start|stop|status`). |
