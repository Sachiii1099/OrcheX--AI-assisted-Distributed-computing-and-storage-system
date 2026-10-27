# Testing and validation

Results of the actual runs are in [FINAL_TEST_REPORT.md](FINAL_TEST_REPORT.md). This page explains the test levels and how to reproduce them.
Rules followed: no test was disabled or weakened to make the suite pass; a test that cannot run reports **SKIPPED (exit 77) with the reason** and is never counted as passed; no output in these documents is invented.

## Test levels

| Level | Where | Needs | What it proves |
|---|---|---|---|
| A. Build | `./scripts/build.sh --clean` | g++, CMake, libpq (+ XGBoost) | compiles with `-Wall -Wextra`, **0 warnings** |
| B. Unit | `build/test_message`, `_scheduler`, `_worker_exec`, `_storage`, `_ml`, `_http`, `_affinity` | nothing | protocol framing, every scheduling policy, executors, block store, chunking/checksums/placement, ML features/training/persistence/fallback (XGBoost + Random Forest), CPU affinity, benchmark plan |
| C. Database | `build/test_db` | PostgreSQL `nexus_test` (wiped) | schema idempotency, constraints, parameterisation, transactions, Worker registry + heartbeat expiry, task life cycle with socket-connected fake Workers |
| D. Integration | `tests/integration_test.py` sections `startup api protocol execution concurrency policies recovery storage mapreduce ml affinity benchmark` | PostgreSQL, built binaries | real Master + real Worker processes + real PostgreSQL |
| E. End to end | the same file, sections `persistence dashboard` plus the whole flow | same | register 3 Workers → upload → block placement → MapReduce vs independent count → every policy → train XGBoost → AI scheduling → kill Worker → retry → Master restart → persistent data → dashboard serves real numbers |
| F. Clean install | documented procedure in a fresh directory | internet | the ZIP + the documented steps work from scratch (result in the report) |
| G. Configuration / error paths | invalid affinity, bad benchmark parameters, unknown model type, empty/invalid uploads, missing database password, duplicate Worker id, malformed protocol messages | | readable errors, nothing silently accepted |

## What the end-to-end test checks (by feature)

* **Worker registration & heartbeats** – three Workers register, appear healthy, a duplicate id is rejected, a killed Worker becomes INACTIVE.
* **Storage** – upload; block sizes add up; every block has 2 replicas on 2 distinct Workers; the **`.blk` files really exist** in the Workers' directories; byte-exact download; corrupted replica detected by `verify`, repaired; Worker down ⇒ still readable (2 replicas) or *not reconstructable* (1 replica); replication 5 on 3 Workers ⇒ under-replicated is reported.
* **MapReduce** – results equal a word count computed independently in Python.
* **Scheduling** – every policy executes; decisions are stored with the policy that was active; FCFS order, Priority order, Round Robin rotation, capacity limits, retry exclusion; benchmark runs verify identical workload per policy, dispatch order evidence (FCFS by id, PRIORITY by priority), makespan equals an independent SQL calculation, original policy restored.
* **ML** – empty history ⇒ `INSUFFICIENT_DATA`, no file; 70 real executions ⇒ XGBoost `TRAINED` with held-out MAE/RMSE better than the baseline; model file starts with the XGBoost header; Random Forest trainable separately and labelled as such; AI decisions carry predictions and are not fallbacks; predictions grow with task scale; predicted-vs-actual stored.
* **CPU affinity** – Worker A1 pinned to one CPU: the API reports `APPLIED`, **`/proc/<pid>/status` and every thread's `Cpus_allowed_list` agree**; A2 with an impossible CPU is reported `FAILED` and really left unpinned; `auto` picks the first allowed CPU for index 0; capacity, threads and affinity are stored as three separate values.
* **Failure & recovery** – killing a Worker mid-task ⇒ the task is retried elsewhere and completes, history shows both attempts; a task whose block is gone fails after 3 attempts with a clear error; Master killed with SIGKILL while a task runs ⇒ after restart the task is recovered (attempt 2), policy, model, datasets and benchmark results persist.
* **Dashboard** – `/` and static files are served, the script uses the real API endpoints (incl. `/api/benchmarks`), contains no `Math.random`, is not path-traversable, and its numbers equal the PostgreSQL counts.

## Reproduce

```bash
# Ubuntu terminal, project directory, database set up (docs/RUN_GUIDE_WINDOWS_WSL.md §3-6)
./scripts/build.sh --clean                  # A
./scripts/run_tests.sh --unit               # B
./scripts/run_tests.sh                      # B + C + D + E (uses database nexus_test)
python3 tests/integration_test.py --only affinity,benchmark   # a subset (after sourcing .env with NEXUS_DB_NAME=nexus_test)
cd build && ctest --output-on-failure       # same unit/DB tests through ctest
```

## Known gaps (not tested)

* Workers on **separate physical computers**, real network latency/partitions, clock skew.
* Native Windows (unsupported by design), non-Ubuntu distributions.
* Long-running soak tests and large (> 64 MB) datasets.
* Statistical significance of scheduler differences – benchmarks are measurements on one small cluster, not proofs of superiority.
