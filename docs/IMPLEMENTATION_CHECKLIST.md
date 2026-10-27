# Implementation checklist and honest status

Environment used for every verification below: Ubuntu 26.04 under WSL 2, g++ 15.2 (`-std=c++17 -Wall -Wextra`, **0 warnings**), CMake 4.2, PostgreSQL 18, Python 3.14.
Master, Workers and PostgreSQL all ran on **one** machine.

Legend: **T** = implemented and covered by an automated test that passed · **M** = implemented, checked manually only · **L** = limitation / not done (reason given).

Last full run of `./scripts/run_tests.sh`: all 8 test programs PASSED (unit: 57 + 67 + 86 + 19 876 + 49 + 24 checks; database tests 137 checks; integration test 135 checks in 12 sections, 0 failures).
The integration test and the database tests were additionally run against a **ThreadSanitizer** build: no data-race reports.

## A. Master–Worker architecture and communication
| Requirement | Status | Evidence |
|---|---|---|
| Master accepts many Workers; unique ids | T | `integration_test: startup, protocol` (duplicate id → `REJECTED`; unsafe id refused), `test_db: worker registry` |
| Periodic heartbeats + resource updates | T | `integration_test: startup` (age < 3 s, rows in `resource_metrics`) |
| Master tracks availability, load, health, running tasks, last heartbeat | T | `GET /api/workers`; `policies: frozen Worker` (health from heartbeat age, not from the stored flag) |
| Assigns tasks to eligible Workers; Workers execute real tasks and return results | T | `execution` (all 5 CPU workloads + block tasks), `test_db` task life cycle |
| Status/results recorded in PostgreSQL | T | `test_db`, `execution`, `dashboard: numbers equal PostgreSQL counts` |
| Disconnections, timeouts, malformed messages, failed tasks | T | `protocol` (garbage, bad values), `recovery` (`kill -9` mid-task → retry elsewhere), `policies` (SIGSTOP → expiry → reconnect), `test_message` |
| No duplicate assignment / inconsistent states | T | guarded `UPDATE … WHERE status='PENDING'` + CTE; `test_db` (stale and duplicate results ignored, capacity) |
| Sockets, synchronisation, cleanup | T | ThreadSanitizer run clean; `stop_all.sh` stops everything in < 1 s |
| Dashboard shows real backend state (no simulated Workers) | T/M | every widget calls `/api/*`; test asserts no `Math.random`; 5 tabs checked visually |

## B. Multi-core processing
| Requirement | Status | Evidence |
|---|---|---|
| Several tasks per Worker, configurable limit | T | `NEXUS_WORKER_CAPACITY`; `concurrency`: max measured concurrency of W1 = 3 |
| Bounded thread pool | T | `test_worker_exec` (never above N threads; overflow rejected; 4 CPU tasks ≈ 1× not 4× wall time) |
| Scheduler considers running tasks and capacity | T | `test_scheduler: capacity`, `integration: 15 tasks on 9 slots` |
| Timestamps, task/worker ids, concurrency statistics | T | `execution_history.started_at/finished_at/concurrent_at_start`, `/api/concurrency`, dashboard timeline |
| Honest about shared host | T | `shared_host_warning`, README, dashboard banner |

## C. Distributed storage
| Requirement | Status | Evidence |
|---|---|---|
| Configurable blocks, text-aligned option | T | `test_storage` |
| Metadata in PostgreSQL (block id, dataset, index, size, checksum, Worker, replication) | T | `integration: storage` |
| Bytes stored on Worker disks | T | files `worker_data/<id>/blocks/*.blk` compared with block sizes |
| Placement, retrieval, checksum validation, reconstruction | T | upload → download byte-identical; whole-file SHA-256 |
| Configurable replication, default 2, distinct Workers | T | `test_storage: placement`, `integration` (distinct, evenly spread; replication 5 on 3 Workers → 3 replicas + warning) |
| Replica health, unavailable replicas, "cannot reconstruct" | T | corruption detected by verify and by download; Worker killed → HTTP 409 + block list; restart → readable again |
| Upload / list / inspect / download / verify / repair commands + API | T | API, `nexus_cli.sh`, Master console, dashboard |
| Tasks read real distributed blocks | T | `BLOCK_CHECKSUM`, MapReduce map tasks (replica holders only) |
| Survives restarts | T | Worker restart and Master crash/restart tests |
| Path-traversal protection | T | `test_message`, `test_worker_exec` (block store), `integration` (names, block ids, static files) |
| Automatic background re-replication | L | repair is on demand (button / command) – not required, not built |
| Files > 64 MB / streaming; dataset deletion | L | whole file held in Master memory (limit `NEXUS_MAX_UPLOAD_MB`); no delete API |

## D. Task processing / MapReduce
| Requirement | Status | Evidence |
|---|---|---|
| Existing task types retained (word count, primes, matrix, checksum) | T | `integration: execution` |
| Split → map → collect → reduce → persist, stage timings | T | `integration: mapreduce` (all five stages with durations; result == Python count, also with a Worker down) |
| Real input data | T | uploaded text corpus; result compared with an independent count (`tr|sort|uniq` in the demo) |
| Other MapReduce jobs than word count | L | not implemented (documented) |

## E. Baseline scheduling
| Requirement | Status | Evidence |
|---|---|---|
| FCFS (stable order) | T | `test_scheduler`, `test_db`, `integration: policies` (queue under saturation) |
| Round Robin (rotation, skips full/unhealthy) | T | same + frozen-Worker test |
| Priority (higher first, documented tie-break, no preemption claimed) | T | same |
| Run-time selection without recompiling; shown in dashboard and logs | T | `POST /api/policy`, console `policy`, persisted across restart |

## F. Resource-aware scheduling
| Requirement | Status | Evidence |
|---|---|---|
| Uses recent CPU, RAM, disk, running count, capacity, heartbeat age, locality | T | `test_scheduler`; formula in README §8 |
| Stale / invalid metrics rejected; ACTIVE flag not trusted | T | `test_scheduler` (age, range, never reported), `integration` (SIGSTOP) |
| Decision + metrics stored | T | `scheduling_decisions` (JSON candidates), dashboard modal; integration recomputes "chosen = minimum score" |

## G. AI-based prediction
| Requirement | Status | Evidence |
|---|---|---|
| Real model trained from PostgreSQL history | T | `integration: ml` (70 real executions → TRAINED, MAE/RMSE < baseline) |
| Features available at scheduling time only; no leakage | T | `ml/features.h`; `test_ml` noise test (held-out error stays high) |
| Train/test split, MAE & RMSE, persisted model, documented retraining | T | `trainer`, `nexus_ml`, `train_model.sh`, `ml_training_runs`, model file reload after restart |
| `AI_ASSISTED` policy; deterministic fallback | T | `test_scheduler`, `integration` (fallback flagged; predictions stored, live accuracy) |
| Honest when data is insufficient | T | `INSUFFICIENT_DATA`, no model file, no fabricated rows |
| Prediction quality on varied real workloads | L | only `CPU_BURN`-style history was generated here; quality for other task mixes depends on your history |

## H. OS resource monitoring
| Requirement | Status | Evidence |
|---|---|---|
| CPU, RAM/available, disk, load, process memory, task concurrency | T/M | non-zero real values asserted in `startup`; correctness of the percentages was not cross-checked against `top` automatically |
| Linux/WSL; other platforms explicit | M | Worker exits with a message on non-Linux (not testable here) |
| Monitoring vs managing documented | — | README §14 |

## I. PostgreSQL
| Requirement | Status | Evidence |
|---|---|---|
| Tables for workers, heartbeats/metrics, jobs, tasks, history, datasets/blocks/replicas, scheduling config + decisions, ML runs + predictions | T | `schema.sql`; `schemaPresent()` check at start |
| Keys, constraints, indexes, parameterised SQL, transactions | T | `test_db` (constraint violations, injection stored as data, rollback) |
| Idempotent setup, upgrade of the old 40 % database | T | schema applied twice in `test_db`; a database created with the original schema was upgraded in place with its rows intact (manual check) |
| No hard-coded secrets; clear errors when PostgreSQL is down | T/M | wrong password / refused connection messages checked manually |
| System-PostgreSQL branch of `setup.sh` (`sudo -u postgres`) | M | not runnable here (no sudo); the same script ran with `PSQL_ADMIN` through `local_pg.sh` |

## J. Dashboard
Cluster overview, Workers, task submission with priority, live queue, policy selector, history, decisions with predicted/actual time, dataset upload + block/replica map + integrity, MapReduce progress and results, resource charts, concurrency timeline, ML status, logs; loading / empty / disconnected states.
Status: **M** for the visual layer (screens inspected in a browser: Overview, Resources, Scheduling, Storage, MapReduce, plus the AI model / Tasks / Workers / Logs tabs in the final check), **T** for everything behind it (the API calls the dashboard makes are exercised by the integration test; HTML/JS/CSS delivery and path safety are asserted). There is no automated browser (click) test.

## K. Scripts and configuration
`check_prereqs.sh`, `build.sh`, `run_tests.sh`, `start_master.sh`, `start_worker.sh`, `start_cluster.sh`, `stop_all.sh`, `run_demo.sh`, `train_model.sh`, `storage_demo.sh`, `nexus_cli.sh`, `setup/setup.sh`, `setup/local_pg.sh`, `.env.example`, `.gitignore` – all exercised during verification (T/M).

## L. Tests – see README §17 for the matrix.

## Not verified / not done (summary)
* Multi-machine deployment, network partitions, clock skew.
* TLS / authentication for the TCP protocol and the HTTP API.
* Native Windows build.
* Automated browser tests; load tests with many simultaneous dashboard users.
* Datasets larger than the 64 MB default limit; background re-replication; dataset deletion.
* Preemptive scheduling, priority aging, task timeouts on a live-but-hung Worker.

## Additions in the completion release (evidence: docs/FINAL_TEST_REPORT.md)

| Requirement | Status | Where / evidence |
|---|---|---|
| Real XGBoost duration model (not a relabelled Random Forest) | Done | `ml/xgb_model.cpp` (libxgboost C API), `test_ml`, integration `ml` |
| Held-out MAE/RMSE, mean baseline, Random Forest comparison, persistence, fallback | Done | `ml/trainer.cpp`, `ml_training_runs` |
| Background auto-retrain | Done (observed live; no automated trigger test) | `MlManager::startAutoRetrain` |
| CPU affinity separate from threads and capacity; honest FAILED/PARTIAL status | Done | `worker/cpu_affinity.cpp`, `test_affinity`, integration `affinity` |
| Configurable Master bind / remote Worker target | Implemented; remote hardware **not tested** | `NEXUS_MASTER_BIND`, `scripts/start_remote_worker.sh` |
| Benchmarks: same workload per policy, persisted, GUI tables + charts | Done | `master/benchmark.cpp`, dashboard *Benchmarks*, integration `benchmark` |
| Documentation set, Windows/WSL guide | Done | `docs/` |
