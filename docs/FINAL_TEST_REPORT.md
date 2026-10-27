# Final test report

Everything below was produced by commands actually run on 2026-10-27 in the environment listed. Numbers are copied from the real outputs; nothing is estimated.
Remote-machine execution and other items that could **not** be tested are listed explicitly in §9.

## 1. Environment

| Item | Version |
|---|---|
| Host | Windows 11 Home, WSL 2 (kernel `6.18.54.1-microsoft-standard-WSL2`), 12 logical CPUs, 7.8 GB RAM |
| Distribution | Ubuntu 26.04.1 LTS |
| Compiler / build | g++ 15.2.0 (C++17), CMake 4.2.3, GNU Make |
| PostgreSQL | server and client 18.6 (private rootless cluster via `setup/local_pg.sh`, port 54329); libpq 18.6 |
| XGBoost | **2.1.4** (`dmlc/xgboost` tag v2.1.4, built from source by `scripts/install_xgboost.sh`, C API) |
| Python | 3.14.4 (standard library only; used by the integration test and CLI helpers) |
| Dependency note | no `pip`, no `sudo` were available in the WSL instance; XGBoost was therefore built from source into `third_party/xgboost-install` |

## 2. Build (A)

| Command | Result |
|---|---|
| `./scripts/build.sh --clean` | success; `-- XGBoost found: …/libxgboost.so`; **0 compiler warnings** (`-Wall -Wextra`) |

## 3. Unit, database and integration tests (B, C, D, E)

Command: `./scripts/run_tests.sh` (database `nexus_test`), then repeated through `ctest -E integration_test`.

| Test program | Checks | Failed | Result |
|---|---|---|---|
| `test_message` | 57 | 0 | PASSED |
| `test_scheduler` | 67 | 0 | PASSED |
| `test_worker_exec` | 86 | 0 | PASSED |
| `test_storage` | 19 876 | 0 | PASSED |
| `test_ml` (Random Forest **and** XGBoost) | 126 | 0 | PASSED (XGBoost part executed, not skipped) |
| `test_http` | 24 | 0 | PASSED |
| `test_affinity` (CPU affinity, benchmark plan/validation) | 134 | 0 | PASSED |
| `test_db` (database tests) | 137 | 0 | PASSED |
| `integration_test.py` | 165 | 0 | PASSED |
| `ctest -E integration_test` | 8 tests | 0 | 100 % passed in 7.9 s |

Integration test sections (real Master + real Workers + real PostgreSQL): startup 7 · api 9 · protocol 7 · execution 9 · concurrency 9 · policies 12 · recovery 9 · storage 29 · mapreduce 12 · **ml 22** · **affinity 8** · **benchmark 14** · persistence 9 · dashboard 9 = **165 checks, 0 failed**.
Before this work the same suite had 135 checks (ml 15, persistence 8); all of them still pass. **Skipped tests: none** in the final run.

One real defect was found by the new tests and fixed before the final run: a Worker whose pinning *failed* still reported the unpinned CPU mask as its `cpu_affinity`, which would have misrepresented an unpinned Worker as pinned; the Worker now reports a CPU list only when pinning was applied/verified (check *"A2: an unavailable CPU is reported FAILED, not silently claimed"*).

## 4. XGBoost validation (held-out split)

Unit test data (synthetic, **test-only**, never used by the running system) – `test_ml`:

| Case | XGBoost MAE | RMSE | Baseline (mean) MAE / RMSE | Random Forest MAE / RMSE |
|---|---|---|---|---|
| learnable task-duration function, 240 rows (192 train / 48 test) | 0.073 | 0.115 | 1.948 / 2.452 | 0.215 / 0.324 |
| unlearnable noise, 300 rows (leakage check) | 2.67 | – | 2.70 | – (RF 2.72) |

Real runs on the live cluster (`ml_training_runs`, real `execution_history`, 80/20 seeded split, MAE/RMSE in seconds):

| Run | Samples (train/test) | XGBoost MAE | XGBoost RMSE | Baseline MAE / RMSE | Random Forest MAE / RMSE (same split) |
|---|---|---|---|---|---|
| 1 (first session, capacity 4 Workers) | 114 (92/22) | 0.177 | 0.282 | 3.014 / 3.489 | 0.374 / 0.551 |
| 2 | 120 (96/24) | 0.411 | 0.890 | 3.450 / 3.833 | 0.400 / 0.787 |
| 4 | 231 (185/46) | 0.360 | 0.664 | 2.049 / 2.449 | 0.258 / 0.419 |
| 5 | 344 (276/68) | 0.354 | 0.935 | 2.159 / 3.557 | 0.409 / 0.916 |

Honest reading: XGBoost beats the "predict the mean" baseline by a wide margin in every run, but **is not consistently better than the Random Forest** on this small, regular data (RF is better in runs 2 and 4, XGBoost in 1 and 5). The comparison is stored and displayed precisely so this can be seen. Held-out numbers describe this cluster and these synthetic workloads only.
Integration checks confirmed: empty history ⇒ `INSUFFICIENT_DATA` and no model file; model file carries the `NEXUSXGB` header; Random Forest trains as a separate labelled model; predictions grow with task scale (scale 1 < scale 10); predicted-vs-actual stored.

## 5. CPU affinity (verified, not assumed)

* `test_affinity`: `sched_setaffinity` to one CPU → `sched_getaffinity` returns exactly that CPU; invalid/partial/auto cases return `FAILED` / `PARTIAL` / distinct CPUs.
* Integration (`affinity` section): Worker pinned to CPU 11 → API `APPLIED`, `/proc/<pid>/status` `Cpus_allowed_list` = 11 **and every thread of the process** = 11; Worker with CPU 9999 → `FAILED`, left unpinned; `auto` index 0 → first allowed CPU; capacity 1, 2 executor threads and affinity stored as separate values in PostgreSQL.
* Live cluster (`./scripts/start_cluster.sh`): W1 → CPU 0, W2 → CPU 1, W3 → CPU 2, `capacity=1 threads=1`; independently confirmed with `grep Cpus_allowed_list /proc/<pid>/status` (`0`, `1`, `2`).

## 6. Scheduling benchmark (measured, live cluster)

Benchmark #1 (`POST /api/benchmarks`, retrained XGBoost first = training run 3): MIXED workload, **30 tasks per policy**, scale 1–2, varied priorities, seed 11, **2 repeats** (policy order rotated), 3 Workers pinned to CPUs 0/1/2, capacity 1 each. All 300 tasks completed, 0 failures, 0 retries.

| Policy | Rep 1 makespan | Rep 2 makespan | Mean makespan | Mean avg-exec | Mean avg-wait | Mean throughput |
|---|---|---|---|---|---|---|
| FCFS | 15.24 s | 13.76 s | 14.50 s | 1.31 s | 6.12 s | 2.07 t/s |
| Round Robin | 15.63 s | 15.02 s | 15.32 s | 1.37 s | 6.27 s | 1.96 t/s |
| Priority | 16.76 s | 15.01 s | 15.88 s | 1.51 s | 6.71 s | 1.89 t/s |
| Resource-Aware | 16.73 s | 14.59 s | 15.66 s | 1.38 s | 6.53 s | 1.92 t/s |
| AI-Assisted (XGBoost) | 15.41 s | 13.43 s | 14.42 s | 1.35 s | 6.15 s | 2.09 t/s |

AI-Assisted: 60/60 decisions used real XGBoost predictions (0 fallbacks); predicted-vs-actual MAE 0.155 s (rep 1) and 0.201 s (rep 2).
**Interpretation:** the policies differ by about 10 % in mean makespan, but the same policy varied by up to 1.75 s (≈ 10 %) between its two repeats (Priority 16.76 s vs 15.01 s; FCFS 15.24 s vs 13.76 s). The differences are therefore **within run-to-run noise** on this small single-host cluster with a homogeneous, short-task workload; this run does **not** establish that any policy is faster. The benchmark machinery (identical seeded workload, idle cluster, batch submission, order rotation, measurement from PostgreSQL) is what is verified; a meaningful ranking needs more repeats, more varied workloads or heterogeneous Workers.
The integration test additionally verified that all five policies received the identical workload, that each run's scheduling decisions were made under that run's policy, FCFS dispatch in id order, PRIORITY dispatch in descending priority, and that the reported makespan equals an independent SQL calculation.

## 7. Dashboard

Verified in the built-in browser against the live cluster: the *Workers* tab shows per-Worker `pinned CPU 0/1/2`, `Tasks running / max` and `Exec. threads`; the *Benchmarks* tab renders the stored run (table + six bar charts + per-Worker load, labelled *latest run*) with no console errors; the *AI model* tab shows the active algorithm and run history. The integration `dashboard` section checks the served files and that dashboard numbers equal PostgreSQL counts.

## 8. Clean-install test (F)

The ZIP (before this section was added) was unpacked into a fresh directory in WSL and the documented steps were followed verbatim: `check_prereqs.sh` → `cp .env.example .env` (password, private-cluster port 54330, Master/HTTP ports 5060/8090 set) → `install_xgboost.sh` (built XGBoost 2.1.4 from source) → `setup/local_pg.sh start` (creates `nexus` and `nexus_test` from `schema.sql` on a brand-new cluster) → `build.sh` → `run_tests.sh --unit` → `run_tests.sh`.
Result: every step succeeded; all unit tests, `test_db` (137 checks) and `integration_test.py` (**165 checks, 0 failed**) PASSED, identical counts to §3; no test skipped. The only difference between the tested ZIP and the delivered one is this report section (plus none of the source files).

## 9. Not tested / limitations

* **Workers on separate physical computers were NOT tested** (only one computer was available). `NEXUS_MASTER_BIND`, `NEXUS_MASTER_HOST` and `scripts/start_remote_worker.sh` exist and local TCP behaviour is covered, but multi-machine operation, firewall configuration and cross-host clock skew are unverified. Separate Worker processes on one host give parallelism, **not** physical-machine fault tolerance.
* Native Windows execution (unsupported), distributions other than Ubuntu 26.04, other PostgreSQL major versions.
* Soak tests, datasets > 64 MB, high Worker counts (> 3 in the integration test).
* CPU pinning constrains the Worker process; it does not reserve a core against other processes.
* Execution is at-least-once with idempotent bookkeeping, **not** exactly-once.
* A worker-failure test during a *benchmark run* was not performed (worker-kill recovery is covered by the `recovery` integration section during normal workloads).
* No claim of being bug-free is made; this report lists what was exercised.

## 10. Exact verification commands

```bash
cd ~/NEXUS
./scripts/check_prereqs.sh
./scripts/install_xgboost.sh
./setup/local_pg.sh start          # or: ./setup/setup.sh --test-db  (system PostgreSQL)
./scripts/build.sh --clean
./scripts/run_tests.sh --unit
./scripts/run_tests.sh
./scripts/start_cluster.sh
curl -s -X POST localhost:8080/api/benchmarks -d "policies=all&type=MIXED&count=30&scale_min=1&scale_max=2&seed=11&repeats=2&retrain_before=1"
for p in $(pgrep -x worker); do grep Cpus_allowed_list /proc/$p/status; done
./scripts/stop_all.sh
```

## 11. Final status summary

| Requested feature | Status | Evidence |
|---|---|---|
| C++17 Master–Worker over TCP, framing | Done (pre-existing, preserved) | `test_message`, integration `protocol` |
| Registration, heartbeats, failure detection | Done (pre-existing, preserved) | `test_db` heartbeat expiry, integration `recovery` |
| Multicore: CPU affinity | **Added**, verified on Linux | `test_affinity`, integration `affinity` (/proc check), live cluster |
| Affinity / threads / capacity are separate settings | **Added** | env vars, DB columns, dashboard columns, `docs/ARCHITECTURE.md` §7 |
| Unavailable cores handled, no false pinning claim | **Added** | `FAILED`/`PARTIAL` tests; defect found and fixed (§3) |
| Master bind address / remote Master host configurable | **Added** (`NEXUS_MASTER_BIND`, `start_remote_worker.sh`) | local TCP tested; **remote machines NOT tested** |
| Distributed storage, replication, checksums, reconstruction, repair | Done (pre-existing, preserved) | integration `storage` 29 checks, `test_storage` |
| Data-locality scheduling | Done (pre-existing), now measured in benchmarks | `test_scheduler`, benchmark `local_tasks` column |
| MapReduce vs independent result | Done (pre-existing, preserved) | integration `mapreduce` 12 checks |
| Five policies selectable in GUI | Done, verified end to end | integration `policies`, `benchmark` |
| Real XGBoost | **Added** (libxgboost 2.1.4 via C API) | `test_ml` 126 checks, integration `ml` 22 checks |
| Train/validation split, MAE/RMSE, baseline, RF comparison | **Added/extended** | §4 |
| Model persistence, reload, fallback, auto-retrain | **Added/extended** | `test_ml`, integration `ml`/`persistence`; background auto-retrain observed live (runs 4 and 5 were triggered automatically during the benchmark, "Auto-retrain triggered (111 new executions)"), but **no automated test** covers the trigger |
| Predictions vs actual stored in PostgreSQL | Done (extended with model type) | `ml_predictions`, benchmark MAE/RMSE |
| Algorithm comparison GUI (tables, charts, persistent) | **Added** | integration `benchmark` 14 checks, §6, §7 |
| Failure recovery / rescheduling | Done (pre-existing, preserved) | integration `recovery` 9 checks |
| PostgreSQL schema, clean init, test DB | Done, extended | `test_db`, clean install |
| Documentation set + Windows/WSL guide | **Added** | `docs/` |
| Remote multi-machine execution | Implemented, **not verified** | §9 |

## 12. Caveats on the benchmark in §6

* The automatic background retrain fired twice *during* that benchmark (after +111 and +113 completed executions), so the XGBoost model used by the AI-Assisted runs may have been swapped mid-run. This is the designed, non-blocking behaviour, but it means the AI runs were not made with a frozen model; set `NEXUS_ML_AUTO_RETRAIN_EVERY=0` for a strictly frozen-model comparison.
* Model runs 2 and 3 show identical numbers because they were trained on the same 120 history rows.
