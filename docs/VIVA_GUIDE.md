# PBL / viva guide

## 1. Which module demonstrates which subject

| Subject | Concept | Where in NEXUS |
|---|---|---|
| **DBMS** | relational schema, keys, constraints | `database/schema.sql` – PKs, FKs with `ON DELETE CASCADE`, `CHECK` constraints, indexes |
| | transactions / atomicity | `Database::transact`, CTE-based claim in `TaskManager::assign`, `mr_reduce_outputs` marker committed with the results |
| | parameterised SQL, injection safety | every query in `database/database.cpp`, `master/*.cpp` uses `$1…`; test `constraints, foreign keys, parameterised SQL` |
| | idempotent migrations | `CREATE … IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`, `master --init-db` |
| | state machine with guarded updates (no double assignment) | `UPDATE tasks … WHERE status='PENDING'` |
| | analytical SQL (window functions) | max concurrency per Worker in `GET /api/concurrency` (`sum(d) OVER (PARTITION BY worker_id ORDER BY t, d)`) |
| | metadata vs data separation | PostgreSQL = block catalogue; bytes on Worker disks |
| **Operating systems** | processes & threads | Master/Worker processes; `std::thread`; bounded `ThreadPool` (`common/thread_pool.h`) |
| | synchronisation | mutexes + condition variables (`TaskManager`, `WorkerManager` RPC table, thread pool), no data races in shared maps |
| | CPU / memory / disk monitoring | `worker/resource_monitor.cpp` – `/proc/stat`, `/proc/meminfo`, `/proc/loadavg`, `/proc/self/statm`, `statvfs()` |
| | file system, atomic writes | `worker/block_store.cpp` – temp file + `rename()`, canonical root, id validation (path traversal) |
| | signals, clean shutdown, process health | `SIGTERM/SIGINT/SIGPIPE` handling, `scripts/stop_all.sh`, heartbeat expiry |
| | scheduling (CPU-scheduling analogy) | `master/scheduler.cpp` – FCFS, Round Robin, Priority (non-preemptive), plus resource-aware |
| **Networking** | TCP sockets (`socket/bind/listen/accept/connect/poll`) | `common/message.cpp`, `master/master_server.cpp`, `master/http_server.cpp` |
| | application protocol over a byte stream (framing) | line-based `TYPE|k=v` protocol; the receive buffer handles split / merged TCP segments (`test_message`) |
| | failure detection | heartbeats + timeout (3 missed beats), reconnect with retry |
| | HTTP/1.1 by hand | `master/http_server.cpp` (routing, `Content-Length`, `Expect: 100-continue`, limits) |
| **Distributed systems** | master–worker architecture | `master/`, `worker/` |
| | replication, placement, consistency check | `common/chunker.h` (placement), `master/storage_manager.cpp` (verify / repair / read-repair by checksum) |
| | data locality | block tasks run only on replica holders; resource score bonus |
| | fault tolerance | retry on another Worker, `attempt` numbers, recovery after Master restart, `integration_test.py` section *recovery* |
| | MapReduce | `master/mapreduce.cpp`, `worker/task_executor.cpp`, `common/wordcount.h` |
| | shared host honesty | dashboard banner + `shared_host_warning` – processes ≠ machines |
| **Scheduling** | baselines vs adaptive | five selectable policies, decisions logged with metrics |
| **AI / ML** | supervised regression, train/test split, MAE/RMSE, baseline, leakage | `ml/xgb_model.cpp`, `ml/random_forest.cpp`, `ml/trainer.cpp`, `tests/test_ml.cpp` |
| | integrating a model into a system with a safe fallback | `Scheduler::pick` AI branch, `MlManager::predictor()` |

## 2. Likely questions (and short answers)

**Why is this multi-core and not just several processes?** Each Worker has a thread pool of `capacity` threads; the Master counts running tasks per Worker and never exceeds it. The proof is measured: `execution_history.started_at/finished_at` overlap, 3 × 1 s tasks finish in ≈ 1 s. Several Workers on one laptop still share the same CPU – the dashboard says so.

**How does the Master know a Worker died?** Two ways: the TCP socket closes (reader thread sees EOF), or no heartbeat for 3 intervals (monitor thread; handles a hung process that keeps its socket open). Health is always computed from the heartbeat age, never from the stored `ACTIVE` flag. Its in-flight tasks are re-queued with the `attempt` counter increased; a late result of the old attempt is ignored.

**How do you avoid assigning a task twice?** A single dispatch thread, plus the claim is `UPDATE … WHERE status='PENDING'` inside a CTE together with the decision row. If it affects 0 rows nothing is sent.

**What happens if two replicas are on the same Worker?** Never: `chooseReplicaNodes` returns distinct Worker ids; with fewer Workers than the factor, fewer replicas are stored and the dataset is flagged *under-replicated*.

**How is data integrity ensured?** SHA-256 per block (stored in PostgreSQL), checked by the Worker on store, by `verify` (re-hash on disk), and by every read; the whole file's SHA-256 is checked after reconstruction. Corrupt replicas are marked and skipped; `repair` restores them.

**What is the difference between monitoring and managing resources?** NEXUS only *reads* OS counters (`/proc`, `statvfs`) and uses them to decide placement. It does not set cgroup limits, priorities or memory quotas; the OS still schedules the threads.

**Explain the resource-aware score.** Weighted sum of normalised CPU, RAM, disk, slot occupancy and heartbeat age, minus a locality bonus (`README §8`). Stale/invalid metrics make a Worker ineligible. The numbers behind every decision are stored.

**Why XGBoost (and why keep a Random Forest)?** Small tabular data with non-linear interactions (task type × scale × load): gradient-boosted trees are the standard strong baseline, need no feature scaling and train in milliseconds. A from-scratch Random Forest is kept as an honestly labelled alternative; every training run scores both on the same split. Evaluated on held-out data against a "predict the mean" baseline; if it does not beat that, it is not activated.

**How do you prevent data leakage in the ML part?** Features contain only scheduling-time information; no per-type target statistics; the model is fitted on the training split only; a test proves that on pure noise the held-out error stays high (a leaking pipeline would show ≈ 0).

**What happens without a trained model?** `AI_ASSISTED` falls back to `RESOURCE_AWARE`; the decision row says `fallback=true`; training reports `INSUFFICIENT_DATA` and creates no model.

**What would you need for real multi-machine use?** Run Workers on other hosts with `NEXUS_MASTER_HOST=<master ip>`, open port 5050, add TLS + authentication, synchronise clocks, and test network partitions – none of that was verified here.

## 3. Five-minute live demo order

1. `./scripts/start_cluster.sh -w 3 -c 4` → dashboard *Overview / Workers* (shared-host banner).
2. *Tasks*: submit 12 × `CPU_BURN` scale 10 → *Resources* tab: concurrency timeline.
3. *Scheduling*: switch FCFS → Priority → Round Robin and show the decision log.
4. *Storage*: upload a text file (tick text-aligned) → block map → corrupt a block file with `printf '\xff' | dd of=worker_data/W1/blocks/<id>.blk bs=1 seek=5 conv=notrunc` → **Verify** → **Repair** → **Download**.
5. *MapReduce*: run word count → stage timeline → top words.
6. *AI model*: **Train** → MAE/RMSE vs baseline → switch to AI_ASSISTED → predicted vs actual.
7. `kill -9` a Worker during a long task → it is retried elsewhere (*Tasks* → click the task).
