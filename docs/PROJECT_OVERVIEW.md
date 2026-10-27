# NEXUS – project overview

## 1. Problem

Processing and storing a large data set on one computer is limited by that computer's CPU cores, memory and disk. Systems such as Hadoop solve this by
splitting data into **blocks**, **replicating** the blocks on several machines, and moving the **computation to the data**. NEXUS is a small, readable,
Hadoop-inspired system that implements those ideas end to end so they can be studied, demonstrated and measured:

* *How is work distributed?* – a **Master** schedules tasks onto **Workers** over TCP.
* *Which Worker should run a task?* – five interchangeable **scheduling policies**, one of which is driven by a machine-learning model that predicts task duration.
* *What happens when a Worker dies?* – heartbeats detect it, its tasks are retried elsewhere, replicas keep the data readable.
* *Where does the data live?* – checksummed blocks, physically stored in each Worker's directory, described in PostgreSQL.

## 2. Objectives and where they are met

| Objective | Implementation |
|---|---|
| C++17 Master–Worker over TCP with message framing | `common/message.*`, `master/master_server.*`, `worker/worker_client.*` – line protocol `TYPE\|key=value\|…` (see [ARCHITECTURE.md](ARCHITECTURE.md)) |
| Registration, heartbeats, failure detection | `master/worker_manager.*` (health = age of the last heartbeat) |
| Multi-core aware execution | per-Worker thread pool + **CPU affinity** (`worker/cpu_affinity.*`); affinity / threads / capacity are separate settings |
| Distributed block storage with replication | `master/storage_manager.*`, `worker/block_store.*`, `common/chunker.h` ([DISTRIBUTED_STORAGE.md](DISTRIBUTED_STORAGE.md)) |
| Data-locality-aware scheduling | block tasks only run on replica holders; Resource-Aware gives a locality bonus |
| MapReduce | `master/mapreduce.*` – word count: SPLIT → MAP → SHUFFLE → REDUCE → FINALIZE |
| Five scheduling policies | `master/scheduler.*` ([SCHEDULING_ALGORITHMS.md](SCHEDULING_ALGORITHMS.md)) |
| XGBoost task-duration prediction | `ml/xgb_model.*`, `ml/trainer.*`, `master/ml_manager.*` ([MACHINE_LEARNING.md](MACHINE_LEARNING.md)) |
| PostgreSQL metadata and history | `database/schema.sql`, `database/database.*` ([DATABASE_SCHEMA.md](DATABASE_SCHEMA.md)) |
| Resource monitoring | `worker/resource_monitor.*` reads `/proc`; history in `resource_metrics` |
| Web dashboard incl. algorithm comparison | `dashboard/` (Benchmarks tab) + `master/benchmark.*` |
| Automated tests | `tests/` ([TESTING_AND_VALIDATION.md](TESTING_AND_VALIDATION.md)) |

## 3. Use cases

1. **Demonstration of a distributed system** – start a Master and several Workers, upload a file, watch blocks and replicas appear in the Workers' directories, run MapReduce, kill a Worker and watch tasks recover.
2. **Scheduling study** – run the same workload under FCFS / Round Robin / Priority / Resource-Aware / AI-Assisted and compare measured makespan, waiting time and utilisation.
3. **ML-in-the-loop systems study** – train XGBoost on the cluster's own history, see held-out MAE/RMSE against a baseline, then watch the scheduler use the predictions.

## 4. Features

Cluster and workers · task submission with priorities · five scheduling policies selectable from the GUI at run time · decision log with the metrics every decision used ·
CPU pinning with verified status · distributed storage with checksums, verify, repair, download · MapReduce word count · XGBoost model with train/test evaluation,
baseline and Random Forest comparison, auto-retrain · persistent benchmarks · resource charts and concurrency timeline · log viewer · REST/JSON API.

## 5. Technology stack

| Layer | Technology |
|---|---|
| Language / build | C++17, CMake ≥ 3.10, GCC (tested with g++ 15.2) |
| Networking | POSIX sockets, own line protocol, one thread per Worker connection |
| Database | PostgreSQL via libpq (parameterised SQL, transactions) |
| ML | **XGBoost 2.1.4** (C API, built by `scripts/install_xgboost.sh`); from-scratch Random Forest as an alternative |
| GUI | static HTML + JavaScript + CSS served by the Master (no framework, no external requests) |
| Tests | C++ unit tests (ctest), Python 3 standard-library integration test with real processes |
| Platform | Linux; on Windows use WSL 2 (Ubuntu). The Worker needs `/proc` and `sched_setaffinity` |

## 6. Limitations (honest list)

* Verified on **one computer**. Several Worker processes on one host share its RAM and CPU package; they provide parallelism but **not** physical-machine fault tolerance.
  Workers on separate physical computers use the same code path but were **not tested on real second hardware**.
* The Master is a single point of failure (its state is in PostgreSQL, so a restart recovers, but nothing runs while it is down).
* No authentication or encryption on the TCP port or the HTTP API – localhost / trusted LAN only.
* Execution is **at-least-once with idempotent bookkeeping**, not exactly-once (see [ARCHITECTURE.md](ARCHITECTURE.md) §9).
* Scheduling is non-preemptive; Priority has no aging; a hung task on a live Worker has no timeout.
* Storage: whole upload held in memory (default limit 64 MB), no dataset deletion, re-replication is manual (`repair`), no encryption.
* MapReduce: word count only; intermediate data flows through the Master and PostgreSQL.
* CPU pinning constrains the Worker but does not reserve a core exclusively; CPU/RAM figures are host-wide.
* Benchmark differences between policies on a small, lightly loaded, single-host cluster are small and dominated by noise (see [SCHEDULING_ALGORITHMS.md](SCHEDULING_ALGORITHMS.md) §7).
