# Troubleshooting

Each entry: **symptom → cause → fix**. Run the commands in the Ubuntu (WSL) terminal in the project directory.

## Database

| Symptom | Cause / fix |
|---|---|
| `NEXUS_DB_PASSWORD is not set. Copy .env.example to .env …` | No password configured. `cp .env.example .env`, set `NEXUS_DB_PASSWORD=…`. Scripts and the Master refuse to start without it (no default password exists). |
| `Cannot connect to PostgreSQL: … Connection refused` | The server is not running. System: `sudo service postgresql start`. Private cluster: `./setup/local_pg.sh start`. Then check the **port**: system = **5432**, private cluster = **54329** (`NEXUS_DB_PORT` in `.env`, and `NEXUS_DB_HOST=127.0.0.1` for the private one). WSL does not start services at boot. |
| `password authentication failed for user "nexus"` | `.env` password differs from the one the role was created with. Re-run `./setup/setup.sh` (or `./setup/local_pg.sh start`); it resets the role password to the current `.env` value. |
| `sudo: a terminal is required / password` in `setup.sh` | Use the no-sudo path `./setup/local_pg.sh start`, or `PSQL_ADMIN="psql -h localhost -U postgres" ./setup/setup.sh`. |
| `Database schema is missing or outdated (table 'benchmark_runs' not found)` | You upgraded the code, not the database. `./build/master --init-db` (idempotent, keeps data). |
| Tests print `SKIPPED … cannot connect to database 'nexus_test'` | Create it: `./setup/setup.sh --test-db` (or `./setup/local_pg.sh start`). |
| `could not open … .pgdata … lock file already exists` | A previous private cluster was killed. `./setup/local_pg.sh status`; if it is not running, remove `.pgdata/postmaster.pid` and start again. |

## Build

| Symptom | Fix |
|---|---|
| `Could NOT find PostgreSQL` | `sudo apt install libpq-dev` (or `./scripts/build.sh -DPostgreSQL_INCLUDE_DIR=/usr/include/postgresql`). |
| `XGBoost NOT found - building with Random Forest only` | Run `./scripts/install_xgboost.sh` then `./scripts/build.sh`. The dashboard shows *XGBoost library: not built in* until then; training XGBoost fails with a clear message, `test_ml` exits 77 (skipped XGBoost part). |
| `install_xgboost.sh`: `git clone` fails | No internet / proxy. Fix the connection and re-run (the script is restartable), or `XGBOOST_TAG=v2.1.4 ./scripts/install_xgboost.sh --force`. |
| Compile error `Compatibility with CMake < 3.5 has been removed` while building XGBoost | The script already passes `-DCMAKE_POLICY_VERSION_MINIMUM=3.5`; if you built XGBoost by hand add the same flag. |
| `error while loading shared libraries: libxgboost.so` when running a binary from another directory | Binaries built by `build.sh` have an RPATH to `third_party/xgboost-install/lib`; if you moved the folder, rebuild (`./scripts/build.sh --clean`) or set `LD_LIBRARY_PATH`. |
| `scripts/*.sh: Permission denied` or `\r: command not found` | The project was unpacked on `/mnt/c` or edited with Windows line endings: `chmod +x scripts/*.sh setup/*.sh` and `sed -i 's/\r$//' scripts/*.sh setup/*.sh`; best keep it under `~`. |

## Running

| Symptom | Cause / fix |
|---|---|
| `Cannot listen on port 5050` / `Cannot start the HTTP API on port 8080` | Another Master is running or the port is used: `./scripts/stop_all.sh`, `ss -ltnp \| grep -E '5050\|8080'`, or change `NEXUS_MASTER_PORT` / `NEXUS_HTTP_PORT`. |
| Worker: `Registration REJECTED … already connected` | That Worker id is alive. Use another id or wait ~10 s after killing the old process. |
| Worker loops on `Connect failed (…)` | Master not running, wrong `NEXUS_MASTER_HOST/PORT`, or (remote) the firewall blocks TCP 5050. Test: `bash -c 'exec 3<>/dev/tcp/<ip>/5050' && echo reachable`. |
| `start_remote_worker.sh: ERROR: '127.0.0.1' is this computer` | Correct – on a remote machine give the **Master computer's LAN IP**. |
| Dashboard unreachable from Windows | WSL 2 forwards `localhost:8080`; if you changed `NEXUS_HTTP_BIND`, keep `127.0.0.1` or use the WSL IP. Binding `0.0.0.0` exposes an **unauthenticated** API. |
| Dashboard says *Master unreachable* | The Master stopped; it reconnects by itself every 2 s once it is back. |
| Worker log: `CPU affinity FAILED: none of the requested CPUs (9) is available; allowed CPUs are 0-3 – Worker is NOT pinned` | You asked for a core that does not exist (or that cgroups/`taskset` forbids). Use `nproc` / `grep Cpus_allowed_list /proc/self/status`, then set `NEXUS_WORKER_CPU_AFFINITY` to an allowed id. The Worker keeps running **unpinned** and the dashboard shows *pin FAILED* – it never pretends otherwise. |
| `CPU affinity PARTIAL … SHARES a core` | More Workers than allowed CPUs with `auto`: two Workers share a core. Start fewer Workers or use `-a none`. |
| `WARNING: capacity 4 > 1 usable CPU(s): CPU-bound tasks will time-share` | Capacity exceeds the pinned CPUs (oversubscription). Lower `NEXUS_WORKER_CAPACITY` or pin more CPUs (`NEXUS_WORKER_CPU_AFFINITY=0-3`). |
| `NEXUS Worker needs Linux (/proc …)` | Run it inside WSL, not in native Windows. |

## Machine learning

| Symptom | Fix |
|---|---|
| `INSUFFICIENT_DATA – need at least 30 completed executions, have N` | Expected with little history. Run more tasks (*Tasks → Count 60*), then train again. No model is created until then; `AI_ASSISTED` uses the Resource-Aware fallback and marks decisions `fallback`. |
| `NOT_ACTIVATED … not better than the baseline` | The model did not beat "predict the mean" on held-out data (e.g. all tasks nearly identical). Submit more varied tasks (different types/scales) and retrain. |
| `XGBoost was requested but this build has no libxgboost` | `./scripts/install_xgboost.sh && ./scripts/build.sh`, or train the Random Forest explicitly. |
| AI policy shows *fell back* in benchmarks | No usable model when the run started. Train first (or tick *retrain the model first*). |

## Benchmarks and storage

| Symptom | Fix |
|---|---|
| `409 the cluster is busy (tasks pending or running)` | Wait until the queue is empty – benchmarks need an idle cluster so every policy starts from the same state. |
| `409 no healthy Worker is connected` | Start Workers first (`./scripts/start_cluster.sh`). |
| A benchmark shows status `FAILED … interrupted: the Master stopped` | The Master was stopped during the run; start a new benchmark. Completed policy runs are kept. |
| Upload: `no healthy Workers available to store blocks` | Start Workers. With fewer Workers than the replication factor the upload succeeds but reports *under-replicated* blocks. |
| MapReduce says the dataset is not text-aligned | Upload again with *text-aligned* ticked (`text=1`). |
| Download returns 409 with `unavailable_blocks` | Every Worker holding a replica of those blocks is down or corrupt. Restart the Workers, then `verify` / `repair`. |

## Windows / WSL

| Symptom | Fix |
|---|---|
| `wsl: command not found` / WSL 1 | Install WSL 2: `wsl --install -d Ubuntu`; convert with `wsl --set-version Ubuntu 2`. |
| Everything is slow | The project is on `/mnt/c`. Move it to `~` (Linux file system). |
| Database/cluster gone after reboot | Services do not auto-start in WSL: start PostgreSQL again (docs/RUN_GUIDE section 19). Data in `.pgdata/` or the system cluster is preserved. |
