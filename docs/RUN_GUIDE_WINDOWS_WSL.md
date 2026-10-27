# Run guide – Windows + WSL/Ubuntu + VS Code

Every command block says **which terminal** it belongs to. "**Ubuntu terminal**" = a WSL Ubuntu shell (in VS Code: *Terminal → New Terminal* after connecting to WSL).
Commands were verified on WSL 2, Ubuntu with g++ 15.2, CMake 4.2, PostgreSQL 18 client/server binaries, Python 3.14. Older Ubuntu LTS versions (22.04 / 24.04) work the same way; CMake ≥ 3.10 and g++ with C++17 are required.

## 1. Prerequisites

| Need | Check / install |
|---|---|
| Windows 10/11 with **WSL 2** and an **Ubuntu** distribution | **Windows PowerShell:** `wsl --status` and `wsl -l -v` (VERSION must be 2). If missing: `wsl --install -d Ubuntu`, reboot, create your Linux user. |
| **VS Code** + the extension **WSL** (ms-vscode-remote.remote-wsl) | https://code.visualstudio.com/ |
| Internet access (once, to install packages and to download XGBoost) | – |

## 2. Open Ubuntu in VS Code and unpack the project

**Windows PowerShell** (replace the path with where the ZIP is):
```powershell
wsl -d Ubuntu
```
**Ubuntu terminal** – keep the project in the Linux file system (`~`), **not** under `/mnt/c` (slow, loses script permissions):
```bash
sudo apt update
sudo apt install -y unzip
cp /mnt/c/Users/<YOUR_WINDOWS_USER>/Downloads/NEXUS_FINAL_COMPLETED.zip ~/
cd ~ && unzip -o NEXUS_FINAL_COMPLETED.zip
cd ~/NEXUS
code .          # opens VS Code connected to WSL in this folder
```
Expected: VS Code opens with `WSL: Ubuntu` in the bottom-left corner. Use *Terminal → New Terminal* inside it for all following commands (it starts in `~/NEXUS`).

## 3. Install system dependencies (Ubuntu terminal)
```bash
cd ~/NEXUS
sudo apt install -y build-essential cmake git libpq-dev postgresql postgresql-client python3 curl
./scripts/check_prereqs.sh
```
Expected: every line `[ok]` except `.env` and `XGBoost library` (`[todo]`) at this point. If `chmod` problems appear: `chmod +x scripts/*.sh setup/*.sh`.

## 4. Build XGBoost (one time, ~5–15 min, no sudo, no pip)
```bash
./scripts/install_xgboost.sh
```
Expected last lines: `XGBoost v2.1.4 installed in …/third_party/xgboost-install`. (Skipping this step is allowed: NEXUS then builds with Random Forest only and the dashboard says *XGBoost library: not built in*.)

## 5. Configure `.env`
```bash
cp .env.example .env
nano .env        # or edit it in VS Code
```
Set at least `NEXUS_DB_PASSWORD=<a password you choose>` (never commit `.env`). Everything else has a working default.
**Which PostgreSQL?** Choose one of the two options of section 6; the port differs:

| Option | `NEXUS_DB_HOST` | `NEXUS_DB_PORT` |
|---|---|---|
| A – system PostgreSQL service | `localhost` | `5432` |
| B – private no-sudo cluster (`setup/local_pg.sh`) | `127.0.0.1` | `54329` |

## 6. Initialise PostgreSQL (both the main and the test database)

**Option A – system PostgreSQL** (asks for your sudo password):
```bash
sudo service postgresql start
./setup/setup.sh --test-db
```
**Option B – private cluster, no sudo** (put `NEXUS_DB_HOST=127.0.0.1` and `NEXUS_DB_PORT=54329` in `.env` first):
```bash
./setup/local_pg.sh start
```
Expected: `==> Creating role 'nexus'`, `==> Creating database 'nexus'`, `…'nexus_test'`, a list of 19 tables, `Done. Database 'nexus' is ready.`
The scripts are **safe to re-run** (idempotent; they never drop data). The test database `nexus_test` is separate – the automated tests **wipe it**, never `nexus`.

## 7. Build
```bash
./scripts/build.sh
```
Expected: `-- XGBoost found: …/libxgboost.so` and finally `Built: …/build/master  …/build/worker  …/build/nexus_ml`. (First build ≈ 1–2 min.)

## 8. Run the tests
```bash
./scripts/run_tests.sh --unit     # no database needed
./scripts/run_tests.sh            # + database tests + end-to-end test (needs nexus_test, section 6)
```
Expected end of the output:
```
==================== TEST SUMMARY ====================
  test_message … test_affinity   PASSED
  test_db            PASSED
  integration_test   PASSED
```
A test that cannot run says `SKIPPED (reason)` – it is never counted as passed. The full end-to-end run takes ≈ 5–8 minutes.

## 9. Start the local cluster
```bash
./scripts/start_cluster.sh              # Master + 3 Workers, each pinned to its own CPU (when enough CPUs exist)
```
Expected:
```
Cluster is up: Master + 3 Workers (max 1 concurrent task(s) each, CPU affinity mode: auto).
  W1   affinity=APPLIED  cpus=0  capacity=1 threads=1
  W2   affinity=APPLIED  cpus=1  …
Dashboard: http://127.0.0.1:8080/
```
Variants: `-w 4` (4 Workers), `-c 2` (capacity), `-a none` (no pinning), `NEXUS_CPU_MAP=2,4,6 ./scripts/start_cluster.sh` (explicit CPUs).
Check the pinning independently: `for p in $(pgrep -x worker); do grep Cpus_allowed_list /proc/$p/status; done`.
Logs: `logs/master.log`, `logs/worker_W1.log`. Foreground alternative (one terminal each): `./scripts/start_master.sh`, `./scripts/start_worker.sh W1 1`.

## 10. Open the dashboard
In **Windows**, open a browser at **http://localhost:8080/** (WSL 2 forwards localhost automatically). Tabs: Overview, Workers, Tasks, Scheduling, Benchmarks, Storage, MapReduce, Resources, AI model, Logs. The pill *live* (top right) shows the Master connection.

## 11. Submit tasks
*Dashboard → Tasks → Submit*: type `CPU_BURN`, scale 3, priority 5, count 10 → **Submit**. The queue table shows PENDING → ASSIGNED → RUNNING → COMPLETED and the Worker that ran each task.
Command line: `./scripts/nexus_cli.sh submit CPU_BURN 3 5` and `./scripts/nexus_cli.sh tasks 10`.

## 12. Upload a dataset
*Dashboard → Storage → Upload*: choose a text file, block size 262144, replication 2, tick **text-aligned** (needed for MapReduce) → **Upload**. Command line: `./scripts/nexus_cli.sh upload myfile.txt 2 65536 1`.
Click the dataset to see every block with its replicas. The physical files:
```bash
ls -l worker_data/W1/blocks/ worker_data/W2/blocks/     # <block_id>.blk files = the real bytes
```
Download = reconstruction with checksum validation: `./scripts/nexus_cli.sh download 1 out.txt && sha256sum myfile.txt out.txt` (the two hashes are equal).

## 13. Run MapReduce
*Dashboard → MapReduce*: pick the dataset, reducers 2 → **Start job**; the stage timeline (SPLIT, MAP, SHUFFLE, REDUCE, FINALIZE) and top words appear. CLI: `./scripts/nexus_cli.sh mapreduce 1 2` then `./scripts/nexus_cli.sh job <job id>`. Compare with `tr -cs 'A-Za-z0-9' '\n' < myfile.txt | tr A-Z a-z | sort | uniq -c | sort -rn | head` (word definition: see `common/wordcount.h`).

## 14. Change the scheduling policy
*Dashboard → Scheduling*: click **FCFS / ROUND_ROBIN / PRIORITY / RESOURCE_AWARE / AI_ASSISTED**; the card shows *✓ active* and the pill in the header changes. CLI: `./scripts/nexus_cli.sh policy ROUND_ROBIN`. Every assignment is logged in the *Scheduling decisions* table with the metrics it used.

## 15. Train or retrain XGBoost
Needs ≥ 30 completed executions (run ~60 tasks first: *Tasks → Count 60*). Then *Dashboard → AI model → Train / retrain XGBoost*, or `./scripts/train_model.sh`.
Expected message (numbers will differ on your data): `xgboost TRAINED: … (MAE 0.4 s, RMSE 0.9 s, baseline MAE 3.5 s)` plus the Random Forest result on the same split. With too little data: `INSUFFICIENT_DATA – need at least 30 completed executions, have N` (no model is created). The *AI model* tab shows active algorithm, library version, held-out MAE/RMSE, baseline, comparison and the training history; `AI_ASSISTED` uses it immediately.

## 16. Run algorithm-comparison benchmarks
*Dashboard → Benchmarks*: keep all five policies ticked, workload MIXED, 24–30 tasks, seed 1, **Repeats 2 or 3** → **Run benchmark**. Results appear per policy as they finish (takes about `policies × repeats × (tasks ÷ workers) × task time`, e.g. ≈ 3 min for 30 tasks × 5 × 2 on 3 Workers). Tables, bar charts and the per-Worker load are drawn from the measured data; the run stays in the list (*latest run* / *historical*) after refresh and restart. Tick *retrain the model first* for a fresh AI model.
CLI: `./scripts/nexus_cli.sh bench all 30 2` then `./scripts/nexus_cli.sh bench-show 1`.
Fairness notes: [SCHEDULING_ALGORITHMS.md](SCHEDULING_ALGORITHMS.md) §7 – the cluster must be idle; small differences between policies are within noise on one machine.

## 17. Inspect logs and troubleshoot
```bash
tail -f logs/master.log            # scheduler decisions, storage, ML, benchmark progress
tail -f logs/worker_W1.log         # affinity status, tasks, heartbeats
```
Dashboard → **Logs** shows the Master log with a tag filter. See [TROUBLESHOOTING.md](TROUBLESHOOTING.md).

## 18. Stop cleanly
```bash
./scripts/stop_all.sh              # Workers first, then the Master
```
Expected: `stopped worker_W1 … stopped master`. (Foreground processes: press **Ctrl+C**.)

## 19. Start and stop the database safely
```bash
# Option A (system PostgreSQL)
sudo service postgresql start            sudo service postgresql stop
# Option B (private cluster)
./setup/local_pg.sh start                ./setup/local_pg.sh stop       ./setup/local_pg.sh status
```
Stop NEXUS first (`./scripts/stop_all.sh`), then the database. Option B uses `pg_ctl -m fast stop` (clean shutdown). Do not delete `.pgdata/` unless you want to erase all data.
After a Windows reboot or `wsl --shutdown` the database is not running: start it again, then `./scripts/start_cluster.sh`.

## 20. Workers on separate physical computers
Use this only when you have a second computer on the same network. **Not verified in this delivery (no second machine was available).**

1. On **every** computer: install dependencies (section 3), unpack the project, `./scripts/install_xgboost.sh` is *not* needed on Worker-only machines but `./scripts/build.sh` is (CMake requires libpq headers; Workers never contact PostgreSQL). If the build fails only because XGBoost is missing, it still builds (Random-Forest-only warning).
2. **Master computer**: `.env` with `NEXUS_MASTER_BIND=0.0.0.0` (default), keep `NEXUS_HTTP_BIND=127.0.0.1`. Start only the Master: `./scripts/start_master.sh`. Find its LAN IP: `hostname -I` (on plain Windows: `ipconfig`).
   **Firewall:** allow inbound **TCP 5050** (Workers). Port 8080 (dashboard) stays local. On **WSL 2** the Linux IP is virtual: either run the Master on a Linux machine, enable WSL *mirrored networking* (`.wslconfig`: `[wsl2]` / `networkingMode=mirrored`) and open the port in Windows Defender Firewall, or add a Windows port proxy: PowerShell (admin) `netsh interface portproxy add v4tov4 listenport=5050 listenaddress=0.0.0.0 connectport=5050 connectaddress=<WSL IP>` plus a firewall rule.
3. **Each Worker computer**: use a **unique** id and the Master's LAN IP – never `127.0.0.1` (there it means *that* computer):
```bash
./scripts/start_remote_worker.sh PC2-W1 192.168.1.20 5050 auto
```
   The script refuses `127.0.0.1`/`localhost` and checks that the port is reachable before starting. Expected on the dashboard: the Worker appears under *Workers* with its own hostname, IP and CPU pinning, and the Hosts panel no longer shows a "shared CPU/RAM" warning.
4. Block data of that Worker is stored on **its own disk** (`worker_data/<ID>/blocks/`), so replicas really are on different machines. Clocks should be roughly in sync (execution times are measured on the Worker).
5. There is no authentication or encryption – use a trusted network only.
