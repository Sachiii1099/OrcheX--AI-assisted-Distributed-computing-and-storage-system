# Demo checklist (evaluation / viva)

**Before the demo** (once): `./scripts/check_prereqs.sh` · database running · `.env` filled · `./scripts/build.sh` · `./scripts/run_tests.sh` is green.

## Automatic demo (≈ 1 minute, prints evidence)
```bash
./scripts/run_demo.sh            # leaves the cluster running; open http://127.0.0.1:8080/
```

## Manual demo script
| # | Do | Say / show |
|---|---|---|
| 1 | `./scripts/start_cluster.sh -w 3 -c 4` | Master + 3 Workers registered; *Workers* tab: CPU/RAM from `/proc`, capacity, heartbeat age. Point at the **shared-host** banner. |
| 2 | *Tasks* → `CPU_BURN`, scale 10, count 12, **Submit** | *Resources* → concurrency table and timeline: tasks overlap, never above a Worker's capacity. |
| 3 | *Scheduling* → click FCFS / Priority / Round Robin / Resource-Aware, submit a burst with mixed priorities | decision log: policy, chosen Worker, score, reason; click a row for every Worker's metrics. |
| 4 | *Storage* → upload a text file, tick *text-aligned*, replication 2 | block map: each block on 2 **different** Workers; `find worker_data -name '*.blk'` shows the real files. |
| 5 | Corrupt a block: `printf '\xff' \| dd of=worker_data/W1/blocks/ds1_b0.blk bs=1 seek=5 conv=notrunc` → **Verify** → **Download** → **Repair** | verify finds `CORRUPT`; download still byte-identical (other replica); repair restores. |
| 6 | Stop one Worker (`kill <pid>`) → **Download** | still works with replication 2; with replication 1 the dataset is reported **not reconstructable** (HTTP 409). |
| 7 | *Storage* → **Run word count** (MapReduce tab) | stage timeline SPLIT/MAP/SHUFFLE/REDUCE/FINALIZE, map tasks on replica holders, top words; compare with `tr … \| sort \| uniq -c`. |
| 8 | Submit a 5 s task, `kill -9` its Worker | task is retried on another Worker (attempt 2); history shows the failed attempt with its reason. |
| 9 | *AI model* → **Train** (≥ 30 finished tasks) → policy `AI_ASSISTED` → submit tasks | MAE/RMSE vs baseline, predicted vs actual durations. Before training: policy shows *fallback*. |
| 10 | `./scripts/stop_all.sh`, start again | datasets, history, policy and model are still there (PostgreSQL + Worker disks). |

## Things to say honestly
* Workers on one laptop share its CPU/RAM; multi-machine operation is supported by the code path but was not tested.
* The model is only as good as the execution history; with too little data it refuses to train.
* No authentication/TLS: localhost only.
