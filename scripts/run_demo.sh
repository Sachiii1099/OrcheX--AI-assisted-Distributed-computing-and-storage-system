#!/usr/bin/env bash
# End-to-end demo of every NEXUS feature against a REAL running cluster (no simulation).
#   ./scripts/run_demo.sh            start a 3-Worker cluster, run the demo, leave the cluster running
#   ./scripts/run_demo.sh --stop     ... and shut everything down at the end
# Needs: ./scripts/build.sh done, database set up (./setup/setup.sh), .env with NEXUS_DB_PASSWORD.
# All 3 Workers run on THIS computer and share its CPU cores and RAM.
. "$(dirname "$0")/common.sh"
need_binaries; need_db_env
CLI="$ROOT/scripts/nexus_cli.sh"
step() { echo; echo "================================================================"; echo "  $*"; echo "================================================================"; }
pyj()  { python3 -c "import sys,json; d=json.load(sys.stdin); $1"; }

"$ROOT/scripts/start_cluster.sh" -w 3 -c 3 -a none      # no pinning: the demo shows 3 tasks running at once on one Worker
BASE_TASKS=$(curl -fsS "$API/api/overview" | pyj "print(d['tasks']['total'])")

step "1. Cluster: Workers, resources, capacity"
curl -fsS "$API/api/workers" | pyj "
for w in d: print('  %-3s %-8s cpu %5.1f%%  ram %5.1f%%  slots %d  heartbeat %.1fs ago  host %s (%d cores)' % (w['worker_id'], 'healthy' if w['healthy'] else 'DOWN', w['cpu'], w['ram'], w['capacity'], w['heartbeat_age_s'], w['hostname'], w['cpu_cores']))"
curl -fsS "$API/api/overview" | pyj "print('  ' + (d['shared_host_note'] or 'Workers run on different hosts'))"

step "2. Multi-core: 3 tasks x 1.0 s of CPU on ONE Worker (capacity 3) run at the same time"
"$CLI" policy FCFS >/dev/null
T0=$(date +%s.%N)
for i in 1 2 3; do "$CLI" submit CPU_BURN 10 5 > /dev/null; done
wait_for "tasks" 30 bash -c "[ \$(curl -s '$API/api/overview' | python3 -c 'import sys,json; o=json.load(sys.stdin)[\"tasks\"]; print(o[\"pending\"]+o[\"assigned\"]+o[\"running\"])') = 0 ]"
T1=$(date +%s.%N)
python3 -c "print('  wall-clock for 3 x 1.0 s tasks: %.2f s (serial execution would need >= 3.0 s)' % ($T1 - $T0))"
curl -fsS "$API/api/history?limit=3" | pyj "
for h in reversed(d): print('  TASK-%03d on %s  started %s  finished %s  (%d tasks running when it started)' % (h['task_id'], h['worker_id'], h['started_at'][11:23], h['finished_at'][11:23], h['concurrent_at_start']))"
curl -fsS "$API/api/concurrency" | pyj "print('  measured: cluster max concurrent = %d; per Worker: %s' % (d['cluster_max_concurrent'], {w['worker_id']: w['max_concurrent'] for w in d['per_worker']}))"

step "3. Scheduling policies: same 6 tasks (priorities 1..6), different decisions"
for pol in FCFS ROUND_ROBIN PRIORITY RESOURCE_AWARE; do
  "$CLI" policy "$pol" > /dev/null
  ids=""
  for p in 1 2 3 4 5 6; do ids="$ids $(curl -sS -X POST "$API/api/tasks" -d "type=CPU_BURN&scale=2&priority=$p" | python3 -c 'import sys,json; print(json.load(sys.stdin)["task_id"])')"; done
  sleep 2
  echo "  $pol -> workers chosen per task: $(for i in $ids; do curl -fsS "$API/api/tasks/$i" | python3 -c 'import sys,json; d=json.load(sys.stdin); print("%s(p%d)" % (d["assigned_worker"], d["priority"]), end=" ")'; done)"
done
"$CLI" policy RESOURCE_AWARE > /dev/null

step "4. Distributed storage: upload, replicate, verify, reconstruct"
python3 - <<'EOF'
import random
r = random.Random(42)
vocab = "nexus master worker block replica scheduler thread socket postgres checksum cluster task map reduce shuffle priority heartbeat".split()
with open("/tmp/nexus_demo_corpus.txt", "w") as f:
    for i in range(80000): f.write(r.choice(vocab) + r.choice([" ", " ", " ", "\n"]))
EOF
echo "  generated /tmp/nexus_demo_corpus.txt ($(wc -c < /tmp/nexus_demo_corpus.txt) bytes)"
UP=$("$CLI" upload /tmp/nexus_demo_corpus.txt 2 100000 1)
DS=$(echo "$UP" | python3 -c 'import sys,json; print(json.load(sys.stdin)["dataset_id"])')
curl -fsS "$API/api/datasets/$DS" | pyj "
print('  dataset %d: %d blocks of <= %d bytes, replication %d, health %s' % (d['dataset_id'], d['block_count'], d['block_size'], d['replication'], d['health']))
for b in d['blocks']: print('    %s  %7d bytes  sha256 %s..  replicas on: %s' % (b['block_id'], b['size'], b['checksum'][:10], ', '.join(r['worker'] for r in b['replicas'])))"
echo "  block files on the Workers' disks:"; find "$ROOT/worker_data" -name "ds${DS}_b*.blk" 2>/dev/null | sort | sed 's/^/    /'
"$CLI" verify "$DS" | pyj "print('  verify: %d replicas checked, %d healthy, %d corrupt, reconstructable=%s' % (d['replicas_checked'], d['healthy'], d['corrupt'], d['reconstructable_now']))" 2>/dev/null || "$CLI" verify "$DS"
"$CLI" download "$DS" /tmp/nexus_demo_corpus.out
cmp /tmp/nexus_demo_corpus.txt /tmp/nexus_demo_corpus.out && echo "  reconstructed file is IDENTICAL to the original"

step "5. MapReduce word count over the distributed blocks"
JOB=$("$CLI" mapreduce "$DS" 3 5 | python3 -c 'import sys,json; print(json.load(sys.stdin)["job_id"])')
wait_for "MapReduce job" 60 bash -c "[ \$(curl -s '$API/api/jobs/$JOB' | python3 -c 'import sys,json; print(json.load(sys.stdin)[\"status\"])') = COMPLETED ]"
curl -fsS "$API/api/jobs/$JOB" | pyj "
print('  job %d %s: %s' % (d['job_id'], d['status'], d['result']))
for s in d['stages']: print('    stage %-8s %-9s %.3f s  (%d task(s))' % (s['stage_name'], s['status'], s['duration_s'], s['task_count']))
for t in d['tasks']: print('    %-6s %-7s on %s  %s' % ('TASK-%03d' % t['task_id'], t['role'], t['assigned_worker'], t['block_id'] or 'partition %s' % t['partition_idx']))"
echo "  independent check with tr|sort|uniq:"
tr 'A-Z' 'a-z' < /tmp/nexus_demo_corpus.txt | tr -cs 'a-z0-9' '\n' | grep -v '^$' | sort | uniq -c | sort -rn | head -3 | sed 's/^/    /'
curl -fsS "$API/api/jobs/$JOB/results?limit=3" | pyj "print('  NEXUS result top-3: ' + ', '.join('%s=%d' % (r['word'], r['count']) for r in d))"

step "6. AI task-time prediction: build real history, train, schedule with the model"
echo "  running 60 real tasks of varying size to create execution history..."
for i in $(seq 1 60); do curl -sS -X POST "$API/api/tasks" -d "type=CPU_BURN&scale=$(( (i % 10) + 1 ))&priority=5" > /dev/null; done
wait_for "history" 120 bash -c "[ \$(curl -s '$API/api/overview' | python3 -c 'import sys,json; o=json.load(sys.stdin)[\"tasks\"]; print(o[\"pending\"]+o[\"assigned\"]+o[\"running\"])') = 0 ]"
"$CLI" train | pyj "print('  training: %s  samples=%d (train %d / test %d)  MAE=%.4fs  RMSE=%.4fs  baseline MAE=%.4fs\n  %s' % (d['status'], d['n_samples'], d['n_train'], d['n_test'], d['mae'], d['rmse'], d['baseline_mae'], d['message']))"
"$CLI" policy AI_ASSISTED > /dev/null
ids=""; for s in 1 5 9; do ids="$ids $(curl -sS -X POST "$API/api/tasks" -d "type=CPU_BURN&scale=$s&priority=5" | python3 -c 'import sys,json; print(json.load(sys.stdin)["task_id"])')"; done
sleep 3
curl -fsS "$API/api/decisions?limit=3" | pyj "
for x in reversed(d): print('  TASK-%03d scale-> predicted %s s, actual %s s on %s  [%s%s]' % (x['task_id'], x['predicted_seconds'], x['actual_seconds'], x['worker_id'], x['effective_policy'], ' (fallback)' if x['fallback'] else ''))"
"$CLI" policy RESOURCE_AWARE > /dev/null

step "7. Persistence: everything above is in PostgreSQL"
curl -fsS "$API/api/overview" | pyj "print('  tasks %d, jobs %d, datasets %d, blocks %d, replicas %d' % (d['tasks']['total'], d['jobs']['total'], d['storage']['datasets'], d['storage']['blocks'], d['storage']['replicas']))"
echo
echo "Demo finished. Open the dashboard: $API/"
if [ "${1:-}" = "--stop" ]; then "$ROOT/scripts/stop_all.sh"; else echo "(cluster left running - stop it with ./scripts/stop_all.sh)"; fi
