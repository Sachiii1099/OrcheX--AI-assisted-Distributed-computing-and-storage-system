# Scheduling algorithms

Code: `master/scheduler.{h,cpp}` (pure decision logic – no sockets, no database, so it is unit-tested in `tests/test_scheduler.cpp`) and `master/task_manager.cpp` (feeds it snapshots, applies its decisions).
Select a policy from **Dashboard → Scheduling** (takes effect on the next dispatch pass, no restart), `POST /api/policy policy=NAME`, or `NEXUS_SCHED_POLICY` at Master start. The choice is stored in `scheduler_config`, so the GUI always shows the policy the backend really uses.

Every policy answers two questions: `order()` – in which order are PENDING tasks considered? and `pick()` – which eligible Worker runs the task?

## 1. Common eligibility rules (all policies)

A Worker is a candidate only if **all** hold; each rejection is recorded as a note in `scheduling_decisions.candidates`:

1. it has a live connection **and** a heartbeat younger than 3 × `NEXUS_HEARTBEAT_SEC` (`isHealthy`);
2. it has a free slot: tasks in flight on it `<` its capacity;
3. for block tasks (`BLOCK_*`): it holds a HEALTHY replica of the block (**data locality is a hard constraint** for block tasks);
4. a Worker that already failed this task is skipped **if another candidate exists** (retry avoidance).

Dependencies: MapReduce REDUCE tasks are only created after all MAP tasks are done (`master/mapreduce.cpp`), so the scheduler never sees a task whose inputs are missing.
All policies are **non-preemptive**: a running task is never interrupted.

## 2. FCFS – First Come, First Served

* **Order:** ascending task id (ids only grow, so this is submission order); priority is ignored.
* **Worker:** the first eligible Worker by id.
```
for task in sorted(pending, key = id):
    w = first(eligible workers sorted by id)
    if w: assign(task, w)
```
Limitation: ignores load and priority; a long task at the head delays everyone.

## 3. Round Robin

* **Order:** submission order.
* **Worker:** the next eligible Worker whose id is greater than the one used last (wraps around); the cursor moves only when a task was really placed, so full or dead Workers are skipped without losing fairness.
```
for task in sorted(pending, key = id):
    w = first(e in eligible sorted by id where e.id > last) or eligible[0]
    last = w.id; assign(task, w)
```
Limitation: blind to task size and Worker speed.

## 4. Priority

* **Priority scale:** integer 0–9, **higher = more urgent** (default 5).
* **Order:** priority descending, ties broken by submission order (stable).
* **Worker:** the least-occupied eligible Worker (`running / capacity`, ties → lower id).
```
for task in sorted(pending, key = (-priority, id)):
    w = argmin(eligible, running/capacity)
```
Limitation: no aging – a continuous stream of high-priority tasks can starve low-priority ones.

## 5. Resource-Aware

* **Order:** submission order.
* **Worker:** the lowest score; every term is normalised to 0..1 and metrics must be **fresh and valid** (finite, 0–100, younger than the heartbeat timeout – otherwise that Worker is rejected with a note):
```
score(w) = 0.35 * cpu/100 + 0.20 * ram/100 + 0.05 * disk/100
         + 0.30 * min(1, running/capacity)
         + 0.10 * min(1, heartbeat_age/timeout)
         - 0.15 if w holds the task's data block
```
The weights are a documented heuristic (CPU and queue length matter most for execution time; heartbeat age is a mild reliability penalty; locality is a bonus). They are not learned.
Limitation: CPU/RAM are host-wide figures, so Workers on the same computer see the same load.

## 6. AI-Assisted (XGBoost)

* **Order:** priority descending, then submission order.
* **Worker:** the Worker with the **shortest predicted execution time** for this task.
```
usable = eligible workers with fresh, valid metrics
if model exists and it predicts for every usable worker:
    w = argmin(predicted_seconds), ties -> lower resource score -> lower id   # reason: "AI_ASSISTED: shortest predicted duration"
else:
    w = RESOURCE_AWARE choice, decision marked fallback=true                  # reason names why (no model / cannot predict / ...)
```
The prediction comes from the trained XGBoost model (`MlManager::predictor`): features are the task's type/scale/input size and the **candidate Worker's** current CPU, RAM, running tasks, capacity, cores (see [MACHINE_LEARNING.md](MACHINE_LEARNING.md)). The prediction is stored in `ml_predictions` and the actual duration is filled in when the task finishes.
If no model has been trained (or it was not better than the baseline) the policy **says so** and uses Resource-Aware – the dashboard shows "fell back" and the benchmark records `ai_fallbacks`.

## 7. Benchmarks – comparing the policies fairly

`POST /api/benchmarks` / **Dashboard → Benchmarks** (`master/benchmark.cpp`):

* The workload is generated from a **seed** (`plan()`): the *i*-th task has the same type, scale and priority for every policy.
* Before each policy run the cluster must be idle; the policy is activated; the whole batch is submitted while dispatching is paused, then released – so the scheduler sees a real queue and the ordering rule matters.
* With `repeats > 1` the policy order is **rotated** each repeat, so no policy is always first; the GUI shows the mean over repeats.
* The original policy is restored afterwards. Results (`benchmark_runs`, `benchmark_results`) survive page refreshes and Master restarts; older runs are labelled *historical*.
* Metrics (all measured from `tasks`, `execution_history`, `ml_predictions`, `block_replicas`): **makespan** (last completion − first submission), **average execution time** (Worker-measured), **average waiting time** (submission → start), **throughput** (completed ÷ makespan), failed tasks, retries (Σ attempts−1), **Worker utilisation** (busy time ÷ makespan × capacity), CPU/RAM at assignment, data-local tasks, and for AI_ASSISTED the number of predictions, fallbacks and **MAE/RMSE of predicted vs actual**.

**Read the numbers with care.** With 3 Worker processes on one machine, short tasks and a homogeneous workload, the policies place tasks almost identically and the differences are a few percent – inside run-to-run noise (OS scheduling, caches, other processes, WSL). A policy only wins clearly when the workload has structure it can exploit: very different task sizes (AI/Resource-Aware), priorities with a backlog (Priority), or unequal Workers. Use `repeats ≥ 3` and compare means; do not read a ★ on a single run as proof.
The measured example in [FINAL_TEST_REPORT.md](FINAL_TEST_REPORT.md) shows exactly this.
