#include "benchmark.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cmath>
#include <set>

#include "../common/json.h"
#include "../common/log.h"
#include "../common/protocol.h"
#include "../common/util.h"
#include "api.h"
#include "sql_util.h"

namespace nexus {

namespace {

// Small deterministic generator (same on every platform / run) so the workload is reproducible.
struct Lcg {
    unsigned long long s;
    explicit Lcg(unsigned seed) : s(seed * 2654435761ull + 88172645463325252ull) {}
    unsigned next() {
        s = s * 6364136223846793005ull + 1442695040888963407ull;
        return static_cast<unsigned>(s >> 33);
    }
};

const char* kMixed[] = {"PRIME_COUNT", "WORD_COUNT", "MATRIX_MULTIPLY", "CHECKSUM", "CPU_BURN"};

std::string joinIds(const std::vector<long long>& ids) {
    std::string s;
    for (size_t i = 0; i < ids.size(); ++i) s += (i ? "," : "") + std::to_string(ids[i]);
    return s;
}

}  // namespace

std::vector<BenchmarkRunner::PlannedTask> BenchmarkRunner::plan(const BenchmarkSpec& spec) {
    std::vector<PlannedTask> out;
    Lcg rng(spec.seed);
    const int span = std::max(1, spec.scaleMax - spec.scaleMin + 1);
    for (int i = 0; i < spec.count; ++i) {
        PlannedTask t;
        t.type = spec.type == "MIXED" ? kMixed[rng.next() % 5] : spec.type;
        t.scale = spec.scaleMin + static_cast<int>(rng.next() % static_cast<unsigned>(span));
        t.priority = spec.priorities == "uniform" ? proto::DEFAULT_PRIORITY : static_cast<int>(rng.next() % 10);
        out.push_back(t);
    }
    return out;
}

bool BenchmarkRunner::parseSpec(const std::string& policies, const std::string& type, int count, int scaleMin,
                                int scaleMax, const std::string& priorities, long long seed, int repeats,
                                long long datasetId, int timeoutSec, BenchmarkSpec& out, std::string& err) {
    BenchmarkSpec s;
    std::string pl = upper(trim(policies));
    if (pl.empty() || pl == "ALL") pl = "FCFS,ROUND_ROBIN,PRIORITY,RESOURCE_AWARE,AI_ASSISTED";
    std::set<std::string> seen;
    for (const std::string& raw : splitOn(pl, ',')) {
        Policy p;
        if (!parsePolicy(raw, p)) { err = "unknown policy '" + trim(raw) + "'"; return false; }
        if (seen.insert(policyName(p)).second) s.policies.push_back(policyName(p));
    }
    s.type = upper(trim(type));
    if (s.type.empty()) s.type = "MIXED";
    if (s.type != "MIXED" && !proto::isUserSubmittable(s.type)) { err = "unknown task type '" + type + "'"; return false; }
    if (count < 1 || count > 500) { err = "count must be 1..500"; return false; }
    const int cap = s.type == "MIXED" ? 20 : TaskManager::maxScale(s.type);
    if (scaleMin < 1 || scaleMax < scaleMin || scaleMax > cap) {
        err = "scale range must satisfy 1 <= scale_min <= scale_max <= " + std::to_string(cap);
        return false;
    }
    s.priorities = trim(priorities);
    for (auto& ch : s.priorities) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    if (s.priorities.empty()) s.priorities = "varied";
    if (s.priorities != "varied" && s.priorities != "uniform") { err = "priorities must be 'varied' or 'uniform'"; return false; }
    if (seed < 0 || seed > 4000000000LL) { err = "seed must be 0..4000000000"; return false; }
    if (repeats < 1 || repeats > 5) { err = "repeats must be 1..5"; return false; }
    if (timeoutSec < 10 || timeoutSec > 3600) { err = "timeout_sec must be 10..3600"; return false; }
    if (proto::isBlockTaskType(s.type) && datasetId <= 0) { err = s.type + " needs a dataset_id (upload one first)"; return false; }
    s.count = count; s.scaleMin = scaleMin; s.scaleMax = scaleMax; s.seed = static_cast<unsigned>(seed);
    s.repeats = repeats; s.datasetId = datasetId; s.timeoutSec = timeoutSec;
    out = s;
    return true;
}

void BenchmarkRunner::recoverOnStart() {
    db_.exec("UPDATE benchmark_runs SET status='FAILED', finished_at=now(), "
             "note='interrupted: the Master stopped while this benchmark was running' WHERE status='RUNNING'");
}

bool BenchmarkRunner::start(const BenchmarkSpec& specIn, long long& benchmarkId, std::string& err) {
    std::lock_guard<std::mutex> lock(startMutex_);
    if (running_.load()) { err = "another benchmark (id " + std::to_string(currentId_.load()) + ") is still running"; return false; }
    if (thread_.joinable()) thread_.join();
    BenchmarkSpec spec = specIn;
    const std::vector<WorkerInfo> ws = workers_.snapshot();
    std::string ids;
    int healthy = 0;
    for (const auto& w : ws) if (workers_.isHealthy(w.id)) { ids += (ids.empty() ? "" : ",") + w.id; ++healthy; }
    if (healthy == 0) { err = "no healthy Worker is connected - start at least one Worker first"; return false; }
    if (tasks_.pendingCount() + tasks_.inFlightCount() > 0) {
        err = "the cluster is busy (tasks pending or running); wait until it is idle so every policy starts from the same state";
        return false;
    }
    if (spec.name.empty()) spec.name = "benchmark " + std::to_string(nowMs());
    if (spec.name.size() > 120) spec.name.resize(120);
    std::string pol;
    for (size_t i = 0; i < spec.policies.size(); ++i) pol += (i ? "," : "") + spec.policies[i];
    json::Obj wl;
    wl.add("type", spec.type).add("count", spec.count).add("scale_min", spec.scaleMin).add("scale_max", spec.scaleMax)
      .add("priorities", spec.priorities).add("seed", static_cast<long long>(spec.seed)).add("repeats", spec.repeats)
      .add("dataset_id", spec.datasetId).add("timeout_sec", spec.timeoutSec);
    Database::Result r = db_.exec(
        "INSERT INTO benchmark_runs (name, workload, policies, n_workers, worker_ids, model_kind, model_run_id) "
        "VALUES ($1,$2,$3,$4::int,$5,$6,$7::int) RETURNING benchmark_id",
        {spec.name, wl.build(), pol, std::to_string(healthy), ids, ml_.modelKind(),
         ml_.modelRunId() > 0 ? std::to_string(ml_.modelRunId()) : kSqlNull});
    if (!r.ok || r.empty()) { err = "database error: " + r.error; return false; }
    benchmarkId = r.i(0, 0);
    currentId_ = benchmarkId;
    cancel_ = false;
    running_ = true;
    thread_ = std::thread(&BenchmarkRunner::runAll, this, spec, benchmarkId);
    return true;
}

void BenchmarkRunner::cancel() { cancel_ = true; }

void BenchmarkRunner::join() {
    std::lock_guard<std::mutex> lock(startMutex_);
    if (thread_.joinable()) thread_.join();
}

void BenchmarkRunner::finish(long long id, const std::string& status, const std::string& note) {
    db_.exec("UPDATE benchmark_runs SET status=$2, note=$3, finished_at=now() WHERE benchmark_id=$1::int",
             {std::to_string(id), status, note});
}

bool BenchmarkRunner::waitIdle(int seconds) {
    for (int i = 0; i < seconds * 10; ++i) {
        if (cancel_) return false;
        if (tasks_.pendingCount() + tasks_.inFlightCount() == 0) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
}

void BenchmarkRunner::runAll(BenchmarkSpec spec, long long id) {
    const std::string original = policyName(scheduler_.policy());
    std::string err, note;
    bool failed = false;
    log("BENCH", "Benchmark " + std::to_string(id) + " '" + spec.name + "' started: " + std::to_string(spec.count) +
                 " tasks x " + std::to_string(spec.policies.size()) + " policies x " + std::to_string(spec.repeats) + " repeat(s)");
    if (spec.retrainBefore) {
        ml::TrainReport tr = ml_.train(spec.modelType);
        note = "retrained before the run: " + tr.status + " (" + tr.modelType + ", " + std::to_string(tr.nSamples) + " samples)";
    }
    for (int rep = 1; rep <= spec.repeats && !failed && !cancel_; ++rep) {
        // rotate the order each repeat so no policy always runs first (cache / warm-up effects)
        std::vector<std::string> order = spec.policies;
        std::rotate(order.begin(), order.begin() + ((rep - 1) % order.size()), order.end());
        for (const std::string& pol : order) {
            if (cancel_) break;
            if (!runOne(spec, id, pol, rep, err)) { failed = true; break; }
        }
    }
    std::string e2;
    applyPolicy(db_, scheduler_, original, e2);        // restore what the operator had selected
    tasks_.pauseDispatch(false);
    if (cancel_) finish(id, "CANCELLED", note + (note.empty() ? "" : "; ") + "cancelled by the user");
    else if (failed) finish(id, "FAILED", err);
    else finish(id, "COMPLETED", note);
    log("BENCH", "Benchmark " + std::to_string(id) + (cancel_ ? " cancelled" : failed ? " FAILED: " + err : " completed") +
                 "; policy restored to " + original);
    running_ = false;
}

bool BenchmarkRunner::runOne(const BenchmarkSpec& spec, long long id, const std::string& policy, int repeat,
                             std::string& err) {
    if (!waitIdle(120)) { err = cancel_ ? "cancelled" : "cluster did not become idle before policy " + policy; return false; }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));      // let the last RESOURCE_UPDATE settle
    std::string e2;
    if (!applyPolicy(db_, scheduler_, policy, e2)) { err = e2; return false; }

    // Block workload: cycle over the blocks of the dataset.
    std::vector<std::string> blocks;
    if (proto::isBlockTaskType(spec.type)) {
        Database::Result b = db_.exec("SELECT block_id FROM blocks WHERE dataset_id=$1::int ORDER BY block_index",
                                      {std::to_string(spec.datasetId)});
        for (size_t i = 0; b.ok && i < b.size(); ++i) blocks.push_back(b.at(i, 0));
        if (blocks.empty()) { err = "dataset " + std::to_string(spec.datasetId) + " has no blocks"; return false; }
    }

    const std::vector<PlannedTask> work = plan(spec);
    std::vector<long long> ids;
    const auto t0 = std::chrono::steady_clock::now();
    tasks_.pauseDispatch(true);                  // the scheduler sees the whole batch at once
    for (size_t i = 0; i < work.size(); ++i) {
        Params extra;
        if (!blocks.empty()) extra["block_id"] = blocks[i % blocks.size()];
        long long tid = 0;
        std::string se;
        if (!tasks_.submit(work[i].type, work[i].scale, work[i].priority, extra, se, &tid)) {
            tasks_.pauseDispatch(false);
            err = "could not submit task " + std::to_string(i) + ": " + se;
            return false;
        }
        ids.push_back(tid);
    }
    tasks_.pauseDispatch(false);
    log("BENCH", "Policy " + policy + " (repeat " + std::to_string(repeat) + "): " + std::to_string(ids.size()) + " tasks submitted");

    const std::string idList = joinIds(ids);
    bool timedOut = false;
    for (;;) {
        Database::Result p = db_.exec(
            "SELECT count(*) FILTER (WHERE status IN ('COMPLETED','FAILED')) FROM tasks WHERE task_id = ANY(string_to_array($1,',')::int[])",
            {idList});
        if (p.ok && !p.empty() && p.i(0, 0) >= static_cast<long long>(ids.size())) break;
        if (cancel_) { err = "cancelled"; tasks_.cancelAll("benchmark cancelled", ids); return false; }
        if (std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count() > spec.timeoutSec) { timedOut = true; break; }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    if (timedOut) tasks_.cancelAll("benchmark timeout", ids);

    // ------------------------------------------------ measure from the database --
    Database::Result m = db_.exec(
        "SELECT count(*), count(*) FILTER (WHERE status='COMPLETED'), count(*) FILTER (WHERE status='FAILED'), "
        "COALESCE(sum(GREATEST(attempts-1,0)),0), "
        "extract(epoch FROM (max(completed_at) - min(created_at))), "
        "avg(extract(epoch FROM (started_at - created_at))) FILTER (WHERE status='COMPLETED'), "
        "avg(extract(epoch FROM (completed_at - started_at))) FILTER (WHERE status='COMPLETED'), "
        "count(*) FILTER (WHERE block_id IS NOT NULL), "
        "count(*) FILTER (WHERE block_id IS NOT NULL AND EXISTS (SELECT 1 FROM block_replicas br "
        "     WHERE br.block_id=tasks.block_id AND br.worker_id=tasks.assigned_worker)) "
        "FROM tasks WHERE task_id = ANY(string_to_array($1,',')::int[])", {idList});
    if (!m.ok || m.empty()) { err = "could not read results: " + m.error; return false; }
    const long long total = m.i(0, 0), done = m.i(0, 1), failedN = m.i(0, 2), retries = m.i(0, 3);
    const double makespan = m.isNull(0, 4) ? 0 : m.d(0, 4);
    const double avgWait = m.isNull(0, 5) ? 0 : m.d(0, 5);
    const double avgRun = m.isNull(0, 6) ? 0 : m.d(0, 6);
    const long long blockTasks = m.i(0, 7), localTasks = m.i(0, 8);

    Database::Result e = db_.exec(
        "SELECT avg(execution_time), avg(worker_cpu_at_assign), avg(worker_ram_at_assign) FROM execution_history "
        "WHERE status='COMPLETED' AND task_id = ANY(string_to_array($1,',')::int[])", {idList});
    const double avgExec = e.ok && !e.empty() && !e.isNull(0, 0) ? e.d(0, 0) : avgRun;
    const double avgCpu = e.ok && !e.empty() && !e.isNull(0, 1) ? e.d(0, 1) : 0;
    const double avgRam = e.ok && !e.empty() && !e.isNull(0, 2) ? e.d(0, 2) : 0;

    Database::Result ai = db_.exec(
        "SELECT (SELECT count(*) FROM ml_predictions WHERE task_id = ANY(string_to_array($1,',')::int[])), "
        "       (SELECT avg(abs(predicted_seconds-actual_seconds)) FROM ml_predictions "
        "         WHERE actual_seconds IS NOT NULL AND task_id = ANY(string_to_array($1,',')::int[])), "
        "       (SELECT sqrt(avg(power(predicted_seconds-actual_seconds,2))) FROM ml_predictions "
        "         WHERE actual_seconds IS NOT NULL AND task_id = ANY(string_to_array($1,',')::int[])), "
        "       (SELECT count(*) FROM scheduling_decisions WHERE fallback AND task_id = ANY(string_to_array($1,',')::int[]))",
        {idList});
    const long long nPred = ai.ok && !ai.empty() ? ai.i(0, 0) : 0;
    const bool haveMae = ai.ok && !ai.empty() && !ai.isNull(0, 1);
    const long long nFallback = ai.ok && !ai.empty() ? ai.i(0, 3) : 0;

    Database::Result w = db_.exec(
        "SELECT t.assigned_worker, count(*), COALESCE(sum(eh.execution_time),0), max(wk.capacity), max(wk.cpu_affinity) "
        "FROM tasks t JOIN workers wk ON wk.worker_id=t.assigned_worker "
        "LEFT JOIN execution_history eh ON eh.task_id=t.task_id AND eh.status='COMPLETED' "
        "WHERE t.task_id = ANY(string_to_array($1,',')::int[]) AND t.status='COMPLETED' "
        "GROUP BY t.assigned_worker ORDER BY t.assigned_worker", {idList});
    std::string wstats = "[";
    for (size_t i = 0; w.ok && i < w.size(); ++i) {
        const double busy = w.d(i, 2);
        const int cap = static_cast<int>(std::max<long long>(1, w.i(i, 3)));
        const double util = makespan > 0 ? busy / (makespan * cap) : 0;
        if (i) wstats += ",";
        wstats += json::Obj().add("worker", w.at(i, 0)).add("tasks", w.i(i, 1)).add("busy_s", busy)
                      .add("capacity", cap).add("utilization", util).add("cpu_affinity", w.at(i, 4)).build();
    }
    wstats += "]";

    const std::string status = timedOut ? "TIMEOUT" : failedN > 0 ? "PARTIAL" : "COMPLETED";
    Database::Result ins = db_.exec(
        "INSERT INTO benchmark_results (benchmark_id, repeat_idx, policy, status, tasks_total, tasks_completed, tasks_failed, "
        " retries, makespan_s, avg_exec_s, avg_wait_s, throughput_tps, avg_cpu, avg_ram, local_tasks, block_tasks, "
        " ai_predictions, ai_fallbacks, pred_mae, pred_rmse, worker_stats, task_ids, started_at, finished_at) "
        "VALUES ($1::int,$2::int,$3,$4,$5::int,$6::int,$7::int,$8::int,$9::double precision,$10::double precision,"
        " $11::double precision,$12::double precision,$13::double precision,$14::double precision,$15::int,$16::int,"
        " $17::int,$18::int,$19::double precision,$20::double precision,$21,$22, (SELECT min(created_at) FROM tasks WHERE task_id = ANY(string_to_array($22,',')::int[])), now())",
        {std::to_string(id), std::to_string(repeat), policy, status, std::to_string(total), std::to_string(done),
         std::to_string(failedN), std::to_string(retries), fmtDouble(makespan, 6), fmtDouble(avgExec, 6),
         fmtDouble(avgWait, 6), makespan > 0 ? fmtDouble(static_cast<double>(done) / makespan, 6) : kSqlNull,
         fmtDouble(avgCpu, 3), fmtDouble(avgRam, 3), std::to_string(localTasks), std::to_string(blockTasks),
         std::to_string(nPred), std::to_string(nFallback),
         haveMae ? fmtDouble(ai.d(0, 1), 6) : kSqlNull, haveMae ? fmtDouble(ai.d(0, 2), 6) : kSqlNull, wstats, idList});
    if (!ins.ok) { err = "could not store results: " + ins.error; return false; }
    log("BENCH", "Policy " + policy + " repeat " + std::to_string(repeat) + ": " + status + ", makespan " + fmtDouble(makespan, 2) +
                 " s, " + std::to_string(done) + "/" + std::to_string(total) + " completed, " + std::to_string(retries) + " retries");
    return true;
}

std::string BenchmarkRunner::listJson() {
    Database::Result r = db_.exec(
        "SELECT b.benchmark_id, b.name, b.status, b.workload, b.policies, b.n_workers, b.model_kind, b.note, "
        NEXUS_TS("b.started_at") " AS started_at, " NEXUS_TS("b.finished_at") " AS finished_at, "
        "(SELECT count(*) FROM benchmark_results x WHERE x.benchmark_id=b.benchmark_id) AS results "
        "FROM benchmark_runs b ORDER BY b.benchmark_id DESC LIMIT 50");
    return json::Obj().add("running", running_.load()).add("current_id", currentId_.load())
                      .raw("benchmarks", r.ok ? r.toJson() : "[]").build();
}

std::string BenchmarkRunner::getJson(long long id) {
    Database::Result b = db_.exec(
        "SELECT benchmark_id, name, status, workload, policies, n_workers, worker_ids, model_kind, model_run_id, note, "
        NEXUS_TS("started_at") " AS started_at, " NEXUS_TS("finished_at") " AS finished_at "
        "FROM benchmark_runs WHERE benchmark_id=$1::int", {std::to_string(id)});
    if (!b.ok || b.empty()) return "";
    Database::Result r = db_.exec(
        "SELECT result_id, repeat_idx, policy, status, tasks_total, tasks_completed, tasks_failed, retries, makespan_s, "
        "avg_exec_s, avg_wait_s, throughput_tps, avg_cpu, avg_ram, local_tasks, block_tasks, ai_predictions, ai_fallbacks, "
        "pred_mae, pred_rmse, worker_stats FROM benchmark_results WHERE benchmark_id=$1::int ORDER BY result_id",
        {std::to_string(id)});
    // worker_stats is stored as JSON text: splice it in as real JSON, not as an escaped string.
    std::string results = "[";
    for (size_t i = 0; r.ok && i < r.size(); ++i) {
        std::string row = r.rowJson(i);
        const std::string ws = r.at(i, 20);
        size_t pos = row.find("\"worker_stats\":");
        if (pos != std::string::npos && !ws.empty()) {
            row = row.substr(0, pos) + "\"worker_stats\":" + ws + "}";
        }
        results += (i ? "," : "") + row;
    }
    results += "]";
    std::string head = b.rowJson(0);
    // inject results + live flag into the object
    head.pop_back();
    return head + ",\"results\":" + results + ",\"running\":" + (running_.load() && currentId_.load() == id ? "true" : "false") + "}";
}

}  // namespace nexus
