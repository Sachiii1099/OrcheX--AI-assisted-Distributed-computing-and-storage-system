#include "task_manager.h"

#include <algorithm>
#include <chrono>
#include <cstdio>

#include "../common/log.h"
#include "../common/protocol.h"

namespace nexus {

using Clock = std::chrono::steady_clock;

std::string TaskManager::label(long long taskId) {
    char buf[32];
    std::snprintf(buf, sizeof buf, "TASK-%03lld", taskId);
    return buf;
}

// ------------------------------------------------------------ life cycle ----

void TaskManager::start() {
    if (running_.exchange(true)) return;
    dispatcher_ = std::thread(&TaskManager::dispatchLoop, this);
}

void TaskManager::stop() {
    if (!running_.exchange(false)) return;
    wake();
    if (dispatcher_.joinable()) dispatcher_.join();
}

void TaskManager::wake() {
    {
        std::lock_guard<std::mutex> lock(wakeMutex_);
        wakeFlag_ = true;
    }
    wakeCv_.notify_all();
}

void TaskManager::dispatchLoop() {
    while (running_) {
        {
            std::unique_lock<std::mutex> lock(wakeMutex_);
            wakeCv_.wait_for(lock, std::chrono::milliseconds(300), [this] { return wakeFlag_ || !running_; });
            wakeFlag_ = false;
        }
        if (!running_) break;
        try { dispatchOnce(); }
        catch (const std::exception& e) { log("SCHEDULER", std::string("dispatch error: ") + e.what()); }
    }
}

size_t TaskManager::pendingCount() { std::lock_guard<std::mutex> l(mutex_); return pending_.size(); }
size_t TaskManager::inFlightCount() { std::lock_guard<std::mutex> l(mutex_); return inFlight_.size(); }

void TaskManager::recoverOnStart() {
    // Work that was in flight when the Master stopped has no live Worker any more.
    Database::Result a = db_.exec(
        "UPDATE tasks SET status='PENDING', last_error='interrupted: master restarted while task was in flight' "
        "WHERE status IN ('ASSIGNED','RUNNING') AND attempts < max_attempts");
    Database::Result b = db_.exec(
        "UPDATE tasks SET status='FAILED', completed_at=now(), "
        "last_error='interrupted by master restart; attempts exhausted' WHERE status IN ('ASSIGNED','RUNNING')");
    db_.exec("UPDATE jobs j SET status='FAILED', completed_at=now() WHERE j.status IN ('PENDING','RUNNING') AND "
             "EXISTS (SELECT 1 FROM tasks t WHERE t.job_id=j.job_id AND t.role='STANDALONE' AND t.status='FAILED')");
    if (a.ok && a.affected) log("MASTER", "Recovery: " + std::to_string(a.affected) + " interrupted task(s) re-queued");
    if (b.ok && b.affected) log("MASTER", "Recovery: " + std::to_string(b.affected) + " interrupted task(s) failed (attempts exhausted)");

    Database::Result r = db_.exec(
        "SELECT task_id, job_id, task_type, params, priority, scale, attempts, max_attempts, role, "
        "COALESCE(partition_idx,-1), COALESCE(block_id,''), input_bytes FROM tasks WHERE status='PENDING' ORDER BY task_id");
    std::lock_guard<std::mutex> lock(mutex_);
    pending_.clear();
    for (size_t i = 0; r.ok && i < r.size(); ++i) {
        Pending p;
        p.id = r.i(i, 0);
        p.spec.jobId = r.i(i, 1);
        p.spec.type = r.at(i, 2);
        p.spec.params = decodeParams(r.at(i, 3));
        p.spec.priority = static_cast<int>(r.i(i, 4));
        p.spec.scale = static_cast<int>(r.i(i, 5));
        p.attempts = static_cast<int>(r.i(i, 6));
        p.spec.maxAttempts = static_cast<int>(r.i(i, 7));
        p.spec.role = r.at(i, 8);
        p.spec.partition = static_cast<int>(r.i(i, 9));
        p.spec.blockId = r.at(i, 10);
        p.spec.inputBytes = r.i(i, 11);
        pending_.push_back(std::move(p));
    }
    if (!pending_.empty()) log("MASTER", "Recovery: " + std::to_string(pending_.size()) + " PENDING task(s) loaded from PostgreSQL");
}

// ------------------------------------------------------------ submission ----

bool TaskManager::createJob(const std::string& jobType, int priority, const std::string& params,
                            long long datasetId, long long& jobId, std::string& err) {
    Database::Result r = db_.exec(
        "INSERT INTO jobs (job_type, priority, params, dataset_id, status) VALUES ($1, $2::int, $3, $4, 'RUNNING') "
        "RETURNING job_id",
        {jobType, std::to_string(priority), params, datasetId > 0 ? std::to_string(datasetId) : kSqlNull});
    if (!r.ok || r.empty()) { err = r.error; return false; }
    jobId = r.i(0, 0);
    return true;
}

bool TaskManager::addTask(const TaskSpec& s, long long& taskId, std::string& err) {
    Database::Result r = db_.exec(
        "INSERT INTO tasks (job_id, task_type, params, priority, scale, max_attempts, role, partition_idx, block_id, input_bytes) "
        "VALUES ($1::int, $2, $3, $4::int, $5::int, $6::int, $7, $8, $9, $10::bigint) RETURNING task_id",
        {std::to_string(s.jobId), s.type, encodeParams(s.params), std::to_string(s.priority),
         std::to_string(s.scale), std::to_string(s.maxAttempts), s.role,
         s.partition >= 0 ? std::to_string(s.partition) : kSqlNull,
         s.blockId.empty() ? kSqlNull : s.blockId, std::to_string(s.inputBytes)});
    if (!r.ok || r.empty()) { err = "database error: " + r.error; return false; }
    taskId = r.i(0, 0);
    Pending p;
    p.id = taskId;
    p.spec = s;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pending_.push_back(std::move(p));
    }
    wake();
    return true;
}

bool TaskManager::submit(const std::string& rawType, int scale, int priority, Params extra,
                         std::string& err, long long* taskIdOut) {
    const std::string type = upper(trim(rawType));
    if (!proto::isUserSubmittable(type)) {
        err = "unknown task type '" + rawType + "' (WORD_COUNT, PRIME_COUNT, MATRIX_MULTIPLY, CHECKSUM, "
              "CPU_BURN, BLOCK_WORD_COUNT, BLOCK_CHECKSUM)";
        return false;
    }
    if (scale < 1 || scale > maxScale(type)) {
        err = "scale must be 1.." + std::to_string(maxScale(type)) + " for " + type;
        return false;
    }
    if (priority < proto::MIN_PRIORITY || priority > proto::MAX_PRIORITY) {
        err = "priority must be " + std::to_string(proto::MIN_PRIORITY) + ".." + std::to_string(proto::MAX_PRIORITY) +
              " (higher = more urgent)";
        return false;
    }

    TaskSpec s;
    s.type = type;
    s.scale = scale;
    s.priority = priority;
    s.params = std::move(extra);
    s.params["scale"] = std::to_string(scale);
    if (proto::isBlockTaskType(type)) {
        const std::string blockId = paramStr(s.params, "block_id");
        if (!isSafeId(blockId)) { err = type + " needs a valid block_id parameter (e.g. ds1_b0)"; return false; }
        Database::Result b = db_.exec("SELECT checksum, size_bytes FROM blocks WHERE block_id=$1", {blockId});
        if (!b.ok || b.empty()) { err = "unknown block '" + blockId + "'"; return false; }
        s.params["checksum"] = b.at(0, 0);
        s.blockId = blockId;
        s.inputBytes = b.i(0, 1);
    }

    // Job + task are created together (one statement => atomic).
    Database::Result r = db_.exec(
        "WITH j AS (INSERT INTO jobs (job_type, priority, params) VALUES ($1, $2::int, $3) RETURNING job_id) "
        "INSERT INTO tasks (job_id, task_type, params, priority, scale, max_attempts, role, block_id, input_bytes) "
        "SELECT job_id, $1, $3, $2::int, $4::int, $5::int, 'STANDALONE', $6, $7::bigint FROM j "
        "RETURNING task_id, job_id",
        {type, std::to_string(priority), encodeParams(s.params), std::to_string(scale),
         std::to_string(proto::DEFAULT_MAX_ATTEMPTS), s.blockId.empty() ? kSqlNull : s.blockId,
         std::to_string(s.inputBytes)});
    if (!r.ok || r.empty()) { err = "database error: " + r.error; return false; }

    Pending p;
    p.id = r.i(0, 0);
    s.jobId = r.i(0, 1);
    p.spec = s;
    if (taskIdOut) *taskIdOut = p.id;
    log("MASTER", "Task submitted: " + label(p.id) + " (job " + std::to_string(s.jobId) + ", " + type + ", scale " +
                  std::to_string(scale) + ", priority " + std::to_string(priority) + ")");
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pending_.push_back(std::move(p));
    }
    wake();
    return true;
}

void TaskManager::cancelJob(long long jobId, const std::string& reason) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pending_.erase(std::remove_if(pending_.begin(), pending_.end(),
                                      [&](const Pending& p) { return p.spec.jobId == jobId; }), pending_.end());
    }
    db_.exec("UPDATE tasks SET status='FAILED', last_error=$2, completed_at=now() WHERE job_id=$1::int AND status='PENDING'",
             {std::to_string(jobId), reason});
}

// -------------------------------------------------------------- dispatch ----

void TaskManager::dispatchOnce() {
    if (paused_) return;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (pending_.empty()) return;
    }
    const auto now = Clock::now();
    std::map<std::string, int> runningBy;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& kv : inFlight_) ++runningBy[kv.second.workerId];
    }

    std::vector<SchedWorker> sw;
    bool anyFree = false;
    for (const WorkerInfo& wi : workers_.snapshot()) {
        SchedWorker w;
        w.id = wi.id;
        w.connected = static_cast<bool>(wi.conn);
        w.hbAgeSec = std::chrono::duration<double>(now - wi.lastHeartbeat).count();
        w.metricsAgeSec = wi.metricsEver ? std::chrono::duration<double>(now - wi.lastMetrics).count() : -1;
        w.cpu = wi.cpu; w.ram = wi.ram; w.disk = wi.disk;
        w.running = runningBy[wi.id];
        w.capacity = wi.capacity;
        w.cores = wi.cpuCores;
        if (scheduler_.isHealthy(w) && w.running < w.capacity) anyFree = true;
        sw.push_back(w);
    }
    if (!anyFree) return;                 // nothing could run right now: skip the (DB-backed) work below

    std::vector<Pending> pend;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pend = pending_;
    }
    std::vector<SchedTask> st;
    st.reserve(pend.size());
    for (const Pending& p : pend) {
        SchedTask t;
        t.id = p.id;
        t.priority = p.spec.priority;
        t.type = p.spec.type;
        t.scale = p.spec.scale;
        t.inputBytes = p.spec.inputBytes;
        t.excluded = p.excluded;
        if (!p.spec.blockId.empty()) {
            t.localityRequired = true;
            t.localWorkers = storage_.replicaHolders(p.spec.blockId);
        }
        st.push_back(std::move(t));
    }

    std::shared_ptr<Predictor> predictor = ml_.predictor();
    for (size_t idx : scheduler_.order(st)) {
        Decision d = scheduler_.pick(st[idx], sw, predictor.get());
        if (!d.found) continue;
        if (!assign(pend[idx], d, label(pend[idx].id))) continue;

        // Report: what the scheduler saw and chose (also kept in scheduling_decisions).
        std::string rep = "Task: " + label(pend[idx].id) + " (" + pend[idx].spec.type + ", priority " +
                          std::to_string(pend[idx].spec.priority) + ")   Policy: " + d.requestedPolicy +
                          (d.fallback ? " -> " + d.effectivePolicy + " (fallback)" : "") + "\n";
        for (SchedWorker& w : sw) {
            char line[200];
            std::string note;
            for (const auto& c : d.candidates) if (c.worker == w.id) note = c.note;
            std::snprintf(line, sizeof line, "  %-6s CPU %3.0f%%  RAM %3.0f%%  tasks %d/%d%s%s\n", w.id.c_str(), w.cpu, w.ram,
                          w.running, w.capacity, w.id == d.workerId ? "   <-- SELECTED" : "",
                          (!note.empty() && w.id != d.workerId) ? ("   (" + note + ")").c_str() : "");
            rep += line;
        }
        rep += "  " + d.reason;
        log("SCHEDULER", rep);
        for (SchedWorker& w : sw) if (w.id == d.workerId) ++w.running;
    }
}

bool TaskManager::assign(const Pending& p, const Decision& d, const std::string& lbl) {
    const bool hasPred = d.predictedSeconds >= 0;
    const int runId = ml_.modelRunId();
    Database::Result r = db_.exec(
        "WITH c AS (UPDATE tasks SET status='ASSIGNED', assigned_worker=$2, attempts=attempts+1, assigned_at=now(), "
        "                            started_at=NULL, completed_at=NULL, predicted_seconds=$3::double precision "
        "           WHERE task_id=$1::int AND status='PENDING' RETURNING task_id, attempts), "
        "d AS (INSERT INTO scheduling_decisions (task_id, attempt, policy, effective_policy, worker_id, score, "
        "                                        predicted_seconds, fallback, reason, candidates) "
        "      SELECT task_id, attempts, $4, $5, $2, $6::double precision, $3::double precision, $7::boolean, $8, $9 "
        "      FROM c RETURNING decision_id), "
        "pr AS (INSERT INTO ml_predictions (task_id, attempt, worker_id, run_id, predicted_seconds) "
        "       SELECT task_id, attempts, $2, $10::int, $3::double precision FROM c "
        "       WHERE $3::double precision IS NOT NULL RETURNING prediction_id) "
        "SELECT attempts FROM c",
        {std::to_string(p.id), d.workerId, hasPred ? fmtDouble(d.predictedSeconds, 6) : kSqlNull,
         d.requestedPolicy, d.effectivePolicy, fmtDouble(d.score, 6), d.fallback ? "true" : "false", d.reason,
         d.candidatesJson, runId > 0 ? std::to_string(runId) : kSqlNull});

    if (!r.ok) { log("SCHEDULER", "could not record assignment of " + lbl + ": " + r.error); return false; }
    if (r.empty()) {
        // Another path already moved this task out of PENDING: never assign it twice.
        std::lock_guard<std::mutex> lock(mutex_);
        pending_.erase(std::remove_if(pending_.begin(), pending_.end(),
                                      [&](const Pending& x) { return x.id == p.id; }), pending_.end());
        return false;
    }
    const int attempt = static_cast<int>(r.i(0, 0));

    InFlight f;
    f.task = p;
    f.task.attempts = attempt;
    f.workerId = d.workerId;
    f.policy = d.effectivePolicy;
    f.attempt = attempt;
    f.cpu = d.cpu; f.ram = d.ram; f.predicted = hasPred ? d.predictedSeconds : -1;
    f.runningAtAssign = d.running; f.capacity = d.capacity;
    {
        // Register BEFORE sending so a very fast result cannot race us.
        std::lock_guard<std::mutex> lock(mutex_);
        pending_.erase(std::remove_if(pending_.begin(), pending_.end(),
                                      [&](const Pending& x) { return x.id == p.id; }), pending_.end());
        inFlight_[p.id] = f;
    }

    Message m(proto::TASK_ASSIGN);
    m.setInt("task_id", p.id).setInt("attempt", attempt).set("task_label", lbl).set("task_type", p.spec.type)
     .setInt("scale", p.spec.scale).set("params", encodeParams(p.spec.params));
    auto conn = workers_.connectionOf(d.workerId);
    if (!conn || !conn->send(m)) {
        log("MASTER", "could not reach " + d.workerId + " - " + lbl + " goes back to the queue");
        {
            std::lock_guard<std::mutex> lock(mutex_);
            inFlight_.erase(p.id);
            Pending back = p;
            back.excluded.insert(d.workerId);
            pending_.push_back(std::move(back));
        }
        std::string err;
        db_.transact({{"UPDATE tasks SET status='PENDING', attempts=attempts-1, last_error='send to worker failed' "
                       "WHERE task_id=$1::int AND status='ASSIGNED'", {std::to_string(p.id)}},
                      {"DELETE FROM ml_predictions WHERE task_id=$1::int AND attempt=$2::int",
                       {std::to_string(p.id), std::to_string(attempt)}},
                      {"DELETE FROM scheduling_decisions WHERE task_id=$1::int AND attempt=$2::int",
                       {std::to_string(p.id), std::to_string(attempt)}}}, err);
        return false;
    }
    log("MASTER", lbl + " assigned to " + d.workerId + " (attempt " + std::to_string(attempt) + ")");
    return true;
}

// ---------------------------------------------------------- Worker events ----

bool TaskManager::takeInFlight(long long taskId, const std::string& workerId, long long attempt, InFlight& out) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = inFlight_.find(taskId);
    if (it == inFlight_.end() || it->second.workerId != workerId || it->second.attempt != attempt) return false;
    out = it->second;
    inFlight_.erase(it);
    return true;
}

void TaskManager::onTaskStarted(const std::string& workerId, const Message& m) {
    const long long id = m.getInt("task_id");
    long long jobId = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = inFlight_.find(id);
        if (it == inFlight_.end() || it->second.workerId != workerId || it->second.attempt != m.getInt("attempt", 1)) return;
        jobId = it->second.task.spec.jobId;
    }
    db_.exec("UPDATE tasks SET status='RUNNING', started_at=now() WHERE task_id=$1::int AND status='ASSIGNED'",
             {std::to_string(id)});
    db_.exec("UPDATE jobs SET status='RUNNING' WHERE job_id=$1::int AND status='PENDING'", {std::to_string(jobId)});
}

Database::Stmt TaskManager::historyStmt(const InFlight& f, const char* status, double seconds, const std::string& error,
                                        long long startMs, long long endMs, int concurrent) const {
    return {"INSERT INTO execution_history (task_id, worker_id, task_type, execution_time, status, "
            "  worker_cpu_at_assign, worker_ram_at_assign, scheduler_policy, attempt, scale, input_bytes, "
            "  running_at_assign, worker_capacity, started_at, finished_at, concurrent_at_start, predicted_seconds, error) "
            "VALUES ($1::int, $2, $3, $4::double precision, $5, $6::double precision, $7::double precision, $8, "
            "  $9::int, $10::int, $11::bigint, $12::int, $13::int, to_timestamp($14::double precision / 1000.0), "
            "  to_timestamp($15::double precision / 1000.0), $16::int, $17::double precision, $18)",
            {std::to_string(f.task.id), f.workerId, f.task.spec.type, fmtDouble(seconds, 6), status,
             fmtDouble(f.cpu, 4), fmtDouble(f.ram, 4), f.policy, std::to_string(f.attempt),
             std::to_string(f.task.spec.scale), std::to_string(f.task.spec.inputBytes),
             std::to_string(f.runningAtAssign), std::to_string(f.capacity),
             startMs > 0 ? std::to_string(startMs) : kSqlNull, endMs > 0 ? std::to_string(endMs) : kSqlNull,
             concurrent > 0 ? std::to_string(concurrent) : kSqlNull,
             f.predicted >= 0 ? fmtDouble(f.predicted, 6) : kSqlNull, error.empty() ? kSqlNull : error}};
}

void TaskManager::emit(const InFlight& f, bool success, const std::string& result, const std::string& data,
                       const std::string& error) {
    if (f.task.spec.role == "STANDALONE" || !callback_) return;
    TaskEvent ev;
    ev.taskId = f.task.id; ev.jobId = f.task.spec.jobId; ev.role = f.task.spec.role;
    ev.partition = f.task.spec.partition; ev.workerId = f.workerId;
    ev.success = success; ev.result = result; ev.data = data; ev.error = error;
    callback_(ev);
}

void TaskManager::onTaskResult(const std::string& workerId, const Message& m) {
    const long long id = m.getInt("task_id");
    InFlight f;
    if (!takeInFlight(id, workerId, m.getInt("attempt", 1), f)) {
        log("MASTER", "Ignoring stale/unknown result for " + label(id) + " from " + workerId);
        return;
    }
    const double sec = m.getDouble("exec_time");
    const std::string result = m.get("result");
    std::vector<Database::Stmt> tx;
    tx.push_back({"UPDATE tasks SET status='COMPLETED', result=$2, completed_at=now(), last_error=NULL "
                  "WHERE task_id=$1::int AND status IN ('ASSIGNED','RUNNING')", {std::to_string(id), result}});
    tx.push_back(historyStmt(f, "COMPLETED", sec, "", m.getInt("start_ms"), m.getInt("end_ms"),
                             static_cast<int>(m.getInt("concurrent"))));
    tx.push_back({"UPDATE ml_predictions SET actual_seconds=$3::double precision WHERE task_id=$1::int AND attempt=$2::int",
                  {std::to_string(id), std::to_string(f.attempt), fmtDouble(sec, 6)}});
    if (f.task.spec.role == "STANDALONE")
        tx.push_back({"UPDATE jobs SET status='COMPLETED', completed_at=now(), result=$2 WHERE job_id=$1::int",
                      {std::to_string(f.task.spec.jobId), result}});
    std::string err;
    if (!db_.transact(tx, err)) log("MASTER", "could not record result of " + label(id) + ": " + err);

    log("MASTER", label(id) + " completed on " + workerId + " in " + fmtDouble(sec, 2) + " s: " + result);
    emit(f, true, result, m.get("data"), "");
    wake();
}

void TaskManager::handleFailure(InFlight f, const std::string& error, bool retryable, double seconds,
                                long long startMs, long long endMs, int concurrent) {
    const long long id = f.task.id;
    std::vector<Database::Stmt> tx;
    tx.push_back(historyStmt(f, "FAILED", seconds, error, startMs, endMs, concurrent));
    const bool retry = retryable && f.attempt < f.task.spec.maxAttempts;
    if (retry) {
        tx.push_back({"UPDATE tasks SET status='PENDING', last_error=$2 WHERE task_id=$1::int AND status IN ('ASSIGNED','RUNNING')",
                      {std::to_string(id), error}});
    } else {
        tx.push_back({"UPDATE tasks SET status='FAILED', result=NULL, last_error=$2, completed_at=now() "
                      "WHERE task_id=$1::int AND status IN ('ASSIGNED','RUNNING')", {std::to_string(id), error}});
        if (f.task.spec.role == "STANDALONE")
            tx.push_back({"UPDATE jobs SET status='FAILED', completed_at=now() WHERE job_id=$1::int",
                          {std::to_string(f.task.spec.jobId)}});
    }
    std::string err;
    if (!db_.transact(tx, err)) log("MASTER", "could not record failure of " + label(id) + ": " + err);

    if (retry) {
        log("MASTER", label(id) + " failed on " + f.workerId + " (" + error + ") - retry " +
                      std::to_string(f.attempt) + "/" + std::to_string(f.task.spec.maxAttempts));
        Pending back = f.task;
        back.excluded.insert(f.workerId);
        {
            std::lock_guard<std::mutex> lock(mutex_);
            pending_.push_back(std::move(back));
        }
        wake();
    } else {
        log("MASTER", label(id) + " FAILED permanently on " + f.workerId + ": " + error);
        emit(f, false, "", "", error);
    }
}

void TaskManager::onTaskFailure(const std::string& workerId, const Message& m) {
    const long long id = m.getInt("task_id");
    InFlight f;
    if (!takeInFlight(id, workerId, m.getInt("attempt", 1), f)) return;
    handleFailure(f, m.get("error", "unknown error"), m.getInt("retryable", 1) != 0, m.getDouble("exec_time"),
                  m.getInt("start_ms"), m.getInt("end_ms"), static_cast<int>(m.getInt("concurrent")));
}

void TaskManager::onWorkerLost(const std::string& workerId, const std::string& reason) {
    std::vector<InFlight> lost;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto it = inFlight_.begin(); it != inFlight_.end();) {
            if (it->second.workerId == workerId) { lost.push_back(it->second); it = inFlight_.erase(it); }
            else ++it;
        }
    }
    for (const auto& f : lost) handleFailure(f, reason, true, 0, 0, 0, 0);
}

}  // namespace nexus

namespace nexus {
void TaskManager::cancelAll(const std::string& reason, const std::vector<long long>& ids) {
    std::set<long long> s(ids.begin(), ids.end());
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pending_.erase(std::remove_if(pending_.begin(), pending_.end(),
                                      [&](const Pending& p) { return s.count(p.id) != 0; }), pending_.end());
    }
    std::string list;
    for (long long id : ids) list += (list.empty() ? "" : ",") + std::to_string(id);
    db_.exec("UPDATE tasks SET status='FAILED', last_error=$2, completed_at=now() "
             "WHERE status='PENDING' AND task_id = ANY(string_to_array($1,',')::int[])", {list, reason});
}
}  // namespace nexus
