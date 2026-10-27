#include "mapreduce.h"

#include "../common/json.h"
#include "../common/log.h"
#include "../common/protocol.h"
#include "../common/wordcount.h"
#include "sql_util.h"

namespace nexus {

namespace {
int reducersOf(Database& db, long long jobId) {
    Database::Result r = db.exec("SELECT params FROM jobs WHERE job_id=$1::int", {std::to_string(jobId)});
    if (!r.ok || r.empty()) return 1;
    return static_cast<int>(paramInt(decodeParams(r.at(0, 0)), "reducers", 1));
}

// PostgreSQL array literal of plain words / numbers (words are [a-z0-9] only: no quoting issues).
std::string pgArray(const std::vector<std::string>& v) {
    std::string out = "{";
    for (size_t i = 0; i < v.size(); ++i) { if (i) out += ","; out += "\"" + v[i] + "\""; }
    return out + "}";
}
}  // namespace

bool MapReduce::start(long long datasetId, int reducers, int priority, long long& jobId, std::string& err) {
    if (reducers < 1 || reducers > 16) { err = "reducers must be 1..16"; return false; }
    if (priority < proto::MIN_PRIORITY || priority > proto::MAX_PRIORITY) { err = "priority must be 0..9"; return false; }
    Database::Result d = db_.exec("SELECT status, text_aligned, name FROM datasets WHERE dataset_id=$1::int",
                                  {std::to_string(datasetId)});
    if (!d.ok || d.empty()) { err = "unknown dataset " + std::to_string(datasetId); return false; }
    if (d.at(0, 0) != "AVAILABLE") { err = "dataset is not AVAILABLE"; return false; }
    if (d.at(0, 1) != "t") {
        err = "dataset was uploaded without text alignment (words could be cut at block borders); "
              "re-upload it with text=1";
        return false;
    }
    Database::Result bl = db_.exec("SELECT block_id, checksum, size_bytes, block_index FROM blocks "
                                   "WHERE dataset_id=$1::int ORDER BY block_index", {std::to_string(datasetId)});
    if (!bl.ok || bl.empty()) { err = "dataset has no blocks"; return false; }

    if (!tasks_.createJob("MAPREDUCE_WORDCOUNT", priority, "reducers=" + std::to_string(reducers), datasetId, jobId, err))
        return false;

    // SPLIT is instantaneous (blocks already exist) but is recorded so every stage is visible.
    std::string e2;
    db_.transact({{"INSERT INTO job_stages (job_id, stage_name, status, task_count, completed_at) "
                   "VALUES ($1::int, 'SPLIT', 'COMPLETED', $2::int, now())",
                   {std::to_string(jobId), std::to_string(bl.size())}},
                  {"INSERT INTO job_stages (job_id, stage_name, status, task_count) VALUES ($1::int, 'MAP', 'RUNNING', $2::int)",
                   {std::to_string(jobId), std::to_string(bl.size())}}}, e2);

    for (size_t i = 0; i < bl.size(); ++i) {
        TaskSpec t;
        t.jobId = jobId;
        t.type = "BLOCK_WORD_COUNT";
        t.role = "MAP";
        t.priority = priority;
        t.blockId = bl.at(i, 0);
        t.inputBytes = bl.i(i, 2);
        t.partition = static_cast<int>(bl.i(i, 3));
        t.params["block_id"] = t.blockId;
        t.params["checksum"] = bl.at(i, 1);
        t.params["reducers"] = std::to_string(reducers);
        long long tid = 0;
        if (!tasks_.addTask(t, tid, err)) { failJob(jobId, err); return false; }
    }
    log("MAPREDUCE", "Job " + std::to_string(jobId) + " started on dataset " + std::to_string(datasetId) + ": " +
                     std::to_string(bl.size()) + " map task(s), " + std::to_string(reducers) + " reducer(s)");
    return true;
}

void MapReduce::failJob(long long jobId, const std::string& why) {
    tasks_.cancelJob(jobId, "job failed: " + why);
    db_.exec("UPDATE jobs SET status='FAILED', completed_at=now(), result=$2 WHERE job_id=$1::int AND status<>'COMPLETED'",
             {std::to_string(jobId), "FAILED: " + why});
    db_.exec("UPDATE job_stages SET status='FAILED', completed_at=now() WHERE job_id=$1::int AND status='RUNNING'",
             {std::to_string(jobId)});
    log("MAPREDUCE", "Job " + std::to_string(jobId) + " FAILED: " + why);
}

void MapReduce::onTaskEvent(const TaskEvent& ev) {
    if (!ev.success) { failJob(ev.jobId, "task " + TaskManager::label(ev.taskId) + " failed: " + ev.error); return; }
    if (ev.role == "MAP") {
        const int R = reducersOf(db_, ev.jobId);
        std::vector<std::string> parts = wc::splitMapOutput(ev.data, R);
        std::vector<Database::Stmt> tx;
        for (int p = 0; p < R; ++p)
            tx.push_back({"INSERT INTO mr_intermediate (job_id, map_task_id, partition_idx, data) "
                          "VALUES ($1::int, $2::int, $3::int, $4) ON CONFLICT (job_id, map_task_id, partition_idx) "
                          "DO UPDATE SET data = EXCLUDED.data",
                          {std::to_string(ev.jobId), std::to_string(ev.taskId), std::to_string(p), parts[static_cast<size_t>(p)]}});
        std::string err;
        if (!db_.transact(tx, err)) { failJob(ev.jobId, "cannot store map output: " + err); return; }
    } else if (ev.role == "REDUCE") {
        wc::Counts merged;
        wc::mergeLines(ev.data, merged);
        std::vector<std::string> words, counts;
        for (const auto& kv : merged) { words.push_back(kv.first); counts.push_back(std::to_string(kv.second)); }
        std::string countArray = "{";
        for (size_t i = 0; i < counts.size(); ++i) countArray += (i ? "," : "") + counts[i];
        countArray += "}";
        std::string err;
        // output rows + the "partition stored" marker commit together
        if (!db_.transact({{"INSERT INTO mr_results (job_id, word, count) SELECT $1::int, w, c FROM unnest($2::text[], $3::bigint[]) AS t(w, c) "
                            "ON CONFLICT (job_id, word) DO UPDATE SET count = EXCLUDED.count",
                            {std::to_string(ev.jobId), pgArray(words), countArray}},
                           {"INSERT INTO mr_reduce_outputs (job_id, partition_idx, task_id, words) VALUES ($1::int, $2::int, $3::int, $4::int) "
                            "ON CONFLICT (job_id, partition_idx) DO UPDATE SET task_id=EXCLUDED.task_id, words=EXCLUDED.words, stored_at=now()",
                            {std::to_string(ev.jobId), std::to_string(ev.partition), std::to_string(ev.taskId),
                             std::to_string(merged.size())}}}, err)) {
            failJob(ev.jobId, "cannot store reduce output: " + err);
            return;
        }
    }
    advance(ev.jobId);
}

void MapReduce::advance(long long jobId) {
    std::lock_guard<std::mutex> lock(mutex_);
    const std::string jid = std::to_string(jobId);
    Database::Result job = db_.exec("SELECT status FROM jobs WHERE job_id=$1::int AND job_type='MAPREDUCE_WORDCOUNT'", {jid});
    if (!job.ok || job.empty() || job.at(0, 0) != "RUNNING") return;
    const int R = reducersOf(db_, jobId);

    auto count = [&](const char* role, bool onlyOpen) {
        Database::Result c = db_.exec(std::string("SELECT count(*) FROM tasks WHERE job_id=$1::int AND role=$2") +
                                      (onlyOpen ? " AND status<>'COMPLETED'" : ""), {jid, role});
        return c.ok && !c.empty() ? c.i(0, 0) : -1LL;
    };

    Database::Result reduceStage = db_.exec("SELECT 1 FROM job_stages WHERE job_id=$1::int AND stage_name='REDUCE'", {jid});
    if (reduceStage.ok && reduceStage.empty()) {
        // ---- MAP -> SHUFFLE -> REDUCE transition (only when every map task is done) ----
        // A task row turns COMPLETED slightly BEFORE its output is stored, so also require that the
        // output of every map task is really in mr_intermediate before shuffling.
        Database::Result stored = db_.exec("SELECT count(DISTINCT map_task_id) FROM mr_intermediate WHERE job_id=$1::int", {jid});
        if (count("MAP", true) != 0 || !stored.ok || stored.empty() || stored.i(0, 0) != count("MAP", false)) return;
        Database::Result created = db_.exec(
            "INSERT INTO job_stages (job_id, stage_name, status, task_count) VALUES ($1::int, 'REDUCE', 'RUNNING', $2::int) "
            "ON CONFLICT (job_id, stage_name) DO NOTHING", {jid, std::to_string(R)});
        if (!created.ok || created.affected == 0) return;                 // someone else did it
        db_.exec("UPDATE job_stages SET status='COMPLETED', completed_at=now() WHERE job_id=$1::int AND stage_name='MAP'", {jid});
        db_.exec("INSERT INTO job_stages (job_id, stage_name, status, task_count, started_at) "
                 "SELECT $1::int, 'SHUFFLE', 'RUNNING', $2::int, completed_at FROM job_stages WHERE job_id=$1::int AND stage_name='MAP' "
                 "ON CONFLICT (job_id, stage_name) DO NOTHING", {jid, std::to_string(R)});
        for (int p = 0; p < R; ++p) {
            Database::Result in = db_.exec(
                "SELECT COALESCE(string_agg(data, '' ORDER BY map_task_id), '') FROM mr_intermediate "
                "WHERE job_id=$1::int AND partition_idx=$2::int", {jid, std::to_string(p)});
            TaskSpec t;
            t.jobId = jobId;
            t.type = "REDUCE_WORD_COUNT";
            t.role = "REDUCE";
            t.partition = p;
            t.params["partition"] = std::to_string(p);
            t.params["inputs"] = in.ok && !in.empty() ? in.at(0, 0) : "";
            t.inputBytes = static_cast<long long>(t.params["inputs"].size());
            Database::Result pr = db_.exec("SELECT priority FROM jobs WHERE job_id=$1::int", {jid});
            t.priority = pr.ok && !pr.empty() ? static_cast<int>(pr.i(0, 0)) : 5;
            long long tid = 0;
            std::string err;
            if (!tasks_.addTask(t, tid, err)) { failJob(jobId, "cannot create reduce task: " + err); return; }
        }
        db_.exec("UPDATE job_stages SET status='COMPLETED', completed_at=now() WHERE job_id=$1::int AND stage_name='SHUFFLE'", {jid});
        log("MAPREDUCE", "Job " + jid + ": map stage finished, " + std::to_string(R) + " reduce task(s) created");
        return;
    }

    // ---- REDUCE -> FINALIZE ----
    Database::Result outs = db_.exec("SELECT count(*) FROM mr_reduce_outputs WHERE job_id=$1::int", {jid});
    if (count("REDUCE", true) != 0 || count("REDUCE", false) != R || !outs.ok || outs.empty() || outs.i(0, 0) != R) return;
    Database::Result done = db_.exec(
        "UPDATE jobs SET status='COMPLETED', completed_at=now() WHERE job_id=$1::int AND status='RUNNING' RETURNING job_id", {jid});
    if (!done.ok || done.empty()) return;                                  // already finalised
    Database::Result tot = db_.exec("SELECT count(*), COALESCE(sum(count),0) FROM mr_results WHERE job_id=$1::int", {jid});
    Database::Result top = db_.exec("SELECT word, count FROM mr_results WHERE job_id=$1::int ORDER BY count DESC, word LIMIT 5", {jid});
    std::string summary = "unique_words=" + (tot.ok && !tot.empty() ? tot.at(0, 0) : "?") +
                          ", total_words=" + (tot.ok && !tot.empty() ? tot.at(0, 1) : "?") + ", top=";
    for (size_t i = 0; top.ok && i < top.size(); ++i) summary += (i ? " " : "") + top.at(i, 0) + "(" + top.at(i, 1) + ")";
    db_.exec("UPDATE jobs SET result=$2 WHERE job_id=$1::int", {jid, summary});
    db_.exec("UPDATE job_stages SET status='COMPLETED', completed_at=now() WHERE job_id=$1::int AND stage_name='REDUCE'", {jid});
    db_.exec("INSERT INTO job_stages (job_id, stage_name, status, task_count, completed_at) VALUES ($1::int,'FINALIZE','COMPLETED',1, now()) "
             "ON CONFLICT (job_id, stage_name) DO NOTHING", {jid});
    log("MAPREDUCE", "Job " + jid + " COMPLETED: " + summary);
}

void MapReduce::resume() {
    Database::Result r = db_.exec("SELECT job_id FROM jobs WHERE job_type='MAPREDUCE_WORDCOUNT' AND status='RUNNING'");
    for (size_t i = 0; r.ok && i < r.size(); ++i) {
        log("MAPREDUCE", "Resuming job " + r.at(i, 0) + " after Master start");
        advance(r.i(i, 0));
    }
}

std::string MapReduce::jobJson(long long jobId) {
    const std::string jid = std::to_string(jobId);
    Database::Result job = db_.exec(
        "SELECT j.job_id, j.job_type, j.status, j.priority, j.params, j.result, j.dataset_id, d.name AS dataset_name, "
        NEXUS_TS("j.submitted_at") " AS submitted_at, " NEXUS_TS("j.completed_at") " AS completed_at, "
        "extract(epoch FROM (COALESCE(j.completed_at, now()) - j.submitted_at)) AS elapsed_s "
        "FROM jobs j LEFT JOIN datasets d ON d.dataset_id=j.dataset_id WHERE j.job_id=$1::int", {jid});
    if (!job.ok || job.empty()) return "";
    Database::Result stages = db_.exec(
        "SELECT stage_name, status, task_count, " NEXUS_TS("started_at") " AS started_at, " NEXUS_TS("completed_at") " AS completed_at, "
        "extract(epoch FROM (COALESCE(completed_at, now()) - started_at)) AS duration_s "
        "FROM job_stages WHERE job_id=$1::int ORDER BY stage_id", {jid});
    Database::Result tasks = db_.exec(
        "SELECT t.task_id, t.task_type, t.role, t.status, t.assigned_worker, t.attempts, t.partition_idx, t.block_id, t.last_error, "
        "(SELECT execution_time FROM execution_history e WHERE e.task_id=t.task_id AND e.status='COMPLETED' ORDER BY execution_id DESC LIMIT 1) AS exec_s, "
        "t.result FROM tasks t WHERE t.job_id=$1::int ORDER BY t.task_id", {jid});
    Database::Result words = db_.exec("SELECT word, count FROM mr_results WHERE job_id=$1::int ORDER BY count DESC, word LIMIT 25", {jid});
    Database::Result prog = db_.exec(
        "SELECT count(*) FILTER (WHERE status='COMPLETED') AS done, count(*) AS total FROM tasks WHERE job_id=$1::int", {jid});

    std::string out = job.rowJson(0);
    out.pop_back();
    out += ",\"stages\":" + stages.toJson() + ",\"tasks\":" + tasks.toJson() + ",\"top_words\":" + words.toJson() +
           ",\"tasks_done\":" + (prog.ok && !prog.empty() ? prog.at(0, 0) : "0") +
           ",\"tasks_total\":" + (prog.ok && !prog.empty() ? prog.at(0, 1) : "0") + "}";
    return out;
}

}  // namespace nexus
