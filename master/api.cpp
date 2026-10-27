#include "api.h"

#include <cctype>
#include <chrono>
#include <map>

#include "../common/config.h"
#include "../common/json.h"
#include "../common/log.h"
#include "../common/protocol.h"
#include "sql_util.h"

namespace nexus {

// ------------------------------------------------------------- policy ------

bool applyPolicy(Database& db, Scheduler& scheduler, const std::string& name, std::string& err) {
    Policy p;
    if (!parsePolicy(name, p)) {
        err = "unknown policy '" + name + "' (FCFS, ROUND_ROBIN, PRIORITY, RESOURCE_AWARE, AI_ASSISTED)";
        return false;
    }
    Database::Result r = db.exec(
        "INSERT INTO scheduler_config (key, value) VALUES ('policy', $1) "
        "ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=now()", {policyName(p)});
    if (!r.ok) { err = "database error: " + r.error; return false; }
    scheduler.setPolicy(p);
    log("SCHEDULER", std::string("Active scheduling policy is now ") + policyName(p));
    return true;
}

void loadPolicy(Database& db, Scheduler& scheduler) {
    Policy p = Policy::RESOURCE_AWARE;
    const std::string fromEnv = envOr("NEXUS_SCHED_POLICY", "");
    Database::Result r = db.exec("SELECT value FROM scheduler_config WHERE key='policy'");
    if (!fromEnv.empty() && parsePolicy(fromEnv, p)) {
        std::string err;
        applyPolicy(db, scheduler, fromEnv, err);          // env overrides and is persisted
        return;
    }
    if (r.ok && !r.empty()) parsePolicy(r.at(0, 0), p);
    scheduler.setPolicy(p);
    log("SCHEDULER", std::string("Scheduling policy: ") + policyName(p));
}

// -------------------------------------------------------------- helpers ----

namespace {

using Resp = HttpResponse;

std::string rowsOrEmpty(const Database::Result& r) { return r.ok ? r.toJson() : "[]"; }

Resp dbError(const Database::Result& r) { return Resp::error(500, "database error: " + r.error); }

int clampLimit(const HttpRequest& rq, int def, int max) {
    long long v = rq.paramInt("limit", def);
    return static_cast<int>(v < 1 ? 1 : (v > max ? max : v));
}

bool truthy(const std::string& s) {
    std::string u = upper(s);
    return u == "1" || u == "TRUE" || u == "YES" || u == "ON";
}

std::string policyInfoJson() {
    struct P { const char* name; const char* order; const char* worker; };
    static const P ps[] = {
        {"FCFS", "submission order", "first eligible Worker (lowest id)"},
        {"ROUND_ROBIN", "submission order", "rotate through eligible Workers"},
        {"PRIORITY", "priority (9 first), then submission order", "least-occupied eligible Worker"},
        {"RESOURCE_AWARE", "submission order", "lowest weighted CPU/RAM/disk/queue score"},
        {"AI_ASSISTED", "priority, then submission order", "shortest predicted duration (falls back to RESOURCE_AWARE)"}};
    std::string out = "[";
    for (size_t i = 0; i < sizeof ps / sizeof ps[0]; ++i) {
        if (i) out += ",";
        out += json::Obj().add("name", ps[i].name).add("task_order", ps[i].order).add("worker_choice", ps[i].worker).build();
    }
    return out + "]";
}

const char* kWorkerCols =
    "w.worker_id, w.hostname, w.ip_address, w.cpu_cores, w.memory_total_mb, w.capacity, "
    "round(w.cpu_usage::numeric,1) AS cpu, round(w.ram_usage::numeric,1) AS ram, round(w.disk_usage::numeric,1) AS disk, "
    "w.ram_available_mb, round(w.load1::numeric,2) AS load1, round(w.process_rss_mb::numeric,1) AS process_rss_mb, "
    "w.storage_bytes, w.status, w.pool_threads, w.logical_cpus, w.cpu_affinity, w.affinity_status, w.affinity_detail, "
    "extract(epoch FROM (now() - w.last_heartbeat)) AS heartbeat_age_s, "
    "(w.status='ACTIVE' AND w.last_heartbeat > now() - make_interval(secs => $1::double precision)) AS healthy, "
    "(SELECT count(*) FROM tasks t WHERE t.assigned_worker=w.worker_id AND t.status IN ('ASSIGNED','RUNNING')) AS running_tasks, "
    "(SELECT count(*) FROM block_replicas br WHERE br.worker_id=w.worker_id) AS blocks_stored, "
    NEXUS_TS("w.last_heartbeat") " AS last_heartbeat";

}  // namespace

// ------------------------------------------------------------------ routes --

void registerApi(HttpServer& http, AppContext& c) {
    const std::string hbTimeout = std::to_string(c.workers.heartbeatTimeoutSec());

    // ---- static dashboard ----
    auto serveFile = [&c](const std::string& name, const char* type) {
        return [&c, name, type](const HttpRequest&) {
            std::string data;
            if (!readFile(c.dashboardDir + "/" + name, data))
                return Resp::error(404, "dashboard file '" + name + "' not found in " + c.dashboardDir +
                                        " (set NEXUS_DASHBOARD_DIR)");
            return Resp::text(data, type);
        };
    };
    http.route("GET", "/", serveFile("index.html", "text/html; charset=utf-8"));
    http.route("GET", "/index.html", serveFile("index.html", "text/html; charset=utf-8"));
    http.route("GET", "/app.js", serveFile("app.js", "application/javascript; charset=utf-8"));
    http.route("GET", "/style.css", serveFile("style.css", "text/css; charset=utf-8"));

    // ---- health / overview ----
    http.route("GET", "/api/health", [&c](const HttpRequest&) {
        return Resp::json(json::Obj().add("ok", true).add("db", c.db.connected()).build());
    });

    http.route("GET", "/api/overview", [&c, hbTimeout](const HttpRequest&) {
        Database::Result w = c.db.exec(
            "SELECT count(*) AS total, "
            "count(*) FILTER (WHERE status='ACTIVE' AND last_heartbeat > now() - make_interval(secs => $1::double precision)) AS healthy, "
            "COALESCE(sum(capacity) FILTER (WHERE status='ACTIVE' AND last_heartbeat > now() - make_interval(secs => $1::double precision)),0) AS capacity "
            "FROM workers", {hbTimeout});
        Database::Result t = c.db.exec(
            "SELECT count(*) FILTER (WHERE status='PENDING') AS pending, count(*) FILTER (WHERE status='ASSIGNED') AS assigned, "
            "count(*) FILTER (WHERE status='RUNNING') AS running, count(*) FILTER (WHERE status='COMPLETED') AS completed, "
            "count(*) FILTER (WHERE status='FAILED') AS failed, count(*) AS total FROM tasks");
        Database::Result j = c.db.exec(
            "SELECT count(*) AS total, count(*) FILTER (WHERE status='RUNNING') AS running, "
            "count(*) FILTER (WHERE status='COMPLETED') AS completed, count(*) FILTER (WHERE status='FAILED') AS failed FROM jobs");
        Database::Result s = c.db.exec(
            "SELECT (SELECT count(*) FROM datasets WHERE status='AVAILABLE') AS datasets, (SELECT count(*) FROM blocks) AS blocks, "
            "(SELECT count(*) FROM block_replicas) AS replicas, (SELECT COALESCE(sum(original_size),0) FROM datasets WHERE status='AVAILABLE') AS bytes");
        Database::Result h = c.db.exec(
            "SELECT hostname, count(*) AS workers, string_agg(worker_id, ',' ORDER BY worker_id) AS ids, max(cpu_cores) AS cores, "
            "max(memory_total_mb) AS memory_mb FROM workers WHERE status='ACTIVE' AND last_heartbeat > now() - make_interval(secs => $1::double precision) "
            "GROUP BY hostname ORDER BY hostname", {hbTimeout});
        std::string hosts = "[";
        bool shared = false;
        for (size_t i = 0; h.ok && i < h.size(); ++i) {
            if (h.i(i, 1) > 1) shared = true;
            std::string row = h.rowJson(i);
            row.pop_back();
            row += ",\"shared_host\":" + json::boolean(h.i(i, 1) > 1) + "}";
            if (i) hosts += ",";
            hosts += row;
        }
        hosts += "]";
        json::Obj o;
        o.add("policy", policyName(c.scheduler.policy())).add("uptime_s", (nowMs() - c.startedMs) / 1000)
         .add("db_ok", c.db.connected()).add("heartbeat_timeout_s", c.workers.heartbeatTimeoutSec())
         .raw("workers", w.ok ? w.rowJson(0) : "null").raw("tasks", t.ok ? t.rowJson(0) : "null")
         .raw("jobs", j.ok ? j.rowJson(0) : "null").raw("storage", s.ok ? s.rowJson(0) : "null")
         .raw("hosts", hosts).add("queue_in_memory", static_cast<long long>(c.tasks.pendingCount()))
         .add("in_flight", static_cast<long long>(c.tasks.inFlightCount())).add("model_loaded", c.ml.hasModel())
         .add("shared_host_warning", shared)
         .add("shared_host_note", shared ? "Several Workers run on the same physical host: they share its CPU cores and RAM, "
                                           "so they are separate processes, not separate machines." : "");
        return Resp::json(o.build());
    });

    // ---- workers ----
    http.route("GET", "/api/workers", [&c, hbTimeout](const HttpRequest&) {
        Database::Result r = c.db.exec(std::string("SELECT ") + kWorkerCols + " FROM workers w ORDER BY w.worker_id", {hbTimeout});
        return r.ok ? Resp::json(r.toJson()) : dbError(r);
    });

    // ---- scheduling policy ----
    http.route("GET", "/api/policy", [&c](const HttpRequest&) {
        return Resp::json(json::Obj().add("policy", policyName(c.scheduler.policy()))
                              .add("model_loaded", c.ml.hasModel()).raw("available", policyInfoJson()).build());
    });
    http.route("POST", "/api/policy", [&c](const HttpRequest& rq) {
        std::string err;
        if (!applyPolicy(c.db, c.scheduler, rq.param("policy"), err)) return Resp::error(400, err);
        c.tasks.wake();
        return Resp::json(json::Obj().add("policy", policyName(c.scheduler.policy())).build());
    });

    // ---- tasks ----
    http.route("POST", "/api/tasks", [&c](const HttpRequest& rq) {
        Params extra;
        if (!rq.param("block_id").empty()) extra["block_id"] = rq.param("block_id");
        long long tid = 0;
        std::string err;
        if (!c.tasks.submit(rq.param("type"), static_cast<int>(rq.paramInt("scale", 1)),
                            static_cast<int>(rq.paramInt("priority", proto::DEFAULT_PRIORITY)), extra, err, &tid))
            return Resp::error(400, err);
        return Resp::json(json::Obj().add("task_id", tid).add("status", "PENDING").build(), 201);
    });
    http.route("GET", "/api/tasks", [&c](const HttpRequest& rq) {
        Database::Result r = c.db.exec(
            "SELECT t.task_id, t.job_id, t.task_type, t.role, t.status, t.priority, t.scale, t.assigned_worker, t.attempts, "
            "t.max_attempts, t.predicted_seconds, t.block_id, " NEXUS_TS("t.created_at") " AS created_at, "
            NEXUS_TS("t.assigned_at") " AS assigned_at, " NEXUS_TS("t.started_at") " AS started_at, "
            NEXUS_TS("t.completed_at") " AS completed_at, extract(epoch FROM (t.completed_at - t.started_at)) AS run_s, "
            "extract(epoch FROM (COALESCE(t.assigned_at, now()) - t.created_at)) AS queue_wait_s, t.result, t.last_error, "
            "(SELECT effective_policy FROM scheduling_decisions d WHERE d.task_id=t.task_id ORDER BY decision_id DESC LIMIT 1) AS policy "
            "FROM tasks t WHERE ($1 = '' OR t.status = $1) ORDER BY t.task_id DESC LIMIT $2::int",
            {upper(rq.param("status")), std::to_string(clampLimit(rq, 50, 500))});
        return r.ok ? Resp::json(r.toJson()) : dbError(r);
    });
    http.route("GET", "/api/tasks/:id", [&c](const HttpRequest& rq) {
        const std::string id = rq.pathParams.at("id");
        Database::Result t = c.db.exec(
            "SELECT task_id, job_id, task_type, role, status, priority, scale, assigned_worker, attempts, max_attempts, "
            "predicted_seconds, block_id, result, last_error, " NEXUS_TS("created_at") " AS created_at, "
            NEXUS_TS("completed_at") " AS completed_at FROM tasks WHERE task_id=$1::int", {id});
        if (!t.ok) return dbError(t);
        if (t.empty()) return Resp::error(404, "no such task");
        Database::Result d = c.db.exec(
            "SELECT decision_id, attempt, policy, effective_policy, worker_id, score, predicted_seconds, fallback, reason, candidates, "
            NEXUS_TS("created_at") " AS created_at FROM scheduling_decisions WHERE task_id=$1::int ORDER BY decision_id", {id});
        Database::Result h = c.db.exec(
            "SELECT execution_id, worker_id, attempt, status, execution_time, concurrent_at_start, error, "
            NEXUS_TS("started_at") " AS started_at, " NEXUS_TS("finished_at") " AS finished_at "
            "FROM execution_history WHERE task_id=$1::int ORDER BY execution_id", {id});
        std::string o = t.rowJson(0);
        o.pop_back();
        o += ",\"decisions\":" + rowsOrEmpty(d) + ",\"executions\":" + rowsOrEmpty(h) + "}";
        return Resp::json(o);
    });

    // ---- jobs / MapReduce ----
    http.route("GET", "/api/jobs", [&c](const HttpRequest& rq) {
        Database::Result r = c.db.exec(
            "SELECT j.job_id, j.job_type, j.status, j.priority, j.dataset_id, j.result, " NEXUS_TS("j.submitted_at") " AS submitted_at, "
            NEXUS_TS("j.completed_at") " AS completed_at, extract(epoch FROM (COALESCE(j.completed_at, now()) - j.submitted_at)) AS elapsed_s, "
            "(SELECT count(*) FROM tasks t WHERE t.job_id=j.job_id) AS tasks, "
            "(SELECT count(*) FROM tasks t WHERE t.job_id=j.job_id AND t.status='COMPLETED') AS tasks_done "
            "FROM jobs j WHERE ($2 = '' OR j.job_type = $2) ORDER BY j.job_id DESC LIMIT $1::int",
            {std::to_string(clampLimit(rq, 30, 200)), upper(rq.param("type"))});
        return r.ok ? Resp::json(r.toJson()) : dbError(r);
    });
    http.route("GET", "/api/jobs/:id", [&c](const HttpRequest& rq) {
        long long id = 0;
        try { id = std::stoll(rq.pathParams.at("id")); } catch (...) { return Resp::error(400, "bad job id"); }
        std::string j = c.mr.jobJson(id);
        if (j.empty()) return Resp::error(404, "no such job");
        return Resp::json(j);
    });
    http.route("GET", "/api/jobs/:id/results", [&c](const HttpRequest& rq) {
        Database::Result r = c.db.exec(
            "SELECT word, count FROM mr_results WHERE job_id=$1::int ORDER BY count DESC, word LIMIT $2::int",
            {rq.pathParams.at("id"), std::to_string(clampLimit(rq, 100, 1000000))});
        return r.ok ? Resp::json(r.toJson()) : dbError(r);
    });
    http.route("POST", "/api/mapreduce", [&c](const HttpRequest& rq) {
        long long job = 0;
        std::string err;
        if (!c.mr.start(rq.paramInt("dataset_id", 0), static_cast<int>(rq.paramInt("reducers", 2)),
                        static_cast<int>(rq.paramInt("priority", proto::DEFAULT_PRIORITY)), job, err))
            return Resp::error(400, err);
        return Resp::json(json::Obj().add("job_id", job).add("status", "RUNNING").build(), 201);
    });

    // ---- history / decisions ----
    http.route("GET", "/api/history", [&c](const HttpRequest& rq) {
        Database::Result r = c.db.exec(
            "SELECT execution_id, task_id, worker_id, task_type, round(execution_time::numeric,3) AS execution_time, status, attempt, scale, "
            "input_bytes, scheduler_policy, round(worker_cpu_at_assign::numeric,1) AS cpu_at_assign, round(worker_ram_at_assign::numeric,1) AS ram_at_assign, "
            "running_at_assign, worker_capacity, concurrent_at_start, round(predicted_seconds::numeric,3) AS predicted_seconds, error, "
            NEXUS_TS("started_at") " AS started_at, " NEXUS_TS("finished_at") " AS finished_at, " NEXUS_TS("created_at") " AS created_at "
            "FROM execution_history ORDER BY execution_id DESC LIMIT $1::int", {std::to_string(clampLimit(rq, 50, 500))});
        return r.ok ? Resp::json(r.toJson()) : dbError(r);
    });
    http.route("GET", "/api/decisions", [&c](const HttpRequest& rq) {
        Database::Result r = c.db.exec(
            "SELECT d.decision_id, d.task_id, t.task_type, t.priority, d.attempt, d.policy, d.effective_policy, d.worker_id, "
            "round(d.score::numeric,4) AS score, round(d.predicted_seconds::numeric,3) AS predicted_seconds, d.fallback, d.reason, d.candidates, "
            "(SELECT round(execution_time::numeric,3) FROM execution_history e WHERE e.task_id=d.task_id AND e.attempt=d.attempt AND e.status='COMPLETED' LIMIT 1) AS actual_seconds, "
            NEXUS_TS("d.created_at") " AS created_at FROM scheduling_decisions d JOIN tasks t ON t.task_id=d.task_id "
            "ORDER BY d.decision_id DESC LIMIT $1::int", {std::to_string(clampLimit(rq, 50, 500))});
        return r.ok ? Resp::json(r.toJson()) : dbError(r);
    });

    // ---- resource charts / concurrency ----
    http.route("GET", "/api/metrics", [&c](const HttpRequest& rq) {
        long long minutes = rq.paramInt("minutes", 10);
        if (minutes < 1) minutes = 1;
        if (minutes > 1440) minutes = 1440;
        Database::Result r = c.db.exec(
            "SELECT * FROM (SELECT worker_id, (extract(epoch FROM \"timestamp\") * 1000)::bigint AS t_ms, round(cpu_usage::numeric,1) AS cpu, "
            "round(ram_usage::numeric,1) AS ram, round(disk_usage::numeric,1) AS disk, running_tasks FROM resource_metrics "
            "WHERE \"timestamp\" > now() - make_interval(mins => $1::int) ORDER BY \"timestamp\" DESC LIMIT 6000) x ORDER BY t_ms",
            {std::to_string(minutes)});
        return r.ok ? Resp::json(r.toJson()) : dbError(r);
    });
    http.route("GET", "/api/concurrency", [&c](const HttpRequest& rq) {
        Database::Result per = c.db.exec(
            "WITH ex AS (SELECT worker_id, started_at, finished_at FROM execution_history "
            "            WHERE status='COMPLETED' AND started_at IS NOT NULL AND finished_at IS NOT NULL), "
            "ev AS (SELECT worker_id, started_at AS t, 1 AS d FROM ex UNION ALL SELECT worker_id, finished_at, -1 FROM ex), "
            "run AS (SELECT worker_id, sum(d) OVER (PARTITION BY worker_id ORDER BY t, d) AS c FROM ev) "
            "SELECT w.worker_id, w.capacity, w.hostname, COALESCE((SELECT max(c) FROM run WHERE run.worker_id=w.worker_id),0) AS max_concurrent, "
            "(SELECT count(*) FROM ex WHERE ex.worker_id=w.worker_id) AS completed, "
            "(SELECT round(avg(execution_time)::numeric,3) FROM execution_history e WHERE e.worker_id=w.worker_id AND e.status='COMPLETED') AS avg_exec_s "
            "FROM workers w ORDER BY w.worker_id");
        Database::Result cl = c.db.exec(
            "WITH ex AS (SELECT started_at, finished_at FROM execution_history WHERE status='COMPLETED' AND started_at IS NOT NULL AND finished_at IS NOT NULL), "
            "ev AS (SELECT started_at AS t, 1 AS d FROM ex UNION ALL SELECT finished_at, -1 FROM ex), "
            "run AS (SELECT sum(d) OVER (ORDER BY t, d) AS c FROM ev) SELECT COALESCE(max(c),0) FROM run");
        Database::Result tl = c.db.exec(
            "SELECT * FROM (SELECT task_id, task_type, worker_id, attempt, concurrent_at_start, "
            "(extract(epoch FROM started_at) * 1000)::bigint AS start_ms, (extract(epoch FROM finished_at) * 1000)::bigint AS end_ms, "
            "round(execution_time::numeric,3) AS execution_time FROM execution_history "
            "WHERE status='COMPLETED' AND started_at IS NOT NULL AND finished_at IS NOT NULL ORDER BY execution_id DESC LIMIT $1::int) x ORDER BY start_ms",
            {std::to_string(clampLimit(rq, 60, 300))});
        json::Obj o;
        o.raw("per_worker", rowsOrEmpty(per)).add("cluster_max_concurrent", cl.ok && !cl.empty() ? cl.i(0, 0) : 0LL)
         .raw("timeline", rowsOrEmpty(tl))
         .add("note", "Concurrency is measured from real start/finish timestamps in execution_history. Workers on the same "
                      "host share its CPU cores; cluster_max_concurrent counts tasks overlapping in time.");
        return Resp::json(o.build());
    });

    // ---- distributed storage ----
    http.route("GET", "/api/datasets", [&c](const HttpRequest&) { return Resp::json(c.storage.listJson()); });
    http.route("POST", "/api/datasets", [&c](const HttpRequest& rq) {
        const long long bs = rq.paramInt("block_size", static_cast<long long>(c.defaultBlockSize));
        const long long rep = rq.paramInt("replication", c.defaultReplication);
        if (bs < static_cast<long long>(StorageManager::kMinBlock) || bs > static_cast<long long>(StorageManager::kMaxBlock))
            return Resp::error(400, "block_size must be " + std::to_string(StorageManager::kMinBlock) + ".." +
                                    std::to_string(StorageManager::kMaxBlock) + " bytes");
        StorageManager::UploadResult u = c.storage.upload(rq.param("name", "upload.bin"), rq.body, static_cast<size_t>(bs),
                                                          static_cast<int>(rep), truthy(rq.param("text")));
        if (!u.ok) return Resp::error(409, u.error);
        json::Obj o;
        o.add("dataset_id", u.datasetId).add("blocks", u.blocks).add("replicas_stored", u.replicasStored)
         .add("under_replicated_blocks", u.underReplicated).add("sha256", u.sha256)
         .add("warning", u.underReplicated ? "some blocks have fewer replicas than requested (not enough distinct healthy Workers)" : "");
        return Resp::json(o.build(), 201);
    });
    http.route("GET", "/api/datasets/:id", [&c](const HttpRequest& rq) {
        long long id = 0;
        try { id = std::stoll(rq.pathParams.at("id")); } catch (...) { return Resp::error(400, "bad dataset id"); }
        std::string j = c.storage.inspect(id);
        return j.empty() ? Resp::error(404, "no such dataset") : Resp::json(j);
    });
    http.route("POST", "/api/datasets/:id/verify", [&c](const HttpRequest& rq) {
        long long id = 0;
        try { id = std::stoll(rq.pathParams.at("id")); } catch (...) { return Resp::error(400, "bad dataset id"); }
        std::string j = c.storage.verify(id);
        return j.empty() ? Resp::error(404, "no such dataset") : Resp::json(j);
    });
    http.route("POST", "/api/datasets/:id/repair", [&c](const HttpRequest& rq) {
        long long id = 0;
        try { id = std::stoll(rq.pathParams.at("id")); } catch (...) { return Resp::error(400, "bad dataset id"); }
        std::string j = c.storage.repair(id);
        return j.empty() ? Resp::error(404, "no such dataset") : Resp::json(j);
    });
    http.route("GET", "/api/datasets/:id/download", [&c](const HttpRequest& rq) {
        long long id = 0;
        try { id = std::stoll(rq.pathParams.at("id")); } catch (...) { return Resp::error(400, "bad dataset id"); }
        Database::Result d = c.db.exec("SELECT name FROM datasets WHERE dataset_id=$1::int", {std::to_string(id)});
        if (!d.ok || d.empty()) return Resp::error(404, "no such dataset");
        std::string data;
        StorageManager::ReconstructResult rr = c.storage.reconstruct(id, data);
        if (!rr.ok) {
            std::string missing = "[";
            for (size_t i = 0; i < rr.unavailableBlocks.size(); ++i) missing += (i ? "," : "") + json::str(rr.unavailableBlocks[i]);
            return Resp::json(json::Obj().add("error", rr.error).raw("unavailable_blocks", missing + "]").build(), 409);
        }
        Resp r = Resp::text(data, "application/octet-stream");
        r.headers["Content-Disposition"] = "attachment; filename=\"" + sanitizeFileName(d.at(0, 0)) + "\"";
        return r;
    });

    // ---- ML ----
    http.route("GET", "/api/ml/status", [&c](const HttpRequest&) { return Resp::json(c.ml.statusJson()); });
    http.route("POST", "/api/ml/train", [&c](const HttpRequest& rq) {
        // optional model_type=xgboost|random_forest (default: NEXUS_ML_MODEL_TYPE, i.e. xgboost)
        std::string mt = rq.param("model_type");
        for (auto& ch : mt) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        if (!mt.empty() && mt != "xgboost" && mt != "random_forest")
            return Resp::error(400, "model_type must be 'xgboost' or 'random_forest'");
        ml::TrainReport r = c.ml.train(mt);
        json::Obj o;
        o.add("status", r.status).add("run_id", r.runId).add("model_type", r.modelType).add("n_samples", r.nSamples)
         .add("n_train", r.nTrain).add("n_test", r.nTest).add("mae", r.mae).add("rmse", r.rmse)
         .add("baseline_mae", r.baselineMae).add("baseline_rmse", r.baselineRmse)
         .add("compare_model", r.compareModel).add("compare_mae", r.compareMae).add("compare_rmse", r.compareRmse)
         .add("hyperparams", r.hyperparams).add("message", r.message).add("model_active", c.ml.hasModel())
         .add("model_kind", c.ml.modelKind());
        return Resp::json(o.build(), r.status == "FAILED" ? 500 : 200);
    });

    // ---- scheduling benchmarks (same workload under each policy; results measured, stored in PostgreSQL) ----
    http.route("POST", "/api/benchmarks", [&c](const HttpRequest& rq) {
        BenchmarkSpec spec;
        std::string err;
        const int smin = static_cast<int>(rq.paramInt("scale_min", 1));
        if (!BenchmarkRunner::parseSpec(rq.param("policies"), rq.param("type", "MIXED"),
                                        static_cast<int>(rq.paramInt("count", 24)), smin,
                                        static_cast<int>(rq.paramInt("scale_max", std::max(smin, 3))),
                                        rq.param("priorities", "varied"), rq.paramInt("seed", 1),
                                        static_cast<int>(rq.paramInt("repeats", 1)), rq.paramInt("dataset_id", 0),
                                        static_cast<int>(rq.paramInt("timeout_sec", 180)), spec, err))
            return Resp::error(400, err);
        spec.name = rq.param("name");
        spec.retrainBefore = truthy(rq.param("retrain_before"));
        spec.modelType = rq.param("model_type");
        long long id = 0;
        if (!c.bench.start(spec, id, err)) return Resp::error(409, err);
        return Resp::json(json::Obj().add("benchmark_id", id).add("status", "RUNNING").build(), 202);
    });
    http.route("GET", "/api/benchmarks", [&c](const HttpRequest&) { return Resp::json(c.bench.listJson()); });
    http.route("POST", "/api/benchmarks/cancel", [&c](const HttpRequest&) {
        c.bench.cancel();
        return Resp::json(json::Obj().add("cancelling", c.bench.running()).build());
    });
    http.route("GET", "/api/benchmarks/:id", [&c](const HttpRequest& rq) {
        long long bid = 0;
        try { bid = std::stoll(rq.pathParams.at("id")); } catch (...) { return Resp::error(400, "bad benchmark id"); }
        std::string j = c.bench.getJson(bid);
        if (j.empty()) return Resp::error(404, "no such benchmark");
        return Resp::json(j);
    });

    // ---- logs ----
    http.route("GET", "/api/logs", [](const HttpRequest& rq) {
        auto lines = LogBuffer::instance().since(rq.paramInt("after", 0), static_cast<size_t>(clampLimit(rq, 200, 500)));
        std::string out = "[";
        long long last = rq.paramInt("after", 0);
        for (size_t i = 0; i < lines.size(); ++i) {
            if (i) out += ",";
            out += json::Obj().add("seq", lines[i].seq).add("ts", lines[i].tsMs).add("tag", lines[i].tag).add("msg", lines[i].msg).build();
            last = lines[i].seq;
        }
        return Resp::json(json::Obj().raw("lines", out + "]").add("last_seq", last).build());
    });
}

}  // namespace nexus
