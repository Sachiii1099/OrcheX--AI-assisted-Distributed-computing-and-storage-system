// NEXUS Master: TCP server for Workers + scheduler + HTTP API/dashboard + console.
//   ./master              run the Master (console on stdin, dashboard on http://127.0.0.1:8080)
//   ./master --init-db    create/upgrade the PostgreSQL tables from database/schema.sql + seed.sql, then exit
#include <poll.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <csignal>
#include <cstdio>
#include <iostream>
#include <sstream>

#include "../common/config.h"
#include "../common/json.h"
#include "../common/log.h"
#include "../common/protocol.h"
#include "../common/util.h"
#include "../database/database.h"
#include "api.h"
#include "http_server.h"
#include "mapreduce.h"
#include "master_server.h"
#include "ml_manager.h"
#include "scheduler.h"
#include "storage_manager.h"
#include "task_manager.h"
#include "worker_manager.h"

using namespace nexus;

namespace {

std::atomic<bool> g_stop{false};
void onSignal(int) { g_stop = true; }

std::string exeDir() {
    char buf[4096];
    ssize_t n = ::readlink("/proc/self/exe", buf, sizeof buf - 1);
    if (n <= 0) return ".";
    buf[n] = 0;
    std::string p = buf;
    size_t s = p.find_last_of('/');
    return s == std::string::npos ? "." : p.substr(0, s);
}

bool dirExists(const std::string& d) {
    struct stat st;
    return ::stat(d.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

// Finds a project sub-directory: explicit env var, ./name, <exe>/../name
std::string findDir(const char* envName, const std::string& name) {
    std::string e = envOr(envName, "");
    if (!e.empty()) return e;
    for (const std::string& c : {name, exeDir() + "/../" + name, exeDir() + "/" + name})
        if (dirExists(c)) return c;
    return name;
}

void printTable(const Database::Result& r) {
    if (!r.ok) { std::printf("  query failed: %s\n", r.error.c_str()); return; }
    if (r.rows.empty()) { std::printf("  (no rows)\n"); return; }
    std::vector<size_t> w(r.columns.size());
    for (size_t c = 0; c < w.size(); ++c) w[c] = r.columns[c].size();
    for (const auto& row : r.rows)
        for (size_t c = 0; c < row.size(); ++c) w[c] = std::max(w[c], std::min<size_t>(row[c].size(), 48));
    auto line = [&](const std::vector<std::string>& cells) {
        std::printf("  ");
        for (size_t c = 0; c < cells.size(); ++c)
            std::printf("%-*.*s  ", (int)w[c], 48, cells[c].c_str());
        std::printf("\n");
    };
    line(r.columns);
    std::string sep;
    for (size_t c = 0; c < w.size(); ++c) sep += std::string(w[c], '-') + "  ";
    std::printf("  %s\n", sep.c_str());
    for (const auto& row : r.rows) line(row);
}

void printHelp() {
    std::printf(
        "\nCommands:\n"
        "  submit <TYPE> [scale] [priority] [block_id]   submit one task (priority 0-9, higher = more urgent)\n"
        "        TYPE: WORD_COUNT PRIME_COUNT MATRIX_MULTIPLY CHECKSUM CPU_BURN BLOCK_CHECKSUM BLOCK_WORD_COUNT\n"
        "  burst <N> <TYPE> [scale] [priority]           submit N tasks back-to-back\n"
        "  policy [NAME]                                 show / set: FCFS ROUND_ROBIN PRIORITY RESOURCE_AWARE AI_ASSISTED\n"
        "  workers | tasks | history | decisions         live tables from PostgreSQL\n"
        "  upload <file> [replication] [block_size] [text]   split a local file into blocks and store them on Workers\n"
        "  datasets | dataset <id> | verify <id> | repair <id>\n"
        "  download <id> <name>                          reconstruct dataset into $NEXUS_EXPORT_DIR/<name>\n"
        "  mapreduce <dataset_id> [reducers] [priority]  word-count MapReduce over a text dataset\n"
        "  job <id>                                      job details (stages, tasks, top words)\n"
        "  train | model                                 train / show the task-duration model\n"
        "  help | quit\n\n");
}

}  // namespace

int main(int argc, char** argv) {
    std::signal(SIGPIPE, SIG_IGN);
    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    const int port = envIntOr("NEXUS_MASTER_PORT", proto::DEFAULT_MASTER_PORT);
    const int hb   = std::max(1, envIntOr("NEXUS_HEARTBEAT_SEC", proto::DEFAULT_HEARTBEAT_SEC));
    const int hbTimeout = hb * proto::HEARTBEAT_MISS_FACTOR;

    std::printf("=====================================================\n"
                "  NEXUS MASTER  -  distributed processing & storage\n"
                "=====================================================\n");

    Database db(Database::Config::fromEnv());
    std::string err;
    if (!db.connect(err)) {
        log("MASTER", "Cannot connect to PostgreSQL: " + err);
        log("MASTER", "Is PostgreSQL running? Check NEXUS_DB_HOST / PORT / NAME / USER / PASSWORD (see .env.example) "
                      "and run ./setup/setup.sh once to create the database.");
        return 1;
    }
    log("MASTER", "Connected to PostgreSQL");

    const std::string dbDir = findDir("NEXUS_DB_DIR", "database");
    if (argc > 1 && std::string(argv[1]) == "--init-db") {
        std::string sql, e2;
        for (const char* f : {"schema.sql", "seed.sql"}) {
            if (!readFile(dbDir + "/" + f, sql)) { log("MASTER", "Cannot read " + dbDir + "/" + f); return 1; }
            if (!db.runScript(sql, e2)) { log("MASTER", std::string("Applying ") + f + " failed: " + e2); return 1; }
            log("MASTER", std::string("Applied ") + f);
        }
        return 0;
    }
    std::string missing;
    if (!db.schemaPresent(missing)) {
        log("MASTER", "Database schema is missing or outdated (table '" + missing + "' not found).");
        log("MASTER", "Run ./setup/setup.sh   or   ./build/master --init-db   and start the Master again.");
        return 1;
    }
    db.markAllWorkersInactive();

    Scheduler scheduler(hbTimeout, hbTimeout);
    WorkerManager workers(db, hbTimeout);
    StorageManager storage(db, workers);
    ml::TrainConfig mlCfg = ml::TrainConfig::fromEnv();
    MlManager mlm(db, mlCfg);
    TaskManager tasks(db, workers, scheduler, storage, mlm);
    MapReduce mr(db, tasks);
    tasks.setEventCallback([&mr](const TaskEvent& ev) { mr.onTaskEvent(ev); });

    mlm.loadFromDisk();
    loadPolicy(db, scheduler);
    tasks.recoverOnStart();
    mr.resume();

    MasterServer server(port, hb, workers, tasks);
    if (!server.start(err)) {
        log("MASTER", "Cannot listen on port " + std::to_string(port) + ": " + err);
        return 1;
    }
    log("MASTER", "Listening for Workers on TCP port " + std::to_string(port) +
                  " (heartbeat " + std::to_string(hb) + " s, timeout " + std::to_string(hbTimeout) + " s)");
    tasks.start();

    BenchmarkRunner bench(db, tasks, scheduler, workers, mlm);
    bench.recoverOnStart();
    mlm.startAutoRetrain(envIntOr("NEXUS_ML_AUTO_RETRAIN_EVERY", 100));
    AppContext ctx{db, workers, scheduler, tasks, storage, mr, mlm, bench, findDir("NEXUS_DASHBOARD_DIR", "dashboard"),
                   nowMs(), static_cast<size_t>(envIntOr("NEXUS_BLOCK_SIZE", 256 * 1024)), envIntOr("NEXUS_REPLICATION", 2)};
    const size_t maxUpload = static_cast<size_t>(envIntOr("NEXUS_MAX_UPLOAD_MB", 64)) * 1024 * 1024;
    HttpServer http(envOr("NEXUS_HTTP_BIND", "127.0.0.1"), envIntOr("NEXUS_HTTP_PORT", proto::DEFAULT_HTTP_PORT), maxUpload);
    registerApi(http, ctx);
    const int httpPort = envIntOr("NEXUS_HTTP_PORT", proto::DEFAULT_HTTP_PORT);
    if (!http.start(err)) {
        log("MASTER", "Cannot start the HTTP API on port " + std::to_string(httpPort) + ": " + err +
                      " (dashboard disabled; set NEXUS_HTTP_PORT)");
    } else {
        log("MASTER", "Dashboard + API: http://" + envOr("NEXUS_HTTP_BIND", "127.0.0.1") + ":" + std::to_string(httpPort) + "/");
    }
    printHelp();

    const std::string exportDir = envOr("NEXUS_EXPORT_DIR", "exports");

    // Console loop. poll() on stdin so Ctrl+C is noticed promptly.
    std::string line;
    bool stdinOpen = true;
    while (!g_stop) {
        if (!stdinOpen) { std::this_thread::sleep_for(std::chrono::milliseconds(200)); continue; }
        pollfd p;
        p.fd = STDIN_FILENO; p.events = POLLIN; p.revents = 0;
        int pr = ::poll(&p, 1, 300);
        if (pr <= 0) continue;
        if (!std::getline(std::cin, line)) { stdinOpen = false; continue; }  // EOF: keep serving until Ctrl+C

        std::istringstream in(line);
        std::string cmd;
        in >> cmd;
        if (cmd.empty()) continue;

        if (cmd == "quit" || cmd == "exit") {
            break;
        } else if (cmd == "help") {
            printHelp();
        } else if (cmd == "submit") {
            std::string type, block; int scale = 1, prio = proto::DEFAULT_PRIORITY;
            in >> type >> scale >> prio >> block;
            if (type.empty()) { std::printf("usage: submit <TYPE> [scale] [priority] [block_id]\n"); continue; }
            Params extra;
            if (!block.empty()) extra["block_id"] = block;
            if (!tasks.submit(type, scale, prio, extra, err)) std::printf("Submit failed: %s\n", err.c_str());
        } else if (cmd == "burst") {
            int n = 0, scale = 1, prio = proto::DEFAULT_PRIORITY; std::string type;
            in >> n >> type >> scale >> prio;
            if (n < 1 || type.empty()) { std::printf("usage: burst <N> <TYPE> [scale] [priority]\n"); continue; }
            for (int i = 0; i < n; ++i)
                if (!tasks.submit(type, scale, prio, {}, err)) { std::printf("Submit failed: %s\n", err.c_str()); break; }
        } else if (cmd == "policy") {
            std::string name;
            in >> name;
            if (name.empty()) std::printf("active policy: %s\n", policyName(scheduler.policy()));
            else if (!applyPolicy(db, scheduler, name, err)) std::printf("%s\n", err.c_str());
        } else if (cmd == "workers") {
            printTable(db.exec(
                "SELECT worker_id, hostname, status, "
                "(status='ACTIVE' AND last_heartbeat > now() - make_interval(secs => $1::double precision)) AS healthy, "
                "round(cpu_usage::numeric,1) AS cpu_pct, round(ram_usage::numeric,1) AS ram_pct, "
                "capacity, (SELECT count(*) FROM tasks t WHERE t.assigned_worker=worker_id AND t.status IN ('ASSIGNED','RUNNING')) AS running, "
                "cpu_cores, memory_total_mb AS mem_mb, to_char(last_heartbeat,'HH24:MI:SS') AS last_hb "
                "FROM workers ORDER BY worker_id", {std::to_string(hbTimeout)}));
        } else if (cmd == "tasks") {
            printTable(db.exec(
                "SELECT task_id, job_id, task_type, role, priority, assigned_worker AS worker, status, attempts AS att, "
                "to_char(completed_at,'HH24:MI:SS') AS done_at, left(COALESCE(result, last_error),60) AS result "
                "FROM tasks ORDER BY task_id DESC LIMIT 15"));
        } else if (cmd == "history") {
            printTable(db.exec(
                "SELECT execution_id AS id, task_id, worker_id, task_type, round(execution_time::numeric,3) AS exec_s, status, "
                "concurrent_at_start AS conc, round(worker_cpu_at_assign::numeric,0) AS cpu_at_assign, scheduler_policy "
                "FROM execution_history ORDER BY execution_id DESC LIMIT 15"));
        } else if (cmd == "decisions") {
            printTable(db.exec(
                "SELECT decision_id AS id, task_id, effective_policy AS policy, worker_id, round(score::numeric,3) AS score, "
                "round(predicted_seconds::numeric,3) AS pred_s, fallback, left(reason,70) AS reason "
                "FROM scheduling_decisions ORDER BY decision_id DESC LIMIT 15"));
        } else if (cmd == "upload") {
            std::string path, text; int rep = ctx.defaultReplication; long long bs = static_cast<long long>(ctx.defaultBlockSize);
            in >> path >> rep >> bs >> text;
            std::string data;
            struct stat st;
            if (path.empty() || ::stat(path.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) { std::printf("usage: upload <regular file> [replication] [block_size] [text]\n"); continue; }
            if (static_cast<size_t>(st.st_size) > maxUpload) { std::printf("file larger than NEXUS_MAX_UPLOAD_MB\n"); continue; }
            if (!readFile(path, data)) { std::printf("cannot read %s\n", path.c_str()); continue; }
            auto u = storage.upload(path, data, static_cast<size_t>(bs), rep, text == "text" || text == "1");
            if (!u.ok) std::printf("Upload failed: %s\n", u.error.c_str());
            else std::printf("Dataset %lld stored: %d blocks, %d replicas, %d under-replicated blocks, sha256 %s\n",
                             u.datasetId, u.blocks, u.replicasStored, u.underReplicated, u.sha256.c_str());
        } else if (cmd == "datasets") {
            printTable(db.exec("SELECT dataset_id AS id, name, original_size AS bytes, block_count AS blocks, block_size, replication AS repl, "
                               "text_aligned AS text, status, left(sha256,12) AS sha256 FROM datasets ORDER BY dataset_id DESC LIMIT 20"));
        } else if (cmd == "dataset") {
            long long id = 0; in >> id;
            std::string j = storage.inspect(id);
            if (j.empty()) std::printf("no such dataset\n");
            else printTable(db.exec(
                "SELECT b.block_id, b.block_index AS idx, b.size_bytes AS bytes, left(b.checksum,10) AS sha256, "
                "COALESCE(string_agg(r.worker_id || ':' || r.status, ', ' ORDER BY r.worker_id), '-') AS replicas "
                "FROM blocks b LEFT JOIN block_replicas r ON r.block_id=b.block_id WHERE b.dataset_id=$1::int "
                "GROUP BY b.block_id, b.block_index, b.size_bytes, b.checksum ORDER BY b.block_index", {std::to_string(id)}));
        } else if (cmd == "verify" || cmd == "repair") {
            long long id = 0; in >> id;
            std::string j = cmd == "verify" ? storage.verify(id) : storage.repair(id);
            std::printf("%s\n", j.empty() ? "no such dataset" : j.c_str());
        } else if (cmd == "download") {
            long long id = 0; std::string name; in >> id >> name;
            if (name.empty()) { std::printf("usage: download <id> <name>\n"); continue; }
            std::string data;
            auto rr = storage.reconstruct(id, data);
            if (!rr.ok) { std::printf("Download failed: %s\n", rr.error.c_str()); for (auto& b : rr.unavailableBlocks) std::printf("  unavailable: %s\n", b.c_str()); continue; }
            ::mkdir(exportDir.c_str(), 0755);
            const std::string out = exportDir + "/" + sanitizeFileName(name);   // basename only: no path traversal
            if (writeFile(out, data)) std::printf("Reconstructed %zu bytes -> %s (checksum verified)\n", data.size(), out.c_str());
            else std::printf("cannot write %s\n", out.c_str());
        } else if (cmd == "mapreduce") {
            long long ds = 0, job = 0; int red = 2, prio = proto::DEFAULT_PRIORITY;
            in >> ds >> red >> prio;
            if (!mr.start(ds, red, prio, job, err)) std::printf("MapReduce failed: %s\n", err.c_str());
            else std::printf("MapReduce job %lld started\n", job);
        } else if (cmd == "job") {
            long long id = 0; in >> id;
            std::string j = mr.jobJson(id);
            std::printf("%s\n", j.empty() ? "no such job" : j.c_str());
        } else if (cmd == "train") {
            ml::TrainReport r = mlm.train();
            std::printf("%s: %s\n", r.status.c_str(), r.message.c_str());
        } else if (cmd == "model") {
            std::printf("%s\n", mlm.statusJson().c_str());
        } else {
            std::printf("unknown command '%s' (try: help)\n", cmd.c_str());
        }
    }

    log("MASTER", "Shutting down...");
    http.stop();
    tasks.stop();
    server.stop();
    return 0;
}
