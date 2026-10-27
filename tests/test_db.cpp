// PostgreSQL-backed tests: schema, constraints, transactions, persistence, Worker registry
// (registration, heartbeat expiry, duplicate ids) and the whole task life cycle driven by
// in-process "fake workers" connected through real sockets (socketpair).
//
// SAFETY: these tests TRUNCATE every NEXUS table, so they only run against a database whose
// name ends in "_test" (scripts/run_tests.sh creates "nexus_test"). Otherwise they are SKIPPED
// (exit code 77), never reported as passed.
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <functional>
#include <memory>
#include <thread>

#include "../common/config.h"
#include "../common/protocol.h"
#include "../master/ml_manager.h"
#include "../master/scheduler.h"
#include "../master/storage_manager.h"
#include "../master/task_manager.h"
#include "../master/worker_manager.h"
#include "test_util.h"

using namespace nexus;

namespace {

bool waitFor(const std::function<bool()>& cond, int ms = 5000) {
    auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
    while (std::chrono::steady_clock::now() < end) {
        if (cond()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return cond();
}

struct Fake {
    std::string id;
    std::shared_ptr<Connection> masterSide;
    std::unique_ptr<Connection> peer;           // what the Worker process would hold
};

std::unique_ptr<Fake> addWorker(WorkerManager& wm, const std::string& id, int capacity, double cpu = 10, double ram = 20) {
    int sv[2];
    ::socketpair(AF_UNIX, SOCK_STREAM, 0, sv);
    auto f = std::unique_ptr<Fake>(new Fake());
    f->id = id;
    f->masterSide = std::make_shared<Connection>(sv[0]);
    f->peer.reset(new Connection(sv[1]));
    Message reg(proto::REGISTER_WORKER);
    reg.set("worker_id", id).set("hostname", "testhost").setInt("cpu_cores", 4).setInt("memory_mb", 1000).setInt("capacity", capacity);
    std::string why;
    CHECK(wm.registerWorker(reg, f->masterSide, why) == WorkerManager::RegisterResult::OK);
    Database::ResourceRow row;
    row.cpu = cpu; row.ram = ram; row.disk = 30;
    wm.onResourceUpdate(id, row);
    return f;
}

bool recvAssign(Fake& w, Message& m, int ms = 5000) {
    auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
    while (std::chrono::steady_clock::now() < end) {
        int r = w.peer->receive(m, 100);
        if (r < 0) return false;
        if (r == 1 && m.type() == proto::TASK_ASSIGN) return true;
    }
    return false;
}

Message resultFor(const Message& assign, const std::string& workerId, double secs = 0.25) {
    Message r(proto::TASK_RESULT);
    long long now = nowMs();
    r.setInt("task_id", assign.getInt("task_id")).setInt("attempt", assign.getInt("attempt")).set("worker_id", workerId)
     .setDouble("exec_time", secs).set("result", "ok-" + assign.get("task_label")).set("data", "")
     .setInt("start_ms", now - 250).setInt("end_ms", now).setInt("concurrent", 1);
    return r;
}

Message failureFor(const Message& assign, const std::string& workerId, bool retryable, const std::string& why) {
    Message r(proto::TASK_FAILURE);
    r.setInt("task_id", assign.getInt("task_id")).setInt("attempt", assign.getInt("attempt")).set("worker_id", workerId)
     .setDouble("exec_time", 0.01).set("error", why).setInt("retryable", retryable ? 1 : 0);
    return r;
}

std::string scalar(Database& db, const std::string& sql, const std::vector<std::string>& p = {}) {
    Database::Result r = db.exec(sql, p);
    return r.ok && !r.empty() ? r.at(0, 0) : "<error:" + r.error + ">";
}

void wipe(Database& db) {
    db.exec("TRUNCATE benchmark_results, benchmark_runs, ml_predictions, ml_training_runs, mr_results, mr_intermediate, job_stages, block_replicas, blocks, datasets, "
            "scheduling_decisions, execution_history, resource_metrics, tasks, jobs, workers RESTART IDENTITY CASCADE");
}

}  // namespace

int main() {
    Database::Config cfg = Database::Config::fromEnv();
    if (cfg.password.empty() || cfg.name.size() < 5 || cfg.name.compare(cfg.name.size() - 5, 5, "_test") != 0) {
        std::printf("SKIPPED: set NEXUS_DB_* and NEXUS_DB_NAME=nexus_test (these tests wipe the database). "
                    "Run ./scripts/run_tests.sh which prepares it.\n");
        return 77;
    }
    Database db(cfg);
    std::string err;
    if (!db.connect(err)) { std::printf("SKIPPED: cannot connect to PostgreSQL: %s\n", err.c_str()); return 77; }

    TEST_CASE("schema: idempotent initialisation, all tables present");
    {
        std::string schema, seed;
        CHECK(readFile(std::string(NEXUS_SOURCE_DIR) + "/database/schema.sql", schema));
        CHECK(readFile(std::string(NEXUS_SOURCE_DIR) + "/database/seed.sql", seed));
        for (int i = 0; i < 2; ++i) {                       // twice: must not fail the second time
            CHECK(db.runScript(schema, err));
            CHECK(db.runScript(seed, err));
        }
        std::string missing;
        CHECK(db.schemaPresent(missing));
        wipe(db);
        CHECK(db.runScript(seed, err));
        CHECK_EQ(scalar(db, "SELECT value FROM scheduler_config WHERE key='policy'"), std::string("RESOURCE_AWARE"));
        CHECK_EQ(scalar(db, "SELECT count(*) FROM task_types"), std::string("8"));
    }

    TEST_CASE("constraints, foreign keys, parameterised SQL");
    {
        CHECK(db.upsertWorker("TDB_C", "h", "127.0.0.1", 4, 1000, 2));
        // SQL injection attempt is just data
        CHECK(db.upsertWorker("x'); DROP TABLE workers;--", "h", "ip", 1, 1, 1));
        std::string missing;
        CHECK(db.schemaPresent(missing));
        CHECK_EQ(scalar(db, "SELECT count(*) FROM workers WHERE worker_id LIKE 'x%'"), std::string("1"));
        CHECK(!db.exec("INSERT INTO jobs (job_type) VALUES ('X') RETURNING job_id").empty());
        CHECK(!db.exec("INSERT INTO tasks (job_id, task_type, priority) VALUES (1, 'CPU_BURN', 11)").ok);      // CHECK priority 0..9
        CHECK(!db.exec("INSERT INTO tasks (job_id, task_type, status) VALUES (1, 'CPU_BURN', 'BOGUS')").ok);   // CHECK status
        CHECK(!db.exec("INSERT INTO tasks (job_id, task_type) VALUES (9999, 'CPU_BURN')").ok);                // FK job
        CHECK(!db.exec("INSERT INTO block_replicas (block_id, worker_id) VALUES ('nope', 'TDB_C')").ok);     // FK block
        CHECK(!db.exec("INSERT INTO datasets (name, original_size, block_size, replication, sha256) VALUES ('a', 0, 1, 1, 'x')").ok);
        CHECK(!db.exec("UPDATE workers SET status='MAYBE' WHERE worker_id='TDB_C'").ok);
        Database::Result r = db.exec("SELECT worker_id, cpu_cores, cpu_usage, (cpu_cores > 2) AS big, NULL::int AS n FROM workers WHERE worker_id='TDB_C'");
        std::string j = r.rowJson(0);
        CHECK(j.find("\"cpu_cores\":4") != std::string::npos);
        CHECK(j.find("\"big\":true") != std::string::npos);
        CHECK(j.find("\"n\":null") != std::string::npos);
    }

    TEST_CASE("transactions: all-or-nothing, and data survives a new connection (persistence)");
    {
        std::string e;
        bool ok = db.transact({{"INSERT INTO workers (worker_id) VALUES ('TDB_TX1')", {}},
                               {"INSERT INTO workers (worker_id) VALUES ('TDB_TX2')", {}},
                               {"INSERT INTO nonexistent_table VALUES (1)", {}}}, e);
        CHECK(!ok);
        CHECK_EQ(scalar(db, "SELECT count(*) FROM workers WHERE worker_id LIKE 'TDB_TX%'"), std::string("0"));   // rolled back
        CHECK(db.transact({{"INSERT INTO workers (worker_id) VALUES ('TDB_TX1')", {}},
                           {"INSERT INTO workers (worker_id) VALUES ('TDB_TX2')", {}}}, e));
        Database fresh(cfg);                                  // a brand-new connection = "after restart"
        CHECK(fresh.connect(err));
        CHECK_EQ(scalar(fresh, "SELECT count(*) FROM workers WHERE worker_id LIKE 'TDB_TX%'"), std::string("2"));
        CHECK(db.exec("UPDATE workers SET cpu_usage=$2::double precision WHERE worker_id=$1", {"TDB_TX1", "42.5"}).ok);
        CHECK_EQ(scalar(fresh, "SELECT cpu_usage FROM workers WHERE worker_id='TDB_TX1'"), std::string("42.5"));
    }

    TEST_CASE("worker registry: registration, heartbeat keeps alive, expiry, duplicate ids");
    {
        wipe(db);
        WorkerManager wm(db, 1);                              // 1 s timeout
        auto w = addWorker(wm, "TDB_HB", 3);
        CHECK(wm.isHealthy("TDB_HB"));
        CHECK_EQ(scalar(db, "SELECT status FROM workers WHERE worker_id='TDB_HB'"), std::string("ACTIVE"));
        CHECK_EQ(scalar(db, "SELECT capacity FROM workers WHERE worker_id='TDB_HB'"), std::string("3"));
        CHECK_EQ(scalar(db, "SELECT cpu_usage FROM workers WHERE worker_id='TDB_HB'"), std::string("10"));
        CHECK_EQ(scalar(db, "SELECT count(*) FROM resource_metrics WHERE worker_id='TDB_HB'"), std::string("1"));

        // duplicate id while the first is alive
        int sv[2];
        ::socketpair(AF_UNIX, SOCK_STREAM, 0, sv);
        auto dupConn = std::make_shared<Connection>(sv[0]);
        Connection dupPeer(sv[1]);
        Message reg(proto::REGISTER_WORKER);
        reg.set("worker_id", "TDB_HB").setInt("capacity", 1);
        std::string why;
        CHECK(wm.registerWorker(reg, dupConn, why) == WorkerManager::RegisterResult::DUPLICATE_ID);
        CHECK(!why.empty());

        std::this_thread::sleep_for(std::chrono::milliseconds(700));
        wm.onHeartbeat("TDB_HB", 0);                          // heartbeat resets the clock
        std::this_thread::sleep_for(std::chrono::milliseconds(700));
        CHECK(wm.expireStale().empty());
        CHECK(wm.isHealthy("TDB_HB"));
        std::this_thread::sleep_for(std::chrono::milliseconds(1300));   // silence > timeout
        CHECK(!wm.isHealthy("TDB_HB"));                       // unhealthy even before the sweep runs
        auto expired = wm.expireStale();
        CHECK_EQ(expired.size(), 1u);
        CHECK_EQ(scalar(db, "SELECT status FROM workers WHERE worker_id='TDB_HB'"), std::string("INACTIVE"));
        CHECK(wm.expireStale().empty());                      // reported once
        Message tmp;
        CHECK_EQ(w->peer->receive(tmp, 500), -1);             // the Master closed the dead Worker's socket
        // after expiry the id may register again
        auto again = addWorker(wm, "TDB_HB", 3);
        CHECK(wm.isHealthy("TDB_HB"));
    }

    // ---- task life cycle with fake workers ----------------------------------------------------
    wipe(db);
    WorkerManager wm(db, 60);
    Scheduler sched(60, 60);
    StorageManager storage(db, wm);
    ml::TrainConfig mcfg;
    mcfg.modelPath = "/tmp/nexus_test_no_such_model.model";
    MlManager mlm(db, mcfg);
    TaskManager tm(db, wm, sched, storage, mlm);
    tm.start();
    std::string e;

    TEST_CASE("FCFS: tasks are dispatched in submission order regardless of priority");
    {
        sched.setPolicy(Policy::FCFS);
        long long t1, t2, t3;
        CHECK(tm.submit("CPU_BURN", 1, 1, {}, e, &t1));
        CHECK(tm.submit("CPU_BURN", 1, 9, {}, e, &t2));
        CHECK(tm.submit("CPU_BURN", 1, 5, {}, e, &t3));
        auto w = addWorker(wm, "TDB_F", 1);
        std::vector<long long> order;
        for (int i = 0; i < 3; ++i) {
            Message a;
            CHECK(recvAssign(*w, a));
            order.push_back(a.getInt("task_id"));
            tm.onTaskStarted("TDB_F", a);
            tm.onTaskResult("TDB_F", resultFor(a, "TDB_F"));
        }
        CHECK((order == std::vector<long long>{t1, t2, t3}));
        CHECK(waitFor([&] { return scalar(db, "SELECT count(*) FROM tasks WHERE status='COMPLETED'") == "3"; }));
        CHECK_EQ(scalar(db, "SELECT count(*) FROM jobs WHERE status='COMPLETED'"), std::string("3"));
        CHECK_EQ(scalar(db, "SELECT count(*) FROM execution_history WHERE status='COMPLETED'"), std::string("3"));
        CHECK_EQ(scalar(db, "SELECT count(*) FROM scheduling_decisions WHERE effective_policy='FCFS'"), std::string("3"));
        CHECK_EQ(scalar(db, "SELECT count(*) FROM execution_history WHERE concurrent_at_start=1 AND started_at IS NOT NULL AND finished_at IS NOT NULL"), std::string("3"));
        CHECK_EQ(scalar(db, "SELECT result FROM tasks WHERE task_id=" + std::to_string(t1)), "ok-" + TaskManager::label(t1));
        CHECK(scalar(db, "SELECT candidates FROM scheduling_decisions WHERE task_id=" + std::to_string(t1)).find("TDB_F") != std::string::npos);
    }

    TEST_CASE("Priority: highest first, equal priority in submission order");
    {
        wipe(db);
        std::string e2;
        // fresh manager state: no worker yet so everything queues up
        WorkerManager wm2(db, 60);
        Scheduler s2(60, 60);
        StorageManager st2(db, wm2);
        TaskManager tm2(db, wm2, s2, st2, mlm);
        tm2.start();
        s2.setPolicy(Policy::PRIORITY);
        long long a, b, c, d;
        CHECK(tm2.submit("CPU_BURN", 1, 1, {}, e2, &a));
        CHECK(tm2.submit("CPU_BURN", 1, 9, {}, e2, &b));
        CHECK(tm2.submit("CPU_BURN", 1, 5, {}, e2, &c));
        CHECK(tm2.submit("CPU_BURN", 1, 9, {}, e2, &d));
        auto w = addWorker(wm2, "TDB_P", 1);
        std::vector<long long> order;
        for (int i = 0; i < 4; ++i) {
            Message m;
            CHECK(recvAssign(*w, m));
            order.push_back(m.getInt("task_id"));
            tm2.onTaskResult("TDB_P", resultFor(m, "TDB_P"));
        }
        CHECK((order == std::vector<long long>{b, d, c, a}));
        CHECK(!tm2.submit("CPU_BURN", 1, 10, {}, e2));        // priority out of range is rejected
        CHECK(!tm2.submit("CPU_BURN", 0, 5, {}, e2));
        CHECK(!tm2.submit("NOPE", 1, 5, {}, e2));
        CHECK(!tm2.submit("BLOCK_CHECKSUM", 1, 5, {{"block_id", "../etc"}}, e2));
        CHECK(!tm2.submit("BLOCK_CHECKSUM", 1, 5, {{"block_id", "ds9_b9"}}, e2));
        tm2.stop();
    }

    TEST_CASE("Round Robin across three workers; capacity is never exceeded");
    {
        wipe(db);
        std::string e2;
        WorkerManager wm2(db, 60);
        Scheduler s2(60, 60);
        StorageManager st2(db, wm2);
        TaskManager tm2(db, wm2, s2, st2, mlm);
        tm2.start();
        s2.setPolicy(Policy::ROUND_ROBIN);
        auto w1 = addWorker(wm2, "TDB_R1", 2), w2 = addWorker(wm2, "TDB_R2", 2), w3 = addWorker(wm2, "TDB_R3", 2);
        for (int i = 0; i < 6; ++i) CHECK(tm2.submit("CPU_BURN", 1, 5, {}, e2));
        CHECK(waitFor([&] { return scalar(db, "SELECT count(*) FROM tasks WHERE status='ASSIGNED'") == "6"; }));
        Database::Result r = db.exec("SELECT assigned_worker FROM tasks ORDER BY task_id");
        std::string seq;
        for (size_t i = 0; i < r.size(); ++i) seq += r.at(i, 0) + " ";
        CHECK_EQ(seq, std::string("TDB_R1 TDB_R2 TDB_R3 TDB_R1 TDB_R2 TDB_R3 "));
        // all three workers (capacity 2 each) are now full; one more task must wait
        CHECK(tm2.submit("CPU_BURN", 1, 5, {}, e2));
        std::this_thread::sleep_for(std::chrono::milliseconds(800));
        CHECK_EQ(scalar(db, "SELECT count(*) FROM tasks WHERE status='PENDING'"), std::string("1"));
        CHECK_EQ(tm2.inFlightCount(), 6u);
        // finish one task on R2 -> the waiting task is dispatched
        Message m;
        CHECK(recvAssign(*w2, m));
        tm2.onTaskResult("TDB_R2", resultFor(m, "TDB_R2"));
        CHECK(waitFor([&] { return scalar(db, "SELECT count(*) FROM tasks WHERE status='PENDING'") == "0"; }));
        // never more than capacity in flight on any worker
        CHECK_EQ(scalar(db, "SELECT max(n) FROM (SELECT count(*) n FROM tasks WHERE status IN ('ASSIGNED','RUNNING') GROUP BY assigned_worker) x"), std::string("2"));
        tm2.stop();
    }

    TEST_CASE("failure handling: retry on another worker, permanent failure, stale / duplicate results");
    {
        wipe(db);
        std::string e2;
        WorkerManager wm2(db, 60);
        Scheduler s2(60, 60);
        StorageManager st2(db, wm2);
        TaskManager tm2(db, wm2, s2, st2, mlm);
        tm2.start();
        s2.setPolicy(Policy::FCFS);
        auto a = addWorker(wm2, "TDB_A", 1), b = addWorker(wm2, "TDB_B", 1);
        Fake* ws[2] = {a.get(), b.get()};

        // (1) retryable failure -> second attempt on the other worker -> success
        long long t1;
        CHECK(tm2.submit("CPU_BURN", 1, 5, {}, e2, &t1));
        Message m1;
        CHECK(recvAssign(*a, m1));                            // FCFS: lowest id first
        CHECK_EQ(m1.getInt("attempt"), 1LL);
        tm2.onTaskResult("TDB_B", resultFor(m1, "TDB_B"));    // a worker that never got the task claims a result: ignored
        tm2.onTaskStarted("TDB_A", m1);
        tm2.onTaskFailure("TDB_A", failureFor(m1, "TDB_A", true, "simulated crash"));
        Message m2;
        CHECK(recvAssign(*b, m2));                            // retried elsewhere (A is avoided)
        CHECK_EQ(m2.getInt("task_id"), t1);
        CHECK_EQ(m2.getInt("attempt"), 2LL);
        Message stale = resultFor(m1, "TDB_A");               // late result of the dead attempt 1
        tm2.onTaskResult("TDB_A", stale);
        tm2.onTaskResult("TDB_B", resultFor(m2, "TDB_B"));
        tm2.onTaskResult("TDB_B", resultFor(m2, "TDB_B"));    // duplicate delivery of the same result
        CHECK(waitFor([&] { return scalar(db, "SELECT status FROM tasks WHERE task_id=" + std::to_string(t1)) == "COMPLETED"; }));
        CHECK_EQ(scalar(db, "SELECT attempts FROM tasks WHERE task_id=" + std::to_string(t1)), std::string("2"));
        CHECK_EQ(scalar(db, "SELECT count(*) FROM execution_history WHERE task_id=" + std::to_string(t1)), std::string("2"));
        CHECK_EQ(scalar(db, "SELECT count(*) FROM execution_history WHERE status='FAILED' AND task_id=" + std::to_string(t1)), std::string("1"));
        CHECK_EQ(scalar(db, "SELECT status FROM jobs WHERE job_id=(SELECT job_id FROM tasks WHERE task_id=" + std::to_string(t1) + ")"), std::string("COMPLETED"));
        CHECK_EQ(scalar(db, "SELECT count(*) FROM scheduling_decisions WHERE task_id=" + std::to_string(t1)), std::string("2"));

        // (2) deterministic failure: no retry, job FAILED
        long long t2;
        CHECK(tm2.submit("CPU_BURN", 1, 5, {}, e2, &t2));
        Message m3;
        CHECK(recvAssign(*a, m3));
        tm2.onTaskFailure("TDB_A", failureFor(m3, "TDB_A", false, "bad parameters"));
        CHECK(waitFor([&] { return scalar(db, "SELECT status FROM tasks WHERE task_id=" + std::to_string(t2)) == "FAILED"; }));
        CHECK_EQ(scalar(db, "SELECT attempts FROM tasks WHERE task_id=" + std::to_string(t2)), std::string("1"));
        CHECK_EQ(scalar(db, "SELECT status FROM jobs WHERE job_id=(SELECT job_id FROM tasks WHERE task_id=" + std::to_string(t2) + ")"), std::string("FAILED"));
        CHECK_EQ(scalar(db, "SELECT last_error FROM tasks WHERE task_id=" + std::to_string(t2)), std::string("bad parameters"));

        // (3) retryable failures until attempts are exhausted (max 3)
        long long t3;
        CHECK(tm2.submit("CPU_BURN", 1, 5, {}, e2, &t3));
        for (int attempt = 1; attempt <= 3; ++attempt) {
            Message m;
            bool got = false;
            for (int k = 0; k < 2 && !got; ++k) if (recvAssign(*ws[k], m, 700)) { got = true; tm2.onTaskFailure(ws[k]->id, failureFor(m, ws[k]->id, true, "boom")); }
            CHECK(got);
        }
        CHECK(waitFor([&] { return scalar(db, "SELECT status FROM tasks WHERE task_id=" + std::to_string(t3)) == "FAILED"; }));
        CHECK_EQ(scalar(db, "SELECT attempts FROM tasks WHERE task_id=" + std::to_string(t3)), std::string("3"));
        CHECK_EQ(scalar(db, "SELECT count(*) FROM execution_history WHERE status='FAILED' AND task_id=" + std::to_string(t3)), std::string("3"));

        // (4) the worker disappears while holding a task -> task re-queued on the survivor
        long long t4;
        CHECK(tm2.submit("CPU_BURN", 1, 5, {}, e2, &t4));
        Message m4;
        CHECK(recvAssign(*a, m4));
        tm2.onWorkerLost("TDB_A", "worker disconnected");
        Message m5;
        CHECK(recvAssign(*b, m5));
        CHECK_EQ(m5.getInt("task_id"), t4);
        CHECK_EQ(m5.getInt("attempt"), 2LL);
        tm2.onTaskResult("TDB_B", resultFor(m5, "TDB_B"));
        CHECK(waitFor([&] { return scalar(db, "SELECT status FROM tasks WHERE task_id=" + std::to_string(t4)) == "COMPLETED"; }));
        tm2.stop();
    }

    TEST_CASE("recovery: PENDING survives a restart, in-flight work is re-queued");
    {
        wipe(db);
        std::string e2;
        long long a;
        {
            WorkerManager wm2(db, 60);
            Scheduler s2(60, 60);
            StorageManager st2(db, wm2);
            TaskManager tm2(db, wm2, s2, st2, mlm);
            CHECK(tm2.submit("CPU_BURN", 1, 5, {}, e2, &a));             // never started: no workers
        }                                                               // "Master stops"
        // simulate a task that was in flight at crash time
        CHECK(db.upsertWorker("TDB_Z", "h", "ip", 1, 1, 1));
        CHECK(db.exec("INSERT INTO jobs (job_type) VALUES ('CPU_BURN')").ok);
        CHECK(db.exec("INSERT INTO tasks (job_id, task_type, status, assigned_worker, attempts, params) "
                      "VALUES ((SELECT max(job_id) FROM jobs), 'CPU_BURN', 'RUNNING', 'TDB_Z', 1, 'scale=1')").ok);
        WorkerManager wm3(db, 60);
        Scheduler s3(60, 60);
        StorageManager st3(db, wm3);
        TaskManager tm3(db, wm3, s3, st3, mlm);
        tm3.recoverOnStart();
        CHECK_EQ(tm3.pendingCount(), 2u);
        CHECK_EQ(scalar(db, "SELECT count(*) FROM tasks WHERE status='PENDING'"), std::string("2"));
        auto w = addWorker(wm3, "TDB_Y", 4);
        tm3.start();
        Message m1, m2;
        CHECK(recvAssign(*w, m1));
        CHECK(recvAssign(*w, m2));
        tm3.stop();
    }

    tm.stop();
    wipe(db);
    return testResult("test_db");
}
