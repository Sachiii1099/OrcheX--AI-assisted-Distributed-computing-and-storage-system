#include "worker_client.h"

#include <unistd.h>

#include <chrono>
#include <cstdio>

#include "../common/log.h"
#include "../common/protocol.h"
#include "../common/sha256.h"
#include "../common/util.h"
#include "task_executor.h"

namespace nexus {

WorkerClient::WorkerClient(Config cfg)
    : cfg_(std::move(cfg)), monitor_(cfg_.storageDir.empty() ? "/" : cfg_.storageDir),
      store_(cfg_.storageDir) {}

void WorkerClient::stop() {
    stop_ = true;
    session_ = false;
    wake_.notify_all();
    if (conn_) conn_->shutdown();
}

bool WorkerClient::connectAndRegister() {
    log(tag(), "Connecting to Master at " + cfg_.masterHost + ":" + std::to_string(cfg_.masterPort) + "...");

    // The Master may not be up yet: retry for a while instead of failing at once.
    std::string err;
    conn_.reset();
    for (int attempt = 1; attempt <= 10 && !stop_; ++attempt) {
        conn_ = Connection::connectTo(cfg_.masterHost, cfg_.masterPort, err);
        if (conn_) break;
        log(tag(), "Connect failed (" + err + "), retry " + std::to_string(attempt) + "/10");
        for (int i = 0; i < 20 && !stop_; ++i) std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    if (!conn_) return false;

    char host[256] = "unknown";
    ::gethostname(host, sizeof host);

    Message reg(proto::REGISTER_WORKER);
    reg.set("worker_id", cfg_.workerId)
       .set("hostname", host)
       .setInt("cpu_cores", monitor_.cpuCores())
       .setInt("memory_mb", monitor_.totalMemoryMb())
       .setInt("capacity", cfg_.capacity)
       .setInt("threads", poolThreads())
       .setInt("logical_cpus", static_cast<long long>(cfg_.affinity.allowed.size()))
       .set("cpu_affinity", cfg_.affinity.pinned() ? formatCpuList(cfg_.affinity.cpus) : "")
       .set("affinity_status", cfg_.affinity.status)
       .set("affinity_detail", cfg_.affinity.detail);
    if (!conn_->send(reg)) { log(tag(), "Could not send registration"); return false; }

    Message ack;
    if (conn_->receive(ack, 10000) != 1 || ack.type() != proto::REGISTER_ACK) {
        log(tag(), "Registration timed out");
        return false;
    }
    if (ack.get("status") != "OK") {
        log(tag(), "Registration REJECTED by Master: " + ack.get("reason"));
        return false;
    }
    log(tag(), "Registration successful (max " + std::to_string(cfg_.capacity) + " concurrent tasks, " +
               std::to_string(poolThreads()) + " executor threads, CPU affinity: " + cfg_.affinity.status +
               (cfg_.affinity.pinned() ? " -> CPU " + formatCpuList(cfg_.affinity.cpus) : "") + ")");
    return true;
}

void WorkerClient::heartbeatLoop(std::shared_ptr<Connection> conn) {
    unsigned long long seq = 0;
    while (session_) {
        // Heartbeat first: proves liveness even if resource sampling were slow.
        Message hb(proto::HEARTBEAT);
        hb.set("worker_id", cfg_.workerId).setInt("seq", static_cast<long long>(++seq))
          .setInt("running", accepted_.load());
        if (!conn->send(hb)) break;

        ResourceSample s = monitor_.sample();
        Message ru(proto::RESOURCE_UPDATE);
        ru.set("worker_id", cfg_.workerId)
          .setDouble("cpu", s.cpuPercent).setDouble("ram", s.ramPercent).setDouble("disk", s.diskPercent)
          .setInt("ram_avail_mb", s.ramAvailableMb).setDouble("load1", s.load1)
          .setDouble("rss_mb", s.processRssMb).setInt("running", accepted_.load())
          .setInt("capacity", cfg_.capacity).setInt("storage_bytes", store_.usedBytes());
        if (!conn->send(ru)) break;
        char buf[128];
        std::snprintf(buf, sizeof buf, "HEARTBEAT | CPU %.0f%% RAM %.0f%% DISK %.0f%% | tasks %d/%d",
                      s.cpuPercent, s.ramPercent, s.diskPercent, accepted_.load(), cfg_.capacity);
        log(tag(), buf);

        std::unique_lock<std::mutex> lock(wakeMutex_);
        wake_.wait_for(lock, std::chrono::seconds(cfg_.heartbeatSec), [this] { return !session_.load(); });
    }
}

void WorkerClient::executeTask(std::shared_ptr<Connection> conn, Message assign) {
    const long long taskId = assign.getInt("task_id");
    const long long attempt = assign.getInt("attempt", 1);
    const std::string lbl  = assign.get("task_label");
    const std::string type = assign.get("task_type");
    const int scale        = static_cast<int>(assign.getInt("scale", 1));
    const Params params    = decodeParams(assign.get("params"));

    const int concurrent = ++executing_;            // how many tasks run at once, including this one
    const long long startMs = nowMs();
    log(tag(), "Task received: " + lbl + " (" + type + ", attempt " + std::to_string(attempt) +
               ") - executing, " + std::to_string(concurrent) + " running concurrently");
    Message started(proto::TASK_STARTED);
    started.setInt("task_id", taskId).setInt("attempt", attempt).set("worker_id", cfg_.workerId)
           .setInt("start_ms", startMs);
    conn->send(started);

    TaskOutcome out = TaskExecutor::run(type, scale, params, &store_);
    const long long endMs = nowMs();

    if (out.ok) {
        log(tag(), "Task completed: " + lbl + " in " + fmtDouble(out.seconds, 2) + " s -> " + out.result);
        Message res(proto::TASK_RESULT);
        res.setInt("task_id", taskId).setInt("attempt", attempt).set("worker_id", cfg_.workerId)
           .setDouble("exec_time", out.seconds).set("result", out.result).set("data", out.data)
           .setInt("start_ms", startMs).setInt("end_ms", endMs).setInt("concurrent", concurrent);
        conn->send(res);
    } else {
        log(tag(), "Task FAILED: " + lbl + ": " + out.error);
        Message fail(proto::TASK_FAILURE);
        fail.setInt("task_id", taskId).setInt("attempt", attempt).set("worker_id", cfg_.workerId)
            .setDouble("exec_time", out.seconds).set("error", out.error)
            .setInt("retryable", out.retryable ? 1 : 0)
            .setInt("start_ms", startMs).setInt("end_ms", endMs).setInt("concurrent", concurrent);
        conn->send(fail);
    }
    --executing_;
    --accepted_;
}

void WorkerClient::handleBlockRequest(Connection& conn, const Message& m) {
    Message reply(proto::BLOCK_REPLY);
    reply.set("req_id", m.get("req_id")).set("worker_id", cfg_.workerId).set("block_id", m.get("block_id"));
    std::string err;
    const std::string& t = m.type();
    const std::string blockId = m.get("block_id");

    if (t == proto::BLOCK_STORE) {
        std::string data;
        if (!base64Decode(m.get("data"), data)) { reply.set("ok", "0").set("error", "bad base64 payload"); }
        else if (store_.put(blockId, data, m.get("checksum"), err)) {
            reply.set("ok", "1").setInt("size", static_cast<long long>(data.size()));
            log(tag(), "Stored block " + blockId + " (" + std::to_string(data.size()) + " bytes)");
        } else reply.set("ok", "0").set("error", err);
    } else if (t == proto::BLOCK_FETCH) {
        std::string data;
        if (store_.get(blockId, data, err)) {
            // checksum of what is on disk NOW, so the Master can detect on-disk corruption
            reply.set("ok", "1").set("data", base64Encode(data)).set("checksum", sha256Hex(data))
                 .setInt("size", static_cast<long long>(data.size()));
        } else reply.set("ok", "0").set("error", err);
    } else if (t == proto::BLOCK_VERIFY) {
        BlockStore::Status s = store_.verify(blockId, m.get("checksum"));
        reply.set("ok", "1").set("state", s == BlockStore::Status::OK ? "HEALTHY"
                                           : s == BlockStore::Status::MISSING ? "MISSING" : "CORRUPT");
    } else if (t == proto::BLOCK_DELETE) {
        if (store_.remove(blockId, err)) reply.set("ok", "1"); else reply.set("ok", "0").set("error", err);
    }
    conn.send(reply);
}

int WorkerClient::run() {
    if (!store_.ready()) {
        log(tag(), "Cannot use storage directory '" + cfg_.storageDir + "'");
        return 1;
    }
    session_ = true;
    if (!connectAndRegister()) { session_ = false; return stop_ ? 0 : 1; }

    auto conn = conn_;
    // Bounded pool: `capacity` executor threads; the Master never assigns more than
    // `capacity` tasks, the small queue only absorbs races.
    pool_.reset(new ThreadPool(static_cast<size_t>(poolThreads()), static_cast<size_t>(cfg_.capacity)));
    std::thread hbThread(&WorkerClient::heartbeatLoop, this, conn);

    Message m;
    while (session_) {
        int r = conn->receive(m, 1000);
        if (r < 0) { if (!stop_) log(tag(), "Connection to Master lost"); break; }
        if (r == 0) continue;
        const std::string& t = m.type();
        if (t == proto::TASK_ASSIGN) {
            ++accepted_;
            Message copy = m;
            bool queued = pool_->submit([this, conn, copy]() { executeTask(conn, copy); });
            if (!queued) {
                --accepted_;
                log(tag(), "Rejecting " + m.get("task_label") + ": worker is full");
                Message fail(proto::TASK_FAILURE);
                fail.setInt("task_id", m.getInt("task_id")).setInt("attempt", m.getInt("attempt", 1))
                    .set("worker_id", cfg_.workerId).set("error", "worker full: concurrency limit reached")
                    .setInt("retryable", 1);
                conn->send(fail);
            }
        } else if (t == proto::BLOCK_STORE || t == proto::BLOCK_FETCH || t == proto::BLOCK_VERIFY ||
                   t == proto::BLOCK_DELETE) {
            handleBlockRequest(*conn, m);
        } else {
            log(tag(), "Ignoring unknown message type '" + t + "'");
        }
    }

    session_ = false;
    wake_.notify_all();
    conn->shutdown();
    hbThread.join();
    pool_.reset();                       // waits for tasks that are still executing
    conn_.reset();
    return stop_ ? 0 : 2;
}

}  // namespace nexus
