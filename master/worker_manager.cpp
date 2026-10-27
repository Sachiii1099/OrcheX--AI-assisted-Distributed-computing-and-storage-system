#include "worker_manager.h"

#include "../common/log.h"
#include "../common/protocol.h"

namespace nexus {

using Clock = std::chrono::steady_clock;

bool WorkerManager::healthyLocked(const WorkerInfo& w) const {
    return w.conn && Clock::now() - w.lastHeartbeat <= std::chrono::seconds(hbTimeout_);
}

WorkerManager::RegisterResult WorkerManager::registerWorker(const Message& reg,
                                                            std::shared_ptr<Connection> conn,
                                                            std::string& why) {
    WorkerInfo w;
    w.id       = reg.get("worker_id");
    w.hostname = reg.get("hostname");
    w.ip       = conn->peerAddress();
    w.cpuCores = static_cast<int>(reg.getInt("cpu_cores"));
    w.memoryMb = reg.getInt("memory_mb");
    w.capacity = static_cast<int>(reg.getInt("capacity", 1));
    if (w.capacity < 1) w.capacity = 1;
    if (w.capacity > 1024) w.capacity = 1024;
    w.threads = static_cast<int>(reg.getInt("threads", w.capacity));
    w.logicalCpus = static_cast<int>(reg.getInt("logical_cpus", 0));
    w.cpuAffinity = reg.get("cpu_affinity");
    w.affinityStatus = reg.get("affinity_status", "UNKNOWN");
    w.affinityDetail = reg.get("affinity_detail");
    w.lastHeartbeat = Clock::now();
    w.conn     = conn;

    std::shared_ptr<Connection> replaced;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = workers_.find(w.id);
        if (it != workers_.end() && healthyLocked(it->second)) {
            why = "worker id '" + w.id + "' is already connected";
            return RegisterResult::DUPLICATE_ID;               // IDs must be unique among live Workers
        }
        if (it != workers_.end()) replaced = it->second.conn;  // stale connection of the same Worker
        workers_[w.id] = w;
    }
    if (replaced) replaced->shutdown();

    if (!db_.upsertWorker(w.id, w.hostname, w.ip, w.cpuCores, w.memoryMb, w.capacity)) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = workers_.find(w.id);
        if (it != workers_.end() && it->second.conn == conn) it->second.conn.reset();
        why = "database error";
        return RegisterResult::DB_ERROR;
    }
    db_.exec("UPDATE workers SET pool_threads=$2::int, logical_cpus=$3::int, cpu_affinity=$4, affinity_status=$5, "
             "affinity_detail=$6 WHERE worker_id=$1",
             {w.id, std::to_string(w.threads), std::to_string(w.logicalCpus), w.cpuAffinity, w.affinityStatus, w.affinityDetail});
    return RegisterResult::OK;
}

void WorkerManager::onHeartbeat(const std::string& id, int reportedRunning) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = workers_.find(id);
        if (it == workers_.end() || !it->second.conn) return;
        it->second.lastHeartbeat = Clock::now();
        it->second.reportedRunning = reportedRunning;
    }
    db_.recordHeartbeat(id, reportedRunning);
}

void WorkerManager::onResourceUpdate(const std::string& id, const Database::ResourceRow& row) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = workers_.find(id);
        if (it == workers_.end() || !it->second.conn) return;
        it->second.cpu = row.cpu;
        it->second.ram = row.ram;
        it->second.disk = row.disk;
        it->second.metricsEver = true;
        it->second.reportedRunning = row.running;
        it->second.lastMetrics = it->second.lastHeartbeat = Clock::now();
    }
    db_.recordResources(id, row);
}

bool WorkerManager::onDisconnect(const std::string& id, const Connection* conn) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = workers_.find(id);
        if (it == workers_.end() || it->second.conn.get() != conn) return false;
        it->second.conn.reset();
        it->second.reportedRunning = 0;
    }
    failPending(id, "worker disconnected");
    db_.setWorkerStatus(id, "INACTIVE");
    return true;
}

std::vector<std::string> WorkerManager::expireStale() {
    std::vector<std::string> expired;
    std::vector<std::shared_ptr<Connection>> toClose;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& kv : workers_) {
            WorkerInfo& w = kv.second;
            if (w.conn && !healthyLocked(w)) {
                expired.push_back(w.id);
                toClose.push_back(w.conn);
                w.conn.reset();              // no longer reachable / schedulable
                w.reportedRunning = 0;
            }
        }
    }
    for (const auto& id : expired) {
        log("MASTER", "Worker " + id + " missed heartbeats -> marked INACTIVE");
        failPending(id, "worker heartbeat expired");
        db_.setWorkerStatus(id, "INACTIVE");
    }
    for (auto& c : toClose) c->shutdown();   // unblocks its reader thread
    return expired;
}

std::vector<WorkerInfo> WorkerManager::snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<WorkerInfo> out;
    for (const auto& kv : workers_) out.push_back(kv.second);
    return out;
}

bool WorkerManager::isHealthy(const std::string& id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = workers_.find(id);
    return it != workers_.end() && healthyLocked(it->second);
}

std::vector<std::string> WorkerManager::healthyIds() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> out;
    for (const auto& kv : workers_) if (healthyLocked(kv.second)) out.push_back(kv.first);
    return out;
}

std::shared_ptr<Connection> WorkerManager::connectionOf(const std::string& id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = workers_.find(id);
    return it == workers_.end() ? nullptr : it->second.conn;
}

void WorkerManager::disconnectAll() {
    std::vector<std::shared_ptr<Connection>> conns;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& kv : workers_) if (kv.second.conn) conns.push_back(kv.second.conn);
    }
    for (auto& c : conns) c->shutdown();
}

// ------------------------------------------------------------------ RPC ----

bool WorkerManager::call(const std::string& workerId, Message req, Message& reply, int timeoutMs) {
    auto conn = connectionOf(workerId);
    if (!conn || !isHealthy(workerId)) { reply = Message(); reply.set("ok", "0").set("error", "worker not reachable"); return false; }

    const std::string reqId = std::to_string(nextReq_++);
    auto p = std::make_shared<Pending>();
    p->workerId = workerId;
    {
        std::lock_guard<std::mutex> lock(rpcMutex_);
        pending_[reqId] = p;
    }
    req.set("req_id", reqId);
    bool sent = conn->send(req);
    bool got = false;
    if (sent) {
        std::unique_lock<std::mutex> lk(p->m);
        got = p->cv.wait_for(lk, std::chrono::milliseconds(timeoutMs), [&] { return p->done; });
    }
    {
        std::lock_guard<std::mutex> lock(rpcMutex_);
        pending_.erase(reqId);
    }
    if (!sent) { reply = Message(); reply.set("ok", "0").set("error", "send failed"); return false; }
    if (!got) { reply = Message(); reply.set("ok", "0").set("error", "timeout waiting for worker"); return false; }
    reply = p->reply;
    return reply.get("ok") == "1";
}

void WorkerManager::onReply(const Message& m) {
    std::shared_ptr<Pending> p;
    {
        std::lock_guard<std::mutex> lock(rpcMutex_);
        auto it = pending_.find(m.get("req_id"));
        if (it == pending_.end()) return;                // late reply for a request that timed out
        p = it->second;
    }
    std::lock_guard<std::mutex> lk(p->m);
    p->reply = m;
    p->done = true;
    p->cv.notify_all();
}

void WorkerManager::failPending(const std::string& workerId, const std::string& why) {
    std::vector<std::shared_ptr<Pending>> victims;
    {
        std::lock_guard<std::mutex> lock(rpcMutex_);
        for (auto& kv : pending_) if (kv.second->workerId == workerId) victims.push_back(kv.second);
    }
    for (auto& p : victims) {
        std::lock_guard<std::mutex> lk(p->m);
        if (p->done) continue;
        p->reply = Message(proto::BLOCK_REPLY);
        p->reply.set("ok", "0").set("error", why);
        p->done = true;
        p->cv.notify_all();
    }
}

}  // namespace nexus
