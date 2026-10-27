// WorkerManager: the Master's in-memory registry of Workers.
// It is the single place that knows who is alive, how loaded they are and
// which socket to use to reach them. State changes are mirrored to PostgreSQL.
//
// "Healthy" is always computed from the age of the last heartbeat - the stored
// ACTIVE/INACTIVE flag is never trusted on its own.
//
// It also provides a small request/response channel (call()) used by distributed
// storage: the Master sends BLOCK_* to a Worker and waits for the BLOCK_REPLY
// that the Worker's reader thread delivers through onReply().
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "../common/message.h"
#include "../database/database.h"

namespace nexus {

struct WorkerInfo {
    std::string id, hostname, ip;
    int cpuCores = 0;
    long long memoryMb = 0;
    int capacity = 1;                           // max tasks at once (NEXUS_WORKER_CAPACITY)
    int threads = 0;                            // executor thread-pool size
    int logicalCpus = 0;                        // CPUs the Worker process was allowed to use at start
    std::string cpuAffinity, affinityStatus = "UNKNOWN", affinityDetail;   // what the Worker reported
    double cpu = 0, ram = 0, disk = 0;          // latest reported usage, percent
    bool metricsEver = false;
    int reportedRunning = 0;                    // what the Worker itself reports
    std::chrono::steady_clock::time_point lastHeartbeat;
    std::chrono::steady_clock::time_point lastMetrics;
    std::shared_ptr<Connection> conn;
};

class WorkerManager {
public:
    enum class RegisterResult { OK, DUPLICATE_ID, DB_ERROR };

    WorkerManager(Database& db, int heartbeatTimeoutSec) : db_(db), hbTimeout_(heartbeatTimeoutSec) {}

    RegisterResult registerWorker(const Message& reg, std::shared_ptr<Connection> conn, std::string& why);
    void onHeartbeat(const std::string& id, int reportedRunning);
    void onResourceUpdate(const std::string& id, const Database::ResourceRow& row);
    // Called when a Worker's socket closes. Returns true if this connection was the
    // Worker's current one (a stale connection after re-register returns false).
    bool onDisconnect(const std::string& id, const Connection* conn);
    // Workers whose heartbeat is older than the timeout: marked INACTIVE and their
    // sockets closed. Returns their ids so tasks in flight can be recovered.
    std::vector<std::string> expireStale();

    std::vector<WorkerInfo> snapshot() const;                 // every known Worker
    bool isHealthy(const std::string& id) const;
    std::vector<std::string> healthyIds() const;
    std::shared_ptr<Connection> connectionOf(const std::string& id) const;
    int heartbeatTimeoutSec() const { return hbTimeout_; }
    void disconnectAll();

    // ---- request/response channel for storage operations ----
    // Sends `req` (a req_id is added) and waits for the BLOCK_REPLY. False on
    // timeout / disconnect (error text in reply "error").
    bool call(const std::string& workerId, Message req, Message& reply, int timeoutMs);
    void onReply(const Message& m);

private:
    struct Pending {
        std::string workerId;
        bool done = false;
        Message reply;
        std::mutex m;
        std::condition_variable cv;
    };
    bool healthyLocked(const WorkerInfo& w) const;
    void failPending(const std::string& workerId, const std::string& why);

    Database& db_;
    int hbTimeout_;
    mutable std::mutex mutex_;
    std::map<std::string, WorkerInfo> workers_;

    std::mutex rpcMutex_;
    std::map<std::string, std::shared_ptr<Pending>> pending_;
    std::atomic<unsigned long long> nextReq_{1};
};

}  // namespace nexus
