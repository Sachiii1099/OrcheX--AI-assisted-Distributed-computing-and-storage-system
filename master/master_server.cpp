#include "master_server.h"

#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cmath>
#include <cstdio>

#include "../common/config.h"
#include "../common/log.h"
#include "../common/protocol.h"
#include "../common/util.h"

namespace nexus {

namespace {
// Set NEXUS_QUIET_HEARTBEAT=1 to hide the per-heartbeat lines.
bool quietHeartbeat() {
    static const bool q = envOr("NEXUS_QUIET_HEARTBEAT", "1") == "1";
    return q;
}

bool validPercent(double v) { return std::isfinite(v) && v >= 0.0 && v <= 100.0; }
}  // namespace

bool MasterServer::start(std::string& err) {
    // NEXUS_MASTER_BIND: interface for the Worker port (0.0.0.0 = every interface, needed for
    // Workers on other computers; 127.0.0.1 = this computer only).
    listenFd_ = createListener(port_, err, envOr("NEXUS_MASTER_BIND", "0.0.0.0"));
    if (listenFd_ < 0) return false;
    running_ = true;
    acceptThread_  = std::thread(&MasterServer::acceptLoop, this);
    monitorThread_ = std::thread(&MasterServer::monitorLoop, this);
    return true;
}

void MasterServer::stop() {
    if (!running_.exchange(false)) return;
    workers_.disconnectAll();                 // wake every blocked reader thread
    if (acceptThread_.joinable()) acceptThread_.join();
    if (monitorThread_.joinable()) monitorThread_.join();
    std::vector<std::thread> threads;
    {
        std::lock_guard<std::mutex> lock(threadsMutex_);
        threads.swap(connThreads_);
    }
    for (auto& t : threads) if (t.joinable()) t.join();
    if (listenFd_ >= 0) { ::close(listenFd_); listenFd_ = -1; }
}

void MasterServer::acceptLoop() {
    while (running_) {
        pollfd p;
        p.fd = listenFd_; p.events = POLLIN; p.revents = 0;
        if (::poll(&p, 1, 500) <= 0) continue;     // timeout lets us notice shutdown

        int fd = ::accept(listenFd_, nullptr, nullptr);
        if (fd < 0) continue;
        auto conn = std::make_shared<Connection>(fd);
        std::lock_guard<std::mutex> lock(threadsMutex_);
        connThreads_.emplace_back(&MasterServer::handleConnection, this, conn);
    }
}

// Failure detection: a Worker that stops heartbeating is marked INACTIVE, its socket is
// closed, and every task that was running on it goes back to the queue.
void MasterServer::monitorLoop() {
    while (running_) {
        for (int i = 0; i < 5 && running_; ++i) std::this_thread::sleep_for(std::chrono::milliseconds(100));
        for (const auto& id : workers_.expireStale()) tasks_.onWorkerLost(id, "worker heartbeat expired");
    }
}

void MasterServer::handleConnection(std::shared_ptr<Connection> conn) {
    // The first message on every connection must be REGISTER_WORKER.
    Message reg;
    if (conn->receive(reg, 10000) != 1 || reg.type() != proto::REGISTER_WORKER ||
        !isSafeId(reg.get("worker_id"), 32)) {
        log("MASTER", "Rejected connection from " + conn->peerAddress() + " (no valid registration)");
        return;
    }
    const std::string id = reg.get("worker_id");
    log("MASTER", "Worker registration received: " + id);

    std::string why;
    WorkerManager::RegisterResult rr = workers_.registerWorker(reg, conn, why);
    if (rr != WorkerManager::RegisterResult::OK) {
        Message nack(proto::REGISTER_ACK);
        nack.set("status", "REJECTED").set("reason", why);
        conn->send(nack);
        log("MASTER", "Registration of " + id + " rejected: " + why);
        return;
    }
    Message ack(proto::REGISTER_ACK);
    ack.set("status", "OK").set("worker_id", id).setInt("heartbeat_sec", heartbeatSec_);
    conn->send(ack);
    log("MASTER", "Worker " + id + " registered successfully (" + reg.get("hostname") + ", " +
                  reg.get("cpu_cores") + " cores, " + reg.get("memory_mb") + " MB RAM, capacity " +
                  reg.get("capacity", "1") + " concurrent tasks)");
    tasks_.wake();

    Message m;
    while (running_) {
        int r = conn->receive(m, 1000);
        if (r < 0) break;
        if (r == 0) continue;

        const std::string& t = m.type();
        if (t == proto::HEARTBEAT) {
            workers_.onHeartbeat(id, static_cast<int>(m.getInt("running")));
            if (!quietHeartbeat()) log("MASTER", "Heartbeat received from " + id);
        } else if (t == proto::RESOURCE_UPDATE) {
            Database::ResourceRow row;
            row.cpu = m.getDouble("cpu", -1); row.ram = m.getDouble("ram", -1); row.disk = m.getDouble("disk", -1);
            if (!validPercent(row.cpu) || !validPercent(row.ram) || !validPercent(row.disk)) {
                log("MASTER", "Ignoring invalid RESOURCE_UPDATE from " + id + " (values out of range)");
                continue;                         // metrics stay stale => resource-aware policies avoid this Worker
            }
            row.load1 = m.getDouble("load1"); row.rssMb = m.getDouble("rss_mb");
            row.ramAvailMb = m.getInt("ram_avail_mb"); row.storageBytes = m.getInt("storage_bytes");
            row.running = static_cast<int>(m.getInt("running"));
            workers_.onResourceUpdate(id, row);
            if (!quietHeartbeat()) {
                char buf[96];
                std::snprintf(buf, sizeof buf, "Resources from %s: CPU %.0f%% | RAM %.0f%% | DISK %.0f%%",
                              id.c_str(), row.cpu, row.ram, row.disk);
                log("MASTER", buf);
            }
        } else if (t == proto::TASK_STARTED) {
            tasks_.onTaskStarted(id, m);
        } else if (t == proto::TASK_RESULT) {
            tasks_.onTaskResult(id, m);
        } else if (t == proto::TASK_FAILURE) {
            tasks_.onTaskFailure(id, m);
        } else if (t == proto::BLOCK_REPLY) {
            workers_.onReply(m);
        } else {
            log("MASTER", "Unknown message type '" + t + "' from " + id);
        }
    }

    if (workers_.onDisconnect(id, conn.get())) {
        log("MASTER", "Worker " + id + " disconnected -> marked INACTIVE");
        tasks_.onWorkerLost(id, "worker disconnected");
    }
}

}  // namespace nexus
