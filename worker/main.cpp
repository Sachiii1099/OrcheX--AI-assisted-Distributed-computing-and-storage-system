// NEXUS Worker.
//   Usage:  ./worker <WORKER_ID> [master_host] [master_port] [capacity]
//   Environment: NEXUS_MASTER_HOST / NEXUS_MASTER_PORT / NEXUS_HEARTBEAT_SEC
//                NEXUS_WORKER_CPU_AFFINITY  none | auto | CPU list ("2", "0,2", "1-3") - which logical CPUs this process may use
//                NEXUS_WORKER_CAPACITY  max tasks the Master may assign at once (default: pinned CPUs, else logical CPUs)
//                NEXUS_WORKER_THREADS   executor thread-pool size (default: = capacity)
//                NEXUS_WORKER_STORAGE   parent directory of the block store (default ./worker_data)
//                NEXUS_WORKER_RECONNECT 1 = keep reconnecting if the Master goes away (default 1)
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <thread>

#include "../common/config.h"
#include "../common/log.h"
#include "../common/protocol.h"
#include "../common/util.h"
#include "cpu_affinity.h"
#include "resource_monitor.h"
#include "worker_client.h"

using namespace nexus;

namespace {
WorkerClient* g_client = nullptr;
void onSignal(int) { if (g_client) g_client->stop(); }
}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: %s <WORKER_ID> [master_host] [master_port] [capacity]\n"
                             "  (defaults come from NEXUS_MASTER_HOST / NEXUS_MASTER_PORT / NEXUS_WORKER_CAPACITY)\n",
                     argv[0]);
        return 2;
    }
    if (!ResourceMonitor::supported()) {
        std::fprintf(stderr, "NEXUS Worker needs Linux (/proc resource monitoring). On Windows run it inside WSL.\n");
        return 2;
    }
    std::signal(SIGPIPE, SIG_IGN);

    WorkerClient::Config cfg;
    cfg.workerId = argv[1];
    if (!isSafeId(cfg.workerId, 32)) {
        std::fprintf(stderr, "WORKER_ID must be 1-32 characters of [A-Za-z0-9_-] (it names the storage directory)\n");
        return 2;
    }
    cfg.masterHost   = argc > 2 ? argv[2] : envOr("NEXUS_MASTER_HOST", "127.0.0.1");
    cfg.masterPort   = argc > 3 ? std::atoi(argv[3]) : envIntOr("NEXUS_MASTER_PORT", proto::DEFAULT_MASTER_PORT);
    cfg.heartbeatSec = envIntOr("NEXUS_HEARTBEAT_SEC", proto::DEFAULT_HEARTBEAT_SEC);

    // 1. CPU affinity FIRST, before any thread exists, so every thread inherits the mask.
    int widx = envIntOr("NEXUS_WORKER_INDEX", workerIndexFromId(cfg.workerId));
    cfg.affinity = applyCpuAffinity(envOr("NEXUS_WORKER_CPU_AFFINITY", "none"), widx);
    if (cfg.affinity.status == "FAILED" || cfg.affinity.status == "PARTIAL")
        log("WORKER " + cfg.workerId, "CPU affinity " + cfg.affinity.status + ": " + cfg.affinity.detail);
    else
        log("WORKER " + cfg.workerId, "CPU affinity " + cfg.affinity.status + ": " + cfg.affinity.detail);

    // 2. capacity (tasks at once) and threads (pool size) are separate from affinity (which CPUs).
    unsigned hw = std::thread::hardware_concurrency();
    int usable = static_cast<int>(cfg.affinity.cpus.empty() ? (hw ? hw : 1) : cfg.affinity.cpus.size());
    int capacity = argc > 4 ? std::atoi(argv[4]) : envIntOr("NEXUS_WORKER_CAPACITY", usable);
    if (capacity < 1) capacity = 1;
    if (capacity > 256) capacity = 256;
    cfg.capacity = capacity;
    cfg.threads = envIntOr("NEXUS_WORKER_THREADS", 0);
    if (cfg.threads < 0 || cfg.threads > 256) cfg.threads = 0;
    if (capacity > usable)
        log("WORKER " + cfg.workerId, "WARNING: capacity " + std::to_string(capacity) + " > " + std::to_string(usable) +
                                      " usable CPU(s): CPU-bound tasks will time-share (oversubscription)");
    if (cfg.threads > 0 && cfg.threads < capacity)
        log("WORKER " + cfg.workerId, "WARNING: NEXUS_WORKER_THREADS " + std::to_string(cfg.threads) + " < capacity " +
                                      std::to_string(capacity) + ": accepted tasks may wait for a free thread");
    cfg.storageDir = envOr("NEXUS_WORKER_STORAGE", "worker_data") + "/" + cfg.workerId;

    WorkerClient client(cfg);
    g_client = &client;
    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    const bool reconnect = envIntOr("NEXUS_WORKER_RECONNECT", 1) != 0;
    int rc = 0;
    for (;;) {
        rc = client.run();
        if (client.stopRequested() || rc == 0) break;
        if (rc == 1 && !reconnect) break;
        if (!reconnect) break;
        log("WORKER " + cfg.workerId, "Will reconnect in 2 s...");
        for (int i = 0; i < 20 && !client.stopRequested(); ++i) std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (client.stopRequested()) break;
    }
    return rc == 2 ? 0 : rc;
}
