// WorkerClient: connects to the Master, registers, then runs
//   - a heartbeat/resource thread (HEARTBEAT + RESOURCE_UPDATE every N s)
//   - a receive loop (main thread) that
//       * hands TASK_ASSIGN messages to a BOUNDED THREAD POOL (size = capacity),
//         so up to `capacity` tasks run truly in parallel on different CPU cores
//       * answers BLOCK_* storage requests directly (small, fast disk operations)
// Heartbeats keep flowing while tasks run because tasks never run on these threads.
#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "../common/message.h"
#include "../common/thread_pool.h"
#include "block_store.h"
#include "cpu_affinity.h"
#include "resource_monitor.h"

namespace nexus {

class WorkerClient {
public:
    struct Config {
        std::string workerId;
        std::string masterHost;
        int masterPort = 5050;
        int heartbeatSec = 3;
        int capacity = 1;              // max tasks the Master may assign at once (NEXUS_WORKER_CAPACITY)
        int threads = 0;               // executor thread-pool size (NEXUS_WORKER_THREADS; 0 = same as capacity)
        AffinityResult affinity;       // CPU pinning that was applied (or why not)
        std::string storageDir;        // block files live here
    };

    explicit WorkerClient(Config cfg);

    // One session: connect, register, serve until the connection drops or stop().
    // Returns 0 = stopped on request, 1 = could not connect/register, 2 = connection lost.
    int run();
    void stop();
    bool stopRequested() const { return stop_.load(); }

private:
    bool connectAndRegister();
    void heartbeatLoop(std::shared_ptr<Connection> conn);
    void executeTask(std::shared_ptr<Connection> conn, Message assign);
    void handleBlockRequest(Connection& conn, const Message& m);
    std::string tag() const { return "WORKER " + cfg_.workerId; }
    int poolThreads() const { return cfg_.threads > 0 ? cfg_.threads : cfg_.capacity; }

    Config cfg_;
    ResourceMonitor monitor_;
    BlockStore store_;
    std::shared_ptr<Connection> conn_;
    std::atomic<bool> stop_{false};
    std::atomic<bool> session_{false};
    std::atomic<int> accepted_{0};     // tasks received and not yet finished (queued + executing)
    std::atomic<int> executing_{0};    // tasks executing right now
    std::unique_ptr<ThreadPool> pool_;
    std::mutex wakeMutex_;
    std::condition_variable wake_;
};

}  // namespace nexus
