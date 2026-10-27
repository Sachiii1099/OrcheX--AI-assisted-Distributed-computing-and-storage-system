// MasterServer: the TCP front door. Accepts Worker connections and runs one
// thread per connection that reads that Worker's messages.
#pragma once

#include <atomic>
#include <mutex>
#include <thread>
#include <vector>

#include "../common/message.h"
#include "task_manager.h"
#include "worker_manager.h"

namespace nexus {

class MasterServer {
public:
    MasterServer(int port, int heartbeatSec, WorkerManager& workers, TaskManager& tasks)
        : port_(port), heartbeatSec_(heartbeatSec), workers_(workers), tasks_(tasks) {}
    ~MasterServer() { stop(); }

    bool start(std::string& err);   // socket() bind() listen() + background threads
    void stop();

private:
    void acceptLoop();
    void monitorLoop();                                     // heartbeat timeout checker
    void handleConnection(std::shared_ptr<Connection> conn);

    int port_, heartbeatSec_;
    int listenFd_ = -1;
    WorkerManager& workers_;
    TaskManager& tasks_;
    std::atomic<bool> running_{false};
    std::thread acceptThread_, monitorThread_;
    std::mutex threadsMutex_;
    std::vector<std::thread> connThreads_;
};

}  // namespace nexus
