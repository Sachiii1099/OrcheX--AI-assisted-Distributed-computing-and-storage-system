// TaskManager: owns the life cycle of every task on the Master side.
//
//   createJob/addTask  ->  PostgreSQL (PENDING)  ->  in-memory queue
//   dispatch thread:  Scheduler.order() / pick()  ->  atomic DB claim (PENDING->ASSIGNED
//                     + scheduling_decisions row)  ->  TASK_ASSIGN over TCP
//   Worker events:    TASK_STARTED / TASK_RESULT / TASK_FAILURE -> PostgreSQL + history
//   Failures:         retry on another Worker (up to max_attempts), or FAIL permanently
//
// State machine (tasks.status):
//   PENDING -> ASSIGNED -> RUNNING -> COMPLETED
//        ^________|___________|----> back to PENDING on retryable failure / lost Worker
//                              \---> FAILED when attempts are exhausted
// Every transition is a guarded UPDATE (... WHERE status = <expected>) so a task can
// never be assigned twice and a stale result from an old attempt is ignored.
#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "../common/message.h"
#include "../common/util.h"
#include "../database/database.h"
#include "ml_manager.h"
#include "scheduler.h"
#include "storage_manager.h"
#include "worker_manager.h"

namespace nexus {

struct TaskSpec {
    std::string type;
    int scale = 1;
    int priority = 5;
    Params params;
    long long jobId = 0;
    std::string role = "STANDALONE";     // STANDALONE | MAP | REDUCE
    int partition = -1;
    std::string blockId;                 // set => task must run on a Worker holding this block
    long long inputBytes = 0;
    int maxAttempts = 3;
};

// Emitted when a task of a MapReduce job finishes for good.
struct TaskEvent {
    long long taskId = 0, jobId = 0;
    std::string role, workerId, result, data, error;
    int partition = -1;
    bool success = false;
};

class TaskManager {
public:
    TaskManager(Database& db, WorkerManager& workers, Scheduler& scheduler, StorageManager& storage,
                MlManager& ml)
        : db_(db), workers_(workers), scheduler_(scheduler), storage_(storage), ml_(ml) {}
    ~TaskManager() { stop(); }

    void start();                         // starts the dispatch thread
    void stop();
    void recoverOnStart();                // re-queue work interrupted by a Master restart

    // Stand-alone task = one job with one task. Block tasks need params["block_id"].
    bool submit(const std::string& type, int scale, int priority, Params extra,
                std::string& err, long long* taskIdOut = nullptr);
    // Job container for multi-task jobs (MapReduce) and a task inside it.
    bool createJob(const std::string& jobType, int priority, const std::string& params,
                   long long datasetId, long long& jobId, std::string& err);
    bool addTask(const TaskSpec& spec, long long& taskId, std::string& err);
    void cancelJob(long long jobId, const std::string& reason);
    // PENDING tasks among `ids` are failed with `reason`; tasks already running finish normally.
    void cancelAll(const std::string& reason, const std::vector<long long>& ids);
    // While paused nothing is dispatched (used to submit a whole benchmark batch before scheduling starts).
    void pauseDispatch(bool paused) { paused_ = paused; if (!paused) wake(); }

    void wake();

    void onTaskStarted(const std::string& workerId, const Message& m);
    void onTaskResult(const std::string& workerId, const Message& m);
    void onTaskFailure(const std::string& workerId, const Message& m);
    // Worker vanished (socket closed / heartbeat expired): its in-flight tasks are retried elsewhere.
    void onWorkerLost(const std::string& workerId, const std::string& reason);

    void setEventCallback(std::function<void(const TaskEvent&)> cb) { callback_ = std::move(cb); }

    size_t pendingCount();
    size_t inFlightCount();
    static std::string label(long long taskId);   // 7 -> "TASK-007"
    static int maxScale(const std::string& type) { return type == "CPU_BURN" ? 100 : 20; }

private:
    struct Pending {
        TaskSpec spec;
        long long id = 0;
        int attempts = 0;
        std::set<std::string> excluded;     // Workers that already failed this task
    };
    struct InFlight {
        Pending task;
        std::string workerId, policy;
        int attempt = 0;
        double cpu = 0, ram = 0, predicted = -1;
        int runningAtAssign = 0, capacity = 1;
    };

    void dispatchLoop();
    void dispatchOnce();
    bool assign(const Pending& p, const Decision& d, const std::string& taskLabel);
    bool takeInFlight(long long taskId, const std::string& workerId, long long attempt, InFlight& out);
    Database::Stmt historyStmt(const InFlight& f, const char* status, double seconds, const std::string& error,
                               long long startMs, long long endMs, int concurrent) const;
    void handleFailure(InFlight f, const std::string& error, bool retryable, double seconds,
                       long long startMs, long long endMs, int concurrent);
    void emit(const InFlight& f, bool success, const std::string& result, const std::string& data,
              const std::string& error);

    Database& db_;
    WorkerManager& workers_;
    Scheduler& scheduler_;
    StorageManager& storage_;
    MlManager& ml_;
    std::function<void(const TaskEvent&)> callback_;

    std::mutex mutex_;                        // guards pending_ and inFlight_
    std::vector<Pending> pending_;
    std::map<long long, InFlight> inFlight_;

    std::mutex wakeMutex_;
    std::condition_variable wakeCv_;
    bool wakeFlag_ = false;
    std::atomic<bool> running_{false};
    std::atomic<bool> paused_{false};
    std::thread dispatcher_;
};

}  // namespace nexus
