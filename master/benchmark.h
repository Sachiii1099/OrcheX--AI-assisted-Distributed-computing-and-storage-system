// BenchmarkRunner: runs the SAME deterministic workload once per scheduling policy on the live
// cluster and stores MEASURED results in PostgreSQL (benchmark_runs / benchmark_results).
//
// Fairness rules (also documented in docs/SCHEDULING_ALGORITHMS.md):
//   * the workload (task types, scales, priorities) is generated from a seed, so every policy
//     receives an identical task list;
//   * before each policy starts the cluster must be idle (no pending / in-flight tasks);
//   * all tasks of a policy are submitted back-to-back, so a queue forms and the policy decides;
//   * with repeats > 1 the policy order is rotated each repeat to spread order/cache effects;
//   * nothing is estimated: every number comes from tasks / execution_history / ml_predictions.
// Only one benchmark runs at a time; the original policy is restored afterwards.
#pragma once

#include <atomic>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "../database/database.h"
#include "ml_manager.h"
#include "scheduler.h"
#include "task_manager.h"
#include "worker_manager.h"

namespace nexus {

struct BenchmarkSpec {
    std::string name;
    std::vector<std::string> policies;    // normalised policy names, execution order
    std::string type = "MIXED";           // MIXED | one user-submittable task type
    int count = 24;                       // tasks per policy run
    int scaleMin = 1, scaleMax = 3;       // task scale drawn uniformly per task (seeded)
    std::string priorities = "varied";    // varied (0..9, seeded) | uniform (all 5)
    unsigned seed = 1;
    int repeats = 1;
    long long datasetId = 0;              // block workloads: tasks cycle over this dataset's blocks
    int timeoutSec = 180;                 // per policy run
    bool retrainBefore = false;           // train the ML model on current history first
    std::string modelType;                // "" = configured, else xgboost | random_forest
};

class BenchmarkRunner {
public:
    BenchmarkRunner(Database& db, TaskManager& tasks, Scheduler& scheduler, WorkerManager& workers, MlManager& ml)
        : db_(db), tasks_(tasks), scheduler_(scheduler), workers_(workers), ml_(ml) {}
    ~BenchmarkRunner() { cancel(); join(); }

    // Parses the request fields into a spec (validates everything). False + err on bad input.
    static bool parseSpec(const std::string& policies, const std::string& type, int count, int scaleMin,
                          int scaleMax, const std::string& priorities, long long seed, int repeats,
                          long long datasetId, int timeoutSec, BenchmarkSpec& out, std::string& err);

    // Marks benchmarks left RUNNING by a previous Master process as FAILED.
    void recoverOnStart();
    bool start(const BenchmarkSpec& spec, long long& benchmarkId, std::string& err);
    void cancel();
    bool running() const { return running_.load(); }
    long long currentId() const { return currentId_.load(); }
    void join();

    std::string listJson();
    std::string getJson(long long id);

    // Deterministic workload (exposed for unit tests): the i-th task of a spec.
    struct PlannedTask { std::string type; int scale = 1; int priority = 5; };
    static std::vector<PlannedTask> plan(const BenchmarkSpec& spec);

private:
    void runAll(BenchmarkSpec spec, long long id);
    bool runOne(const BenchmarkSpec& spec, long long id, const std::string& policy, int repeat, std::string& err);
    bool waitIdle(int seconds);
    void finish(long long id, const std::string& status, const std::string& note);

    Database& db_;
    TaskManager& tasks_;
    Scheduler& scheduler_;
    WorkerManager& workers_;
    MlManager& ml_;
    std::atomic<bool> running_{false};
    std::atomic<bool> cancel_{false};
    std::atomic<long long> currentId_{0};
    std::thread thread_;
    std::mutex startMutex_;
};

}  // namespace nexus
