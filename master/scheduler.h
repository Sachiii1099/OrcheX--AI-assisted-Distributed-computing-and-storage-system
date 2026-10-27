// Scheduler: pure decision logic (no sockets, no database) so every policy can be
// unit-tested. TaskManager feeds it snapshots and applies its decisions.
//
// Two separate questions are answered per policy:
//   order()  - in which order are PENDING tasks considered?
//   pick()   - which eligible Worker runs the task?
//
//  policy          order                                     worker choice
//  FCFS            submission order (task id)                first eligible Worker (lowest id)
//  ROUND_ROBIN     submission order                          next eligible Worker after the last one used
//  PRIORITY        priority desc, then submission order      least-occupied eligible Worker (running/capacity)
//  RESOURCE_AWARE  submission order                          lowest resource score (see resourceScore)
//  AI_ASSISTED     priority desc, then submission order      lowest PREDICTED duration; falls back to
//                                                            RESOURCE_AWARE if no usable model
// All policies are NON-preemptive: a running task is never interrupted.
#pragma once

#include <functional>
#include <mutex>
#include <set>
#include <string>
#include <vector>

namespace nexus {

enum class Policy { FCFS, ROUND_ROBIN, PRIORITY, RESOURCE_AWARE, AI_ASSISTED };

const char* policyName(Policy p);
// Accepts FCFS, ROUND_ROBIN (RR), PRIORITY, RESOURCE_AWARE (LEAST_LOADED), AI_ASSISTED (AI); case-insensitive.
bool parsePolicy(const std::string& s, Policy& out);

struct SchedTask {
    long long id = 0;                 // also the submission sequence (ids only grow)
    int priority = 5;                 // 0..9, higher = more urgent
    std::string type;
    int scale = 1;
    long long inputBytes = 0;
    bool localityRequired = false;    // task reads a block: only replica holders may run it
    std::set<std::string> localWorkers;   // Workers holding a healthy replica of its block
    std::set<std::string> excluded;       // Workers that already failed this task (avoided if possible)
};

struct SchedWorker {
    std::string id;
    bool connected = false;           // has a live TCP connection
    double hbAgeSec = 1e9;            // seconds since the last heartbeat / resource update
    double metricsAgeSec = -1;        // seconds since the last RESOURCE_UPDATE (-1 = never)
    double cpu = 0, ram = 0, disk = 0;  // percent
    int running = 0;                  // tasks the Master has in flight on this Worker
    int capacity = 1;                 // concurrent task limit of the Worker
    int cores = 1;
};

struct Candidate {
    std::string worker;
    bool eligible = false;
    std::string note;                 // why it was rejected / how it scored
    double score = 0;
    double predicted = -1;            // seconds, -1 = none
};

struct Decision {
    bool found = false;
    std::string workerId;
    std::string requestedPolicy;      // the active policy
    std::string effectivePolicy;      // differs from requested when AI falls back
    bool fallback = false;
    double score = 0;
    double predictedSeconds = -1;
    std::string reason;
    std::vector<Candidate> candidates;
    std::string candidatesJson;       // metrics of every Worker at decision time
    // snapshot of the chosen Worker (stored for ML features / audit)
    double cpu = 0, ram = 0;
    int running = 0, capacity = 1, cores = 1;
};

// Fills `seconds` and returns true when a prediction is available.
using Predictor = std::function<bool(const SchedTask&, const SchedWorker&, double& seconds)>;

class Scheduler {
public:
    explicit Scheduler(int heartbeatTimeoutSec = 9, double metricsStaleSec = 9)
        : hbTimeout_(heartbeatTimeoutSec), staleSec_(metricsStaleSec) {}

    void setPolicy(Policy p);
    Policy policy() const;
    void configure(int heartbeatTimeoutSec, double metricsStaleSec);

    // Indices into `tasks` in the order they should be dispatched.
    std::vector<size_t> order(const std::vector<SchedTask>& tasks) const;

    // Chooses a Worker for `task`. Mutates the Round Robin cursor when it returns found.
    Decision pick(const SchedTask& task, const std::vector<SchedWorker>& workers,
                  const Predictor* predictor = nullptr);

    // ---- building blocks (public for tests / documentation) ----
    bool isHealthy(const SchedWorker& w) const;          // connected && heartbeat fresh
    bool metricsUsable(const SchedWorker& w) const;      // finite, in range, recent
    // Lower is better; every term is normalised to 0..1:
    //   0.35*cpu/100 + 0.20*ram/100 + 0.05*disk/100 + 0.30*running/capacity
    //   + 0.10*min(1, heartbeatAge/timeout)  - 0.15 if the Worker holds the task's data
    double resourceScore(const SchedWorker& w, bool hasLocalData) const;

private:
    Decision pickResourceAware(const SchedTask& t, const std::vector<SchedWorker>& ws,
                               const std::vector<int>& eligible, std::vector<Candidate>& cands,
                               const char* label) const;

    mutable std::mutex mutex_;
    Policy policy_ = Policy::RESOURCE_AWARE;
    std::string lastRrWorker_;        // Round Robin cursor
    int hbTimeout_;
    double staleSec_;
};

}  // namespace nexus
