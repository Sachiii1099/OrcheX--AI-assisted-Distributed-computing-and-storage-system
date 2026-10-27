// CPU affinity for a Worker process (Linux sched_setaffinity).
//
// Three DIFFERENT settings control how much CPU a Worker may use - do not confuse them:
//   NEXUS_WORKER_CPU_AFFINITY  which logical CPUs the process may run on   (this file)
//   NEXUS_WORKER_THREADS       size of the executor thread pool            (worker/main.cpp)
//   NEXUS_WORKER_CAPACITY      max tasks the Master may assign at once     (worker/main.cpp)
//
// The result is verified by reading the affinity mask back (sched_getaffinity); a Worker is
// only reported as pinned when the kernel really reports the requested mask. Nothing is
// claimed on failure: the status says FAILED / PARTIAL and the reason is logged.
#pragma once

#include <string>
#include <vector>

namespace nexus {

struct AffinityResult {
    std::string requested;            // the raw setting ("none", "auto", "2", "0,2", "1-3")
    std::string status = "NOT_REQUESTED";   // NOT_REQUESTED | APPLIED | PARTIAL | FAILED
    std::vector<int> cpus;            // mask the process ACTUALLY has after the call
    std::vector<int> allowed;         // CPUs the process was allowed to use before (cgroup/taskset aware)
    std::string detail;               // human-readable explanation / warning
    bool pinned() const { return status == "APPLIED" || status == "PARTIAL"; }
};

// "0,2-3" -> {0,2,3}. Returns false (err) on a syntax error; ids are NOT range-checked here.
bool parseCpuList(const std::string& spec, std::vector<int>& out, std::string& err);
std::string formatCpuList(const std::vector<int>& cpus);       // {0,1,2,5} -> "0-2,5"
std::vector<int> currentAffinity();                            // sched_getaffinity(0)

// spec: ""/"none" (leave the OS to schedule), "auto" (one dedicated CPU chosen from the allowed
// set by `workerIndex`, wrapping if there are fewer CPUs than workers), or an explicit list.
AffinityResult applyCpuAffinity(const std::string& spec, int workerIndex);

// W1 -> 0, W2 -> 1, node-7 -> 6; ids without a trailing number -> -1.
int workerIndexFromId(const std::string& workerId);

}  // namespace nexus
