// TaskExecutor: REAL workloads. Nothing is faked - each task does genuine
// computation (or reads a genuine block from the Worker's disk) and returns a
// value that depends on that work.
// `scale` multiplies the amount of work (default 1 is roughly 1-3 seconds).
#pragma once

#include <string>

#include "../common/util.h"
#include "block_store.h"

namespace nexus {

struct TaskOutcome {
    bool ok = false;
    std::string result;      // short human-readable result, stored in PostgreSQL
    std::string data;        // bulk output (MapReduce intermediate data); may be empty
    std::string error;
    bool retryable = true;   // false for deterministic errors (bad parameters)
    double seconds = 0;      // wall-clock execution time
};

class TaskExecutor {
public:
    // `store` may be null for tasks that do not read blocks.
    static TaskOutcome run(const std::string& type, int scale, const Params& params,
                           const BlockStore* store);

    // Individual workloads (public so tests can call them directly).
    static std::string wordCount(int scale);
    static std::string primeCount(int scale);
    static std::string matrixMultiply(int scale);
    static std::string checksum(int scale);
    static std::string cpuBurn(int scale);
};

}  // namespace nexus
