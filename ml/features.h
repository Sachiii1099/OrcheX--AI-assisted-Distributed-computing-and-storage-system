// Feature engineering for the task-duration model.
//
// ONLY information that exists at scheduling time is used: what the task is, how
// big it is, and the state of the candidate Worker right now. Nothing that is
// known only after the task ran (its own duration, the result) can leak in.
// Per-task-type "historical mean duration" is deliberately NOT a feature: it is
// a function of the training targets and would leak them into the evaluation.
#pragma once

#include <cmath>
#include <string>
#include <vector>

#include "../common/protocol.h"

namespace nexus {
namespace ml {

struct TaskFeatures {
    std::string type;
    int scale = 1;
    long long inputBytes = 0;     // size of the data the task reads (0 for generated workloads)
    double cpu = 0, ram = 0;      // Worker CPU% / RAM% when the task was assigned
    int running = 0;              // tasks already running on that Worker
    int capacity = 1;
    int cores = 1;
};

inline std::vector<std::string> featureNames() {
    std::vector<std::string> n;
    for (const auto& t : proto::allTaskTypes()) n.push_back("is_" + t);
    for (const char* s : {"log2_scale", "log10_input_bytes", "worker_cpu", "worker_ram",
                          "worker_running", "worker_capacity", "worker_cores", "worker_occupancy"})
        n.push_back(s);
    return n;
}

inline std::vector<double> featureVector(const TaskFeatures& f) {
    std::vector<double> v;
    for (const auto& t : proto::allTaskTypes()) v.push_back(f.type == t ? 1.0 : 0.0);
    v.push_back(std::log2(static_cast<double>(f.scale < 1 ? 1 : f.scale)));
    v.push_back(std::log10(1.0 + static_cast<double>(f.inputBytes < 0 ? 0 : f.inputBytes)));
    v.push_back(f.cpu);
    v.push_back(f.ram);
    v.push_back(f.running);
    v.push_back(f.capacity);
    v.push_back(f.cores);
    v.push_back(static_cast<double>(f.running) / (f.capacity < 1 ? 1 : f.capacity));
    return v;
}

}  // namespace ml
}  // namespace nexus
