// REST/JSON API + static dashboard files, served by the Master's HTTP server.
// Every endpoint reads PostgreSQL or calls the real Master components - no
// hard-coded data. See README section "HTTP API" for the full list.
#pragma once

#include <string>

#include "../database/database.h"
#include "benchmark.h"
#include "http_server.h"
#include "mapreduce.h"
#include "ml_manager.h"
#include "scheduler.h"
#include "storage_manager.h"
#include "task_manager.h"
#include "worker_manager.h"

namespace nexus {

struct AppContext {
    Database& db;
    WorkerManager& workers;
    Scheduler& scheduler;
    TaskManager& tasks;
    StorageManager& storage;
    MapReduce& mr;
    MlManager& ml;
    BenchmarkRunner& bench;
    std::string dashboardDir;
    long long startedMs = 0;
    size_t defaultBlockSize = 256 * 1024;
    int defaultReplication = 2;
};

void registerApi(HttpServer& http, AppContext& ctx);

// Changes the active scheduling policy at run time and persists it (scheduler_config).
bool applyPolicy(Database& db, Scheduler& scheduler, const std::string& name, std::string& err);
// Loads the persisted policy (or NEXUS_SCHED_POLICY / default) at start-up.
void loadPolicy(Database& db, Scheduler& scheduler);

}  // namespace nexus
