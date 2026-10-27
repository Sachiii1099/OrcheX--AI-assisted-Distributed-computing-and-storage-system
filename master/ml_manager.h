// MlManager: owns the active task-duration model inside the Master.
//  - loads the persisted model at start-up (if a valid file exists)
//  - train() runs the real pipeline on PostgreSQL history and hot-swaps the model
//    only when it beat the baseline on the held-out split
//  - predictor() is what the AI_ASSISTED scheduling policy calls; it is nullptr
//    when no trained model exists, which triggers the resource-aware fallback.
#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "../database/database.h"
#include "../ml/duration_model.h"
#include "../ml/trainer.h"
#include "scheduler.h"

namespace nexus {

class MlManager {
public:
    MlManager(Database& db, ml::TrainConfig cfg) : db_(db), cfg_(std::move(cfg)) {}

    bool loadFromDisk();
    ml::TrainReport train(const std::string& modelType = "");   // "" = configured default
    std::string modelKind() const;                    // xgboost | random_forest | none
    // Background retraining: every `everyNew` newly completed executions (0 = off). Never blocks task execution.
    void startAutoRetrain(int everyNew, int pollSec = 10);
    void stopAutoRetrain();
    std::shared_ptr<Predictor> predictor() const;     // nullptr = no usable model
    bool hasModel() const;
    int modelRunId() const;
    std::string statusJson();

private:
    Database& db_;
    ml::TrainConfig cfg_;
    mutable std::mutex mutex_;
    std::shared_ptr<const ml::DurationModel> model_;
    int runId_ = 0;
    std::string kind_ = "none";
    std::atomic<bool> autoRun_{false};
    std::thread autoThread_;
    long long poolAtLastTrain_ = 0;
    std::mutex trainMutex_;                           // one training run at a time
};

}  // namespace nexus
