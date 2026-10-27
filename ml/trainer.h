// Training / evaluation pipeline for the task-duration model.
//   1. read REAL completed executions from PostgreSQL (execution_history)
//   2. seeded shuffle, hold out a test split (default 20 %)
//   3. fit the model (XGBoost by default, Random Forest optional) on the TRAIN split only
//   4. report MAE / RMSE on the held-out TEST split next to a trivial baseline
//      ("always predict the training mean"), so the numbers can be judged. The other
//      algorithm is fitted on the same split and reported as a labelled comparison.
//   5. persist the model file + a row in ml_training_runs
// With too little history nothing is trained and the run is recorded as
// INSUFFICIENT_DATA - no sample or metric is ever invented.
#pragma once

#include <string>
#include <vector>

#include "../database/database.h"
#include "duration_model.h"
#include "random_forest.h"
#include "xgb_model.h"

namespace nexus {
namespace ml {

struct TrainConfig {
    size_t minSamples = 30;           // below this: INSUFFICIENT_DATA
    double testFraction = 0.2;
    std::string modelType = "xgboost";   // "xgboost" | "random_forest"
    ForestParams forest;              // used for random_forest (and for the comparison fit)
    XgbParams xgb;                    // used for xgboost
    uint64_t splitSeed = 7;
    std::string modelPath;            // where to save the model ("" = do not save)
    static TrainConfig fromEnv();
    // models/duration_<type>.model unless NEXUS_ML_MODEL overrides it
    std::string pathFor(const std::string& type) const;
};

struct TrainReport {
    // TRAINED | NOT_ACTIVATED (trained but not better than the baseline) | INSUFFICIENT_DATA | FAILED
    std::string status = "FAILED";
    int runId = 0;
    int nSamples = 0, nTrain = 0, nTest = 0;
    double mae = 0, rmse = 0, baselineMae = 0, baselineRmse = 0;
    std::string modelType;            // the model that was trained: xgboost | random_forest
    std::string compareModel;         // the other algorithm fitted on the SAME split ("" = none)
    double compareMae = 0, compareRmse = 0;
    std::string hyperparams;          // human-readable, stored in ml_training_runs
    std::string message;
    bool activated() const { return status == "TRAINED"; }
};

// Pure pipeline (used by the unit tests and by trainFromDatabase).
TrainReport trainAndEvaluate(const std::vector<std::vector<double>>& X, const std::vector<double>& y,
                             const TrainConfig& cfg, DurationModel& modelOut);

// Loads history from PostgreSQL, trains, saves the model file and records the run.
TrainReport trainFromDatabase(Database& db, const TrainConfig& cfg, DurationModel& modelOut);

// Model file = "RUN <id>\n" + model text (NEXUSXGB... or NEXUSRF...). Returns false if missing / corrupt.
bool loadModelFile(const std::string& path, DurationModel& out, int& runId);
bool saveModelFile(const std::string& path, const DurationModel& model, int runId, std::string& err);

}  // namespace ml
}  // namespace nexus
