#include "ml_manager.h"

#include <chrono>

#include "../common/json.h"
#include "../common/log.h"
#include "../common/util.h"
#include "../ml/features.h"
#include "sql_util.h"

namespace nexus {

namespace {
const char* kPoolSql =
    "SELECT count(*) FROM execution_history WHERE status='COMPLETED' AND execution_time IS NOT NULL "
    "AND scale IS NOT NULL AND running_at_assign IS NOT NULL";
}

bool MlManager::loadFromDisk() {
    ml::DurationModel m;
    int runId = 0;
    // The configured type first, then the other one (an older model file stays usable).
    const std::string other = cfg_.modelType == "xgboost" ? "random_forest" : "xgboost";
    for (const std::string& path : {cfg_.modelPath, cfg_.pathFor(other)}) {
        if (ml::loadModelFile(path, m, runId)) {
            std::lock_guard<std::mutex> lock(mutex_);
            model_ = std::make_shared<const ml::DurationModel>(m);
            runId_ = runId;
            kind_ = m.kindName();
            log("ML", "Loaded " + kind_ + " duration model from '" + path + "' (training run " + std::to_string(runId) + ")");
            return true;
        }
    }
    log("ML", "No trained model at '" + cfg_.modelPath + "' - AI_ASSISTED will fall back to RESOURCE_AWARE");
    return false;
}

ml::TrainReport MlManager::train(const std::string& modelType) {
    std::lock_guard<std::mutex> one(trainMutex_);
    ml::TrainConfig cfg = cfg_;
    if (modelType == "xgboost" || modelType == "random_forest") {
        cfg.modelType = modelType;
        cfg.modelPath = cfg_.pathFor(modelType);
    }
    ml::DurationModel m;
    ml::TrainReport r = ml::trainFromDatabase(db_, cfg, m);
    {
        Database::Result pool = db_.exec(kPoolSql);
        std::lock_guard<std::mutex> lock(mutex_);
        if (pool.ok && !pool.empty()) poolAtLastTrain_ = pool.i(0, 0);
    }
    if (r.activated()) {
        std::lock_guard<std::mutex> lock(mutex_);
        model_ = std::make_shared<const ml::DurationModel>(m);
        runId_ = r.runId;
        kind_ = m.kindName();
    }
    log("ML", "Training run " + std::to_string(r.runId) + " [" + r.modelType + "]: " + r.status + " (" +
              std::to_string(r.nSamples) + " samples" +
              (r.nTest ? ", MAE " + fmtDouble(r.mae, 4) + " s vs baseline " + fmtDouble(r.baselineMae, 4) + " s" : "") +
              ") - " + r.message);
    return r;
}

void MlManager::startAutoRetrain(int everyNew, int pollSec) {
    if (everyNew <= 0 || autoRun_.exchange(true)) return;
    autoThread_ = std::thread([this, everyNew, pollSec]() {
        log("ML", "Auto-retrain enabled: every " + std::to_string(everyNew) + " new completed executions (background thread)");
        while (autoRun_) {
            for (int i = 0; i < pollSec * 10 && autoRun_; ++i) std::this_thread::sleep_for(std::chrono::milliseconds(100));
            if (!autoRun_) break;
            Database::Result pool = db_.exec(kPoolSql);
            if (!pool.ok || pool.empty()) continue;
            long long n = pool.i(0, 0), last;
            { std::lock_guard<std::mutex> lock(mutex_); last = poolAtLastTrain_; }
            if (n >= static_cast<long long>(cfg_.minSamples) && n - last >= everyNew) {
                log("ML", "Auto-retrain triggered (" + std::to_string(n - last) + " new executions)");
                train();
            }
        }
    });
}

void MlManager::stopAutoRetrain() {
    autoRun_ = false;
    if (autoThread_.joinable()) autoThread_.join();
}

bool MlManager::hasModel() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return model_ != nullptr;
}

std::string MlManager::modelKind() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return model_ ? kind_ : "none";
}

int MlManager::modelRunId() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return model_ ? runId_ : 0;
}

std::shared_ptr<Predictor> MlManager::predictor() const {
    std::shared_ptr<const ml::DurationModel> m;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        m = model_;
    }
    if (!m) return nullptr;
    return std::make_shared<Predictor>([m](const SchedTask& t, const SchedWorker& w, double& seconds) {
        ml::TaskFeatures f;
        f.type = t.type; f.scale = t.scale; f.inputBytes = t.inputBytes;
        f.cpu = w.cpu; f.ram = w.ram; f.running = w.running; f.capacity = w.capacity; f.cores = w.cores;
        std::vector<double> x = ml::featureVector(f);
        if (x.size() != m->featureCount()) return false;
        seconds = m->predict(x);
        return seconds >= 0;
    });
}

std::string MlManager::statusJson() {
    json::Obj o;
    o.add("model_loaded", hasModel()).add("model_run_id", modelRunId()).add("model_kind", modelKind())
     .add("configured_type", cfg_.modelType).add("model_path", cfg_.modelPath)
     .add("xgboost_available", ml::XgbRegressor::available())
     .add("xgboost_version", ml::XgbRegressor::libraryVersion())
     .add("min_samples", static_cast<long long>(cfg_.minSamples))
     .add("n_trees", cfg_.forest.nTrees).add("max_depth", cfg_.forest.maxDepth)
     .add("xgb_rounds", cfg_.xgb.rounds).add("xgb_max_depth", cfg_.xgb.maxDepth).add("xgb_eta", cfg_.xgb.eta)
     .add("auto_retrain", autoRun_.load());

    Database::Result pool = db_.exec(kPoolSql);
    o.add("training_pool_size", pool.ok && !pool.empty() ? pool.i(0, 0) : 0LL);

    const std::string cols = "run_id, status, model_type, n_samples, n_train, n_test, mae, rmse, baseline_mae, baseline_rmse, "
                             "n_trees, max_depth, hyperparams, compare_model, compare_mae, compare_rmse, message, "
                             NEXUS_TS("finished_at") " AS finished_at";
    Database::Result last = db_.exec("SELECT " + cols + " FROM ml_training_runs ORDER BY run_id DESC LIMIT 1");
    o.raw("latest_run", last.ok && !last.empty() ? last.rowJson(0) : "null");
    Database::Result lastGood = db_.exec("SELECT " + cols + " FROM ml_training_runs WHERE status='TRAINED' ORDER BY run_id DESC LIMIT 1");
    o.raw("latest_trained_run", lastGood.ok && !lastGood.empty() ? lastGood.rowJson(0) : "null");
    Database::Result runs = db_.exec("SELECT " + cols + " FROM ml_training_runs ORDER BY run_id DESC LIMIT 20");
    o.raw("runs", runs.ok ? runs.toJson() : "[]");

    // How good were the live predictions once the tasks actually finished?
    Database::Result live = db_.exec(
        "SELECT count(*), avg(abs(predicted_seconds-actual_seconds)), "
        "sqrt(avg((predicted_seconds-actual_seconds)^2)) FROM ml_predictions WHERE actual_seconds IS NOT NULL");
    if (live.ok && !live.empty() && live.i(0, 0) > 0)
        o.raw("live_accuracy", json::Obj().add("n", live.i(0, 0)).add("mae", live.d(0, 1)).add("rmse", live.d(0, 2)).build());
    else
        o.raw("live_accuracy", "null");
    Database::Result recent = db_.exec(
        "SELECT p.task_id, p.worker_id, p.predicted_seconds, p.actual_seconds, t.task_type, " NEXUS_TS("p.created_at") " AS created_at "
        "FROM ml_predictions p JOIN tasks t ON t.task_id=p.task_id ORDER BY p.prediction_id DESC LIMIT 20");
    o.raw("recent_predictions", recent.ok ? recent.toJson() : "[]");
    return o.build();
}

}  // namespace nexus
