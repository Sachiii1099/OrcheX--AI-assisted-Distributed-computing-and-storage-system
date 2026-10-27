#include "trainer.h"

#include <sys/stat.h>

#include <algorithm>
#include <cstdio>
#include <numeric>

#include "../common/config.h"
#include "../common/log.h"
#include "../common/util.h"
#include "features.h"

namespace nexus {
namespace ml {

TrainConfig TrainConfig::fromEnv() {
    TrainConfig c;
    c.minSamples = static_cast<size_t>(envIntOr("NEXUS_ML_MIN_SAMPLES", 30));
    c.forest.nTrees = envIntOr("NEXUS_ML_TREES", 60);
    c.forest.maxDepth = envIntOr("NEXUS_ML_MAX_DEPTH", 12);
    c.xgb.rounds = envIntOr("NEXUS_ML_XGB_ROUNDS", 150);
    c.xgb.maxDepth = envIntOr("NEXUS_ML_XGB_DEPTH", 4);
    try { c.xgb.eta = std::stod(envOr("NEXUS_ML_XGB_ETA", "0.1")); } catch (...) {}
    std::string t = envOr("NEXUS_ML_MODEL_TYPE", "xgboost");
    for (auto& ch : t) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    c.modelType = (t == "random_forest" || t == "rf") ? "random_forest" : "xgboost";
    c.modelPath = c.pathFor(c.modelType);
    return c;
}

std::string TrainConfig::pathFor(const std::string& type) const {
    const std::string o = envOr("NEXUS_ML_MODEL", "");
    if (!o.empty()) return o;
    return "models/duration_" + std::string(type == "random_forest" ? "rf" : "xgb") + ".model";
}

namespace {

std::string describe(const TrainConfig& cfg, const std::string& type) {
    if (type == "xgboost")
        return "xgboost rounds=" + std::to_string(cfg.xgb.rounds) + " max_depth=" + std::to_string(cfg.xgb.maxDepth) +
               " eta=" + fmtDouble(cfg.xgb.eta, 3) + " objective=reg:squarederror tree_method=hist seed=" +
               std::to_string(cfg.xgb.seed) + " lib=" + XgbRegressor::libraryVersion();
    return "random_forest trees=" + std::to_string(cfg.forest.nTrees) + " max_depth=" + std::to_string(cfg.forest.maxDepth) +
           " seed=" + std::to_string(cfg.forest.seed);
}

bool fitModel(const std::string& type, const TrainConfig& cfg, const std::vector<std::vector<double>>& X,
              const std::vector<double>& y, DurationModel& out, std::string& err) {
    DurationModel m;
    if (type == "xgboost") {
        m.xgb = std::make_shared<XgbRegressor>();
        if (!m.xgb->fit(X, y, cfg.xgb, err)) return false;
    } else {
        m.rf = std::make_shared<RandomForest>();
        m.rf->fit(X, y, cfg.forest);
        if (!m.rf->trained()) { err = "training produced no trees"; return false; }
    }
    out = m;
    return true;
}

void score(const DurationModel& m, const std::vector<std::vector<double>>& X, const std::vector<double>& y,
           double& mae, double& rmse) {
    std::vector<double> pred;
    for (const auto& x : X) pred.push_back(m.predict(x));
    mae = meanAbsError(pred, y);
    rmse = rootMeanSquaredError(pred, y);
}

}  // namespace

TrainReport trainAndEvaluate(const std::vector<std::vector<double>>& X, const std::vector<double>& y,
                             const TrainConfig& cfg, DurationModel& modelOut) {
    TrainReport r;
    r.modelType = cfg.modelType;
    r.hyperparams = describe(cfg, cfg.modelType);
    r.nSamples = static_cast<int>(y.size());
    if (X.size() != y.size()) { r.message = "feature/target size mismatch"; return r; }
    if (y.size() < cfg.minSamples || y.size() < 10) {
        r.status = "INSUFFICIENT_DATA";
        r.message = "need at least " + std::to_string(std::max<size_t>(cfg.minSamples, 10)) +
                    " completed executions, have " + std::to_string(y.size()) +
                    ". Run more tasks, then retrain. No model was created.";
        return r;
    }
    if (cfg.modelType == "xgboost" && !XgbRegressor::available()) {
        r.message = "XGBoost was requested but this build has no libxgboost "
                    "(run scripts/install_xgboost.sh and rebuild, or set NEXUS_ML_MODEL_TYPE=random_forest)";
        return r;
    }

    // Seeded Fisher-Yates shuffle, then split. The model never sees the test rows.
    std::vector<size_t> idx(y.size());
    std::iota(idx.begin(), idx.end(), 0);
    uint64_t s = cfg.splitSeed ? cfg.splitSeed : 1;
    auto rnd = [&]() { s ^= s >> 12; s ^= s << 25; s ^= s >> 27; return s * 2685821657736338717ull; };
    for (size_t i = idx.size() - 1; i > 0; --i) std::swap(idx[i], idx[rnd() % (i + 1)]);

    size_t nTest = std::max<size_t>(3, static_cast<size_t>(cfg.testFraction * static_cast<double>(y.size())));
    size_t nTrain = y.size() - nTest;
    std::vector<std::vector<double>> Xtr, Xte;
    std::vector<double> ytr, yte;
    for (size_t k = 0; k < idx.size(); ++k) {
        (k < nTrain ? Xtr : Xte).push_back(X[idx[k]]);
        (k < nTrain ? ytr : yte).push_back(y[idx[k]]);
    }
    r.nTrain = static_cast<int>(nTrain);
    r.nTest = static_cast<int>(nTest);

    DurationModel model;
    std::string err;
    if (!fitModel(cfg.modelType, cfg, Xtr, ytr, model, err)) { r.message = "training failed: " + err; return r; }

    double mean = std::accumulate(ytr.begin(), ytr.end(), 0.0) / static_cast<double>(ytr.size());
    std::vector<double> base(yte.size(), mean);
    score(model, Xte, yte, r.mae, r.rmse);
    r.baselineMae = meanAbsError(base, yte);
    r.baselineRmse = rootMeanSquaredError(base, yte);

    // Honest side-by-side: the OTHER algorithm, same train rows, same test rows.
    const std::string other = cfg.modelType == "xgboost" ? "random_forest" : "xgboost";
    if (other == "random_forest" || XgbRegressor::available()) {
        DurationModel cm;
        std::string cerr;
        if (fitModel(other, cfg, Xtr, ytr, cm, cerr)) {
            r.compareModel = other;
            score(cm, Xte, yte, r.compareMae, r.compareRmse);
        }
    }

    if (r.mae < r.baselineMae) {
        r.status = "TRAINED";
        r.message = cfg.modelType + " beats the mean-duration baseline on the held-out split";
        modelOut = model;
    } else {
        r.status = "NOT_ACTIVATED";
        r.message = "model is not better than the mean-duration baseline on the held-out split; "
                    "the scheduler keeps using the resource-aware fallback. Collect more varied history.";
    }
    return r;
}

bool saveModelFile(const std::string& path, const DurationModel& model, int runId, std::string& err) {
    size_t slash = path.find_last_of('/');
    if (slash != std::string::npos) {
        std::string dir = path.substr(0, slash);
        ::mkdir(dir.c_str(), 0755);      // one level is enough for the default models/
    }
    std::string tmp = path + ".tmp";
    const std::string body = model.serialize();
    if (body.empty()) { err = "model could not be serialised"; return false; }
    if (!writeFile(tmp, "RUN " + std::to_string(runId) + "\n" + body)) {
        err = "cannot write " + tmp;
        return false;
    }
    if (std::rename(tmp.c_str(), path.c_str()) != 0) { err = "cannot rename model file"; return false; }
    return true;
}

bool loadModelFile(const std::string& path, DurationModel& out, int& runId) {
    std::string text;
    if (!readFile(path, text)) return false;
    size_t nl = text.find('\n');
    if (nl == std::string::npos || text.compare(0, 4, "RUN ") != 0) return false;
    try { runId = std::stoi(text.substr(4, nl - 4)); } catch (...) { return false; }
    DurationModel m;
    if (!DurationModel::deserialize(text.substr(nl + 1), m)) return false;
    if (m.featureCount() != featureNames().size()) return false;     // trained with other features
    out = m;
    return true;
}

TrainReport trainFromDatabase(Database& db, const TrainConfig& cfg, DurationModel& modelOut) {
    TrainReport r;
    r.modelType = cfg.modelType;
    Database::Result h = db.exec(
        "SELECT eh.task_type, eh.scale, COALESCE(eh.input_bytes,0), eh.worker_cpu_at_assign, "
        "       eh.worker_ram_at_assign, eh.running_at_assign, eh.worker_capacity, w.cpu_cores, "
        "       eh.execution_time "
        "FROM execution_history eh JOIN workers w ON w.worker_id = eh.worker_id "
        "WHERE eh.status = 'COMPLETED' AND eh.execution_time IS NOT NULL AND eh.scale IS NOT NULL "
        "  AND eh.running_at_assign IS NOT NULL AND eh.worker_capacity IS NOT NULL "
        "  AND eh.worker_cpu_at_assign IS NOT NULL AND eh.worker_ram_at_assign IS NOT NULL "
        "ORDER BY eh.execution_id");
    if (!h.ok) { r.message = "database error: " + h.error; return r; }

    std::vector<std::vector<double>> X;
    std::vector<double> y;
    for (size_t i = 0; i < h.size(); ++i) {
        TaskFeatures f;
        f.type = h.at(i, 0);
        f.scale = static_cast<int>(h.i(i, 1, 1));
        f.inputBytes = h.i(i, 2);
        f.cpu = h.d(i, 3); f.ram = h.d(i, 4);
        f.running = static_cast<int>(h.i(i, 5));
        f.capacity = static_cast<int>(h.i(i, 6, 1));
        f.cores = static_cast<int>(h.i(i, 7, 1));
        X.push_back(featureVector(f));
        y.push_back(h.d(i, 8));
    }

    r = trainAndEvaluate(X, y, cfg, modelOut);

    // Record the run first so the model file can carry its run id.
    const bool rf = cfg.modelType == "random_forest";
    Database::Result ins = db.exec(
        "INSERT INTO ml_training_runs (status, n_samples, n_train, n_test, mae, rmse, baseline_mae, "
        "  baseline_rmse, n_trees, max_depth, model_path, message, model_type, hyperparams, compare_model, "
        "  compare_mae, compare_rmse, finished_at) "
        "VALUES ($1,$2::int,$3::int,$4::int,$5,$6,$7,$8,$9::int,$10::int,$11,$12,$13,$14,$15,$16,$17, now()) "
        "RETURNING run_id",
        {r.status, std::to_string(r.nSamples), std::to_string(r.nTrain), std::to_string(r.nTest),
         r.nTest ? fmtDouble(r.mae, 6) : kSqlNull, r.nTest ? fmtDouble(r.rmse, 6) : kSqlNull,
         r.nTest ? fmtDouble(r.baselineMae, 6) : kSqlNull, r.nTest ? fmtDouble(r.baselineRmse, 6) : kSqlNull,
         std::to_string(rf ? cfg.forest.nTrees : cfg.xgb.rounds),
         std::to_string(rf ? cfg.forest.maxDepth : cfg.xgb.maxDepth),
         r.activated() ? cfg.modelPath : kSqlNull, r.message, cfg.modelType, r.hyperparams,
         r.compareModel.empty() ? kSqlNull : r.compareModel,
         r.compareModel.empty() ? kSqlNull : fmtDouble(r.compareMae, 6),
         r.compareModel.empty() ? kSqlNull : fmtDouble(r.compareRmse, 6)});
    if (ins.ok && !ins.empty()) r.runId = static_cast<int>(ins.i(0, 0));

    if (r.activated() && !cfg.modelPath.empty()) {
        std::string err;
        if (!saveModelFile(cfg.modelPath, modelOut, r.runId, err)) {
            r.status = "FAILED";
            r.message = "trained but could not save the model: " + err;
            db.exec("UPDATE ml_training_runs SET status='FAILED', message=$2 WHERE run_id=$1::int",
                    {std::to_string(r.runId), r.message});
        }
    }
    return r;
}

}  // namespace ml
}  // namespace nexus
