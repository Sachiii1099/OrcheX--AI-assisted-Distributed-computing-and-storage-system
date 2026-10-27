// nexus_ml: stand-alone training / evaluation tool for the task-duration model.
//   nexus_ml train [xgboost|random_forest]   read execution_history from PostgreSQL, train, evaluate, save the model
//   nexus_ml status    show the latest training runs
//   nexus_ml predict <TYPE> <scale> <cpu%> <ram%> <running> <capacity> <cores> [input_bytes]
// Exit codes: 0 ok, 3 not enough history (no model created), 1 failure.
#include <cstdio>
#include <cstdlib>
#include <string>

#include "../common/log.h"
#include "../database/database.h"
#include "../ml/features.h"
#include "../ml/trainer.h"

using namespace nexus;

int main(int argc, char** argv) {
    const std::string cmd = argc > 1 ? argv[1] : "";
    ml::TrainConfig cfg = ml::TrainConfig::fromEnv();

    if (cmd == "predict") {
        if (argc < 9) { std::fprintf(stderr, "usage: nexus_ml predict TYPE scale cpu ram running capacity cores [input_bytes]\n"); return 1; }
        ml::DurationModel rf;
        int runId = 0;
        if (!ml::loadModelFile(cfg.modelPath, rf, runId)) {
            std::printf("no trained model at %s - the scheduler would use the RESOURCE_AWARE fallback\n", cfg.modelPath.c_str());
            return 3;
        }
        ml::TaskFeatures f;
        f.type = argv[2]; f.scale = std::atoi(argv[3]); f.cpu = std::atof(argv[4]); f.ram = std::atof(argv[5]);
        f.running = std::atoi(argv[6]); f.capacity = std::atoi(argv[7]); f.cores = std::atoi(argv[8]);
        f.inputBytes = argc > 9 ? std::atoll(argv[9]) : 0;
        std::printf("predicted duration: %.4f s (model from run %d)\n", rf.predict(ml::featureVector(f)), runId);
        return 0;
    }

    if (cmd != "train" && cmd != "status") {
        std::fprintf(stderr, "usage: nexus_ml train | status | predict ...\n");
        return 1;
    }
    Database db(Database::Config::fromEnv());
    std::string err;
    if (!db.connect(err)) { std::fprintf(stderr, "Cannot connect to PostgreSQL: %s\n", err.c_str()); return 1; }

    if (cmd == "status") {
        Database::Result r = db.exec(
            "SELECT run_id, status, n_samples, n_train, n_test, round(mae::numeric,4) AS mae, round(rmse::numeric,4) AS rmse, "
            "round(baseline_mae::numeric,4) AS base_mae, to_char(finished_at,'YYYY-MM-DD HH24:MI:SS') AS at, message "
            "FROM ml_training_runs ORDER BY run_id DESC LIMIT 10");
        if (!r.ok) { std::fprintf(stderr, "%s\n", r.error.c_str()); return 1; }
        if (r.empty()) std::printf("no training runs yet (run: nexus_ml train)\n");
        for (size_t i = 0; i < r.size(); ++i) {
            for (size_t c = 0; c < r.columns.size(); ++c) std::printf("%s=%s  ", r.columns[c].c_str(), r.at(i, c).c_str());
            std::printf("\n");
        }
        return 0;
    }

    ml::DurationModel rf;
    if (argc > 2 && cmd == "train" && (std::string(argv[2]) == "random_forest" || std::string(argv[2]) == "xgboost")) {
        cfg.modelType = argv[2];
        cfg.modelPath = cfg.pathFor(cfg.modelType);
    }
    ml::TrainReport r = ml::trainFromDatabase(db, cfg, rf);
    std::printf("status        : %s\n", r.status.c_str());
    std::printf("samples       : %d (train %d / test %d)\n", r.nSamples, r.nTrain, r.nTest);
    if (r.nTest) {
        std::printf("MAE           : %.4f s   (baseline 'predict the mean': %.4f s)\n", r.mae, r.baselineMae);
        std::printf("RMSE          : %.4f s   (baseline: %.4f s)\n", r.rmse, r.baselineRmse);
    }
    std::printf("message       : %s\n", r.message.c_str());
    if (r.activated()) std::printf("model saved   : %s (run %d)\n", cfg.modelPath.c_str(), r.runId);
    std::printf("(Restart the Master or use the dashboard/console 'train' to load a model into a running Master.)\n");
    if (r.status == "INSUFFICIENT_DATA") return 3;
    return r.status == "FAILED" ? 1 : 0;
}
