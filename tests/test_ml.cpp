// ML pipeline: random forest, train/test split without leakage, honest status reporting,
// model persistence. The synthetic data below is TEST-ONLY (it checks the algorithm); it is
// never used by the running system, which trains only on real execution_history rows.
#include <cmath>
#include <random>

#include "../common/util.h"
#include "../ml/features.h"
#include "../ml/duration_model.h"
#include "../ml/random_forest.h"
#include "../ml/xgb_model.h"
#include "../ml/trainer.h"
#include "test_util.h"

using namespace nexus;
using namespace nexus::ml;

int main() {
    TEST_CASE("random forest learns a known non-linear function");
    {
        std::mt19937 rng(5);
        std::uniform_real_distribution<double> u(0, 1);
        std::vector<std::vector<double>> X;
        std::vector<double> y;
        auto f = [](double a, double b, double c) { return 2.0 * a + (b > 0.5 ? 3.0 : 0.0) + (c > 0.7 ? a * 4 : 0.0); };
        for (int i = 0; i < 600; ++i) { double a = u(rng), b = u(rng), c = u(rng); X.push_back({a, b, c}); y.push_back(f(a, b, c)); }
        RandomForest rf;
        ForestParams p;
        p.nTrees = 60; p.maxDepth = 10;
        rf.fit(X, y, p);
        CHECK(rf.trained());
        CHECK_EQ(rf.treeCount(), 60u);
        std::vector<double> pred, truth;
        for (int i = 0; i < 200; ++i) { double a = u(rng), b = u(rng), c = u(rng); pred.push_back(rf.predict({a, b, c})); truth.push_back(f(a, b, c)); }
        double mae = meanAbsError(pred, truth);
        std::printf("  held-out MAE on synthetic function: %.3f (target range 0..9)\n", mae);
        CHECK(mae < 0.45);

        // deterministic: same seed => identical model
        RandomForest rf2;
        rf2.fit(X, y, p);
        CHECK(rf.serialize() == rf2.serialize());

        // persistence round trip gives bit-identical predictions
        RandomForest loaded;
        CHECK(RandomForest::deserialize(rf.serialize(), loaded));
        for (int i = 0; i < 20; ++i) { std::vector<double> x = {u(rng), u(rng), u(rng)}; CHECK(rf.predict(x) == loaded.predict(x)); }
        CHECK(!RandomForest::deserialize("garbage", loaded));
        CHECK(!RandomForest::deserialize("NEXUSRF 1 3 1\n2\n0 0.5 5 6 1\n", loaded));   // child index out of range
    }

    TEST_CASE("metrics");
    {
        CHECK(meanAbsError({1, 2, 3}, {2, 2, 5}) == 1.0);
        CHECK(std::fabs(rootMeanSquaredError({0, 0}, {3, 4}) - std::sqrt(12.5)) < 1e-12);
    }

    TEST_CASE("too little history: honest INSUFFICIENT_DATA, no model");
    {
        std::vector<std::vector<double>> X(10, std::vector<double>(featureNames().size(), 1.0));
        std::vector<double> y(10, 1.0);
        DurationModel rf;
        TrainConfig cfg;
        cfg.minSamples = 30;
        TrainReport r = trainAndEvaluate(X, y, cfg, rf);
        CHECK_EQ(r.status, std::string("INSUFFICIENT_DATA"));
        CHECK(!rf.trained());
        CHECK(!r.activated());
        CHECK_EQ(r.nTrain, 0);
        CHECK(r.message.find("30") != std::string::npos);
        TrainReport empty = trainAndEvaluate({}, {}, cfg, rf);
        CHECK_EQ(empty.status, std::string("INSUFFICIENT_DATA"));
    }

    TEST_CASE("no train/test leakage: on unlearnable noise the held-out error is NOT near zero");
    {
        // If test rows leaked into training, a forest would memorise them and report MAE ~ 0.
        std::mt19937 rng(11);
        std::uniform_real_distribution<double> u(0, 1);
        std::vector<std::vector<double>> X;
        std::vector<double> y;
        for (int i = 0; i < 300; ++i) {
            std::vector<double> x(featureNames().size());
            for (auto& v : x) v = u(rng);
            X.push_back(x);
            y.push_back(u(rng) * 10);                       // target independent of the features
        }
        DurationModel rf;
        TrainConfig cfg;
        cfg.modelType = "random_forest";
        TrainReport r = trainAndEvaluate(X, y, cfg, rf);
        CHECK_EQ(r.nTrain + r.nTest, 300);
        CHECK(r.nTest >= 50);
        std::printf("  noise data: held-out MAE %.2f vs baseline MAE %.2f, status %s\n", r.mae, r.baselineMae, r.status.c_str());
        CHECK(r.mae > 0.6 * r.baselineMae);                 // no memorisation of the test split
        CHECK(r.status != "TRAINED" || r.mae < r.baselineMae);
    }

    TEST_CASE("learnable data: model beats the baseline and is activated");
    {
        std::mt19937 rng(3);
        std::uniform_real_distribution<double> u(0, 1);
        std::vector<std::vector<double>> X;
        std::vector<double> y;
        const auto& types = proto::allTaskTypes();
        for (int i = 0; i < 240; ++i) {
            TaskFeatures f;
            f.type = types[rng() % 5];
            f.scale = 1 + static_cast<int>(rng() % 8);
            f.cpu = u(rng) * 100; f.ram = u(rng) * 100; f.running = static_cast<int>(rng() % 4); f.capacity = 4; f.cores = 4;
            double base = f.type == "CPU_BURN" ? 0.1 : (f.type == "CHECKSUM" ? 1.0 : 0.5);
            y.push_back(base * f.scale * (1 + 0.15 * f.running));
            X.push_back(featureVector(f));
        }
        DurationModel rf;
        TrainConfig cfg;
        cfg.modelType = "random_forest";
        TrainReport r = trainAndEvaluate(X, y, cfg, rf);
        CHECK_EQ(r.status, std::string("TRAINED"));
        CHECK(r.activated() && rf.trained() && rf.kindName() == "random_forest");
        CHECK(r.mae < r.baselineMae && r.rmse < r.baselineRmse);
        CHECK(r.nTest > 0 && r.nTrain > r.nTest);
        std::printf("  MAE %.3f RMSE %.3f (baseline MAE %.3f RMSE %.3f)\n", r.mae, r.rmse, r.baselineMae, r.baselineRmse);
    }

    TEST_CASE("features and model file");
    {
        TaskFeatures f;
        f.type = "CHECKSUM"; f.scale = 4;
        CHECK_EQ(featureVector(f).size(), featureNames().size());
        DurationModel rf;
        int run = 0;
        CHECK(!loadModelFile("/tmp/definitely/not/here.model", rf, run));        // missing model => not loaded => fallback
        std::string path = "/tmp/nexus_ml_test.model", err;
        std::vector<std::vector<double>> X = {{1.0}, {2.0}, {3.0}, {4.0}};
        std::vector<double> y = {1, 2, 3, 4};
        RandomForest tiny;
        ForestParams p; p.nTrees = 3;
        tiny.fit(X, y, p);
        DurationModel tinyModel;
        tinyModel.rf = std::make_shared<RandomForest>(tiny);
        CHECK(saveModelFile(path, tinyModel, 7, err));
        CHECK(!loadModelFile(path, rf, run));                                    // 1 feature != featureNames(): refused
        CHECK(writeFile(path, "RUN x\nbroken"));
        CHECK(!loadModelFile(path, rf, run));
        std::remove(path.c_str());
    }
    TEST_CASE("feature list contains only scheduling-time information (no leakage)");
    {
        for (const auto& n : featureNames()) {
            CHECK(n.find("duration") == std::string::npos && n.find("exec") == std::string::npos &&
                  n.find("result") == std::string::npos && n.find("finish") == std::string::npos);
        }
        CHECK_EQ(featureNames().size(), proto::allTaskTypes().size() + 8);
    }

    // ---------------- XGBoost (real libxgboost) ----------------
    int skipped = 0;
    if (!XgbRegressor::available()) {
        std::printf("SKIPPED XGBoost tests: this build has no libxgboost (run scripts/install_xgboost.sh and rebuild)\n");
        skipped = 1;
    } else {
        std::printf("libxgboost version %s\n", XgbRegressor::libraryVersion().c_str());

        TEST_CASE("xgboost learns a known non-linear function; training is deterministic");
        {
            std::mt19937 rng(5);
            std::uniform_real_distribution<double> u(0, 1);
            std::vector<std::vector<double>> X;
            std::vector<double> y;
            auto f = [](double a, double b, double c) { return 2.0 * a + (b > 0.5 ? 3.0 : 0.0) + (c > 0.7 ? a * 4 : 0.0); };
            for (int i = 0; i < 600; ++i) { double a = u(rng), b = u(rng), c = u(rng); X.push_back({a, b, c}); y.push_back(f(a, b, c)); }
            XgbParams p;
            XgbRegressor m1, m2;
            std::string err;
            CHECK(m1.fit(X, y, p, err));
            CHECK(m1.trained());
            CHECK_EQ(m1.featureCount(), 3u);
            std::vector<double> pred, truth;
            for (int i = 0; i < 200; ++i) { double a = u(rng), b = u(rng), c = u(rng); pred.push_back(m1.predict({a, b, c})); truth.push_back(f(a, b, c)); }
            double mae = meanAbsError(pred, truth);
            std::printf("  xgboost held-out MAE on synthetic function: %.3f (target range 0..9)\n", mae);
            CHECK(mae < 0.45);
            CHECK(m2.fit(X, y, p, err));
            CHECK(m1.serialize() == m2.serialize());                    // same data + seed => identical model

            XgbRegressor loaded;                                          // persistence round trip
            CHECK(XgbRegressor::deserialize(m1.serialize(), loaded));
            for (int i = 0; i < 20; ++i) { std::vector<double> x = {u(rng), u(rng), u(rng)}; CHECK(m1.predict(x) == loaded.predict(x)); }
            CHECK(!XgbRegressor::deserialize("garbage", loaded));
            CHECK(!XgbRegressor::deserialize("NEXUSXGB 1 3 5\n{not json", loaded));
            XgbRegressor bad;
            CHECK(!bad.fit({}, {}, p, err));                               // empty data refused
            CHECK(!bad.trained());
        }

        TEST_CASE("xgboost pipeline: held-out MAE/RMSE, beats the mean baseline, compared with random forest");
        {
            std::mt19937 rng(3);
            std::uniform_real_distribution<double> u(0, 1);
            std::vector<std::vector<double>> X;
            std::vector<double> y;
            const auto& types = proto::allTaskTypes();
            for (int i = 0; i < 240; ++i) {
                TaskFeatures f;
                f.type = types[rng() % 5];
                f.scale = 1 + static_cast<int>(rng() % 8);
                f.cpu = u(rng) * 100; f.ram = u(rng) * 100; f.running = static_cast<int>(rng() % 4); f.capacity = 4; f.cores = 4;
                double base = f.type == "CPU_BURN" ? 0.1 : (f.type == "CHECKSUM" ? 1.0 : 0.5);
                y.push_back(base * f.scale * (1 + 0.15 * f.running));
                X.push_back(featureVector(f));
            }
            TrainConfig cfg;                                              // default model type = xgboost
            CHECK_EQ(cfg.modelType, std::string("xgboost"));
            DurationModel model;
            TrainReport r = trainAndEvaluate(X, y, cfg, model);
            CHECK_EQ(r.status, std::string("TRAINED"));
            CHECK_EQ(r.modelType, std::string("xgboost"));
            CHECK_EQ(model.kindName(), std::string("xgboost"));
            CHECK(r.mae < r.baselineMae && r.rmse < r.baselineRmse);
            CHECK(r.nTest > 0 && r.nTrain > r.nTest && r.nTrain + r.nTest == 240);
            CHECK_EQ(r.compareModel, std::string("random_forest"));       // same split, other algorithm
            CHECK(r.compareMae > 0);
            std::printf("  XGBoost MAE %.3f RMSE %.3f | baseline MAE %.3f RMSE %.3f | RF MAE %.3f RMSE %.3f\n",
                        r.mae, r.rmse, r.baselineMae, r.baselineRmse, r.compareMae, r.compareRmse);

            // determinism of the whole pipeline
            DurationModel again;
            TrainReport r2 = trainAndEvaluate(X, y, cfg, again);
            CHECK(r.mae == r2.mae && r.rmse == r2.rmse);

            // model file round trip through the same code the Master uses at start-up
            std::string path = "/tmp/nexus_xgb_test.model", err;
            CHECK(saveModelFile(path, model, 42, err));
            DurationModel back;
            int run = 0;
            CHECK(loadModelFile(path, back, run));
            CHECK_EQ(run, 42);
            CHECK_EQ(back.kindName(), std::string("xgboost"));
            for (int i = 0; i < 10; ++i) CHECK(model.predict(X[static_cast<size_t>(i)]) == back.predict(X[static_cast<size_t>(i)]));
            CHECK(writeFile(path, "RUN 1\nNEXUSXGB 1 16 10\n{broken"));
            CHECK(!loadModelFile(path, back, run));                       // corrupt file => not loaded => fallback
            std::remove(path.c_str());
        }

        TEST_CASE("xgboost: no leakage on unlearnable noise, honest NOT_ACTIVATED");
        {
            std::mt19937 rng(11);
            std::uniform_real_distribution<double> u(0, 1);
            std::vector<std::vector<double>> X;
            std::vector<double> y;
            for (int i = 0; i < 300; ++i) {
                std::vector<double> x(featureNames().size());
                for (auto& v : x) v = u(rng);
                X.push_back(x);
                y.push_back(u(rng) * 10);
            }
            DurationModel m;
            TrainConfig cfg;
            TrainReport r = trainAndEvaluate(X, y, cfg, m);
            std::printf("  noise data: xgboost held-out MAE %.2f vs baseline MAE %.2f, status %s\n", r.mae, r.baselineMae, r.status.c_str());
            CHECK(r.mae > 0.6 * r.baselineMae);
            CHECK(r.status != "TRAINED" || r.mae < r.baselineMae);
            if (r.status == "NOT_ACTIVATED") CHECK(!m.trained());
        }

        TEST_CASE("xgboost: too little history => INSUFFICIENT_DATA, no model");
        {
            std::vector<std::vector<double>> X(10, std::vector<double>(featureNames().size(), 1.0));
            std::vector<double> y(10, 1.0);
            DurationModel m;
            TrainConfig cfg;
            TrainReport r = trainAndEvaluate(X, y, cfg, m);
            CHECK_EQ(r.status, std::string("INSUFFICIENT_DATA"));
            CHECK(!m.trained());
        }
    }
    int rc = testResult("test_ml");
    if (rc == 0 && skipped) { std::printf("test_ml: XGBoost part SKIPPED (exit 77)\n"); return 77; }
    return rc;
}
