// XGBoost regressor (gradient-boosted trees) used for task-duration prediction.
//
// This is the REAL XGBoost library (dmlc/xgboost, C API: libxgboost). It is linked only
// when CMake found the library (see scripts/install_xgboost.sh); otherwise
// XgbRegressor::available() is false, every fit() fails and NEXUS reports honestly that
// XGBoost is not compiled in - it never silently substitutes another algorithm.
#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace nexus {
namespace ml {

struct XgbParams {
    int rounds = 150;            // boosting rounds (trees)
    int maxDepth = 4;
    double eta = 0.1;            // learning rate
    double lambda = 1.0;         // L2 regularisation
    double minChildWeight = 1.0;
    int seed = 42;
    int nthread = 1;             // 1 => bit-for-bit deterministic training
};

class XgbRegressor {
public:
    XgbRegressor();
    ~XgbRegressor();
    XgbRegressor(const XgbRegressor&) = delete;
    XgbRegressor& operator=(const XgbRegressor&) = delete;

    static bool available();                       // compiled with libxgboost?
    static std::string libraryVersion();           // "2.1.4" or "" when unavailable

    // Trains on (X, y). Returns false (and err) on failure / when XGBoost is unavailable.
    bool fit(const std::vector<std::vector<double>>& X, const std::vector<double>& y,
             const XgbParams& p, std::string& err);
    double predict(const std::vector<double>& x) const;
    bool trained() const { return trained_; }
    size_t featureCount() const { return nFeatures_; }
    int rounds() const { return rounds_; }

    // "NEXUSXGB 1 <nfeatures> <rounds>\n<xgboost JSON model>"
    std::string serialize() const;
    static bool deserialize(const std::string& text, XgbRegressor& out);

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
    bool trained_ = false;
    size_t nFeatures_ = 0;
    int rounds_ = 0;
    mutable std::mutex mutex_;
};

}  // namespace ml
}  // namespace nexus
