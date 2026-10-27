#include "xgb_model.h"

#include <cmath>
#include <cstdio>
#include <cstring>
#include <sstream>

#ifdef NEXUS_HAVE_XGBOOST
#include <xgboost/c_api.h>
#endif

namespace nexus {
namespace ml {

#ifdef NEXUS_HAVE_XGBOOST

struct XgbRegressor::Impl {
    BoosterHandle booster = nullptr;
    ~Impl() { if (booster) XGBoosterFree(booster); }
};

namespace {
bool ok(int rc, std::string& err) {
    if (rc == 0) return true;
    err = XGBGetLastError();
    return false;
}
}  // namespace

XgbRegressor::XgbRegressor() : impl_(new Impl) {}
XgbRegressor::~XgbRegressor() = default;
bool XgbRegressor::available() { return true; }
std::string XgbRegressor::libraryVersion() {
    int a = 0, b = 0, c = 0;
    XGBoostVersion(&a, &b, &c);
    return std::to_string(a) + "." + std::to_string(b) + "." + std::to_string(c);
}

bool XgbRegressor::fit(const std::vector<std::vector<double>>& X, const std::vector<double>& y,
                       const XgbParams& p, std::string& err) {
    std::lock_guard<std::mutex> lock(mutex_);
    trained_ = false;
    if (X.empty() || X.size() != y.size() || X[0].empty()) { err = "empty or inconsistent training data"; return false; }
    const size_t n = X.size(), f = X[0].size();
    std::vector<float> flat(n * f), labels(n);
    for (size_t i = 0; i < n; ++i) {
        if (X[i].size() != f) { err = "ragged feature matrix"; return false; }
        for (size_t j = 0; j < f; ++j) flat[i * f + j] = static_cast<float>(X[i][j]);
        labels[i] = static_cast<float>(y[i]);
    }
    DMatrixHandle dm = nullptr;
    // "missing" marker is NaN so that legitimate 0 / -1 feature values are never treated as missing
    if (!ok(XGDMatrixCreateFromMat(flat.data(), n, f, std::nanf(""), &dm), err)) return false;
    if (!ok(XGDMatrixSetFloatInfo(dm, "label", labels.data(), n), err)) { XGDMatrixFree(dm); return false; }

    if (impl_->booster) { XGBoosterFree(impl_->booster); impl_->booster = nullptr; }
    BoosterHandle b = nullptr;
    if (!ok(XGBoosterCreate(&dm, 1, &b), err)) { XGDMatrixFree(dm); return false; }
    auto set = [&](const char* k, const std::string& v) { return XGBoosterSetParam(b, k, v.c_str()) == 0; };
    bool good = set("objective", "reg:squarederror") && set("tree_method", "hist") &&
                set("max_depth", std::to_string(p.maxDepth)) && set("eta", std::to_string(p.eta)) &&
                set("lambda", std::to_string(p.lambda)) && set("min_child_weight", std::to_string(p.minChildWeight)) &&
                set("seed", std::to_string(p.seed)) && set("nthread", std::to_string(p.nthread)) &&
                set("verbosity", "0");
    if (!good) { err = XGBGetLastError(); XGBoosterFree(b); XGDMatrixFree(dm); return false; }
    for (int i = 0; i < p.rounds; ++i)
        if (XGBoosterUpdateOneIter(b, i, dm) != 0) { err = XGBGetLastError(); XGBoosterFree(b); XGDMatrixFree(dm); return false; }
    XGDMatrixFree(dm);
    impl_->booster = b;
    nFeatures_ = f;
    rounds_ = p.rounds;
    trained_ = true;
    return true;
}

double XgbRegressor::predict(const std::vector<double>& x) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!trained_ || x.size() != nFeatures_) return 0;
    std::vector<float> row(x.begin(), x.end());
    DMatrixHandle dm = nullptr;
    if (XGDMatrixCreateFromMat(row.data(), 1, row.size(), std::nanf(""), &dm) != 0) return 0;
    const char* cfg = "{\"type\":0,\"training\":false,\"iteration_begin\":0,\"iteration_end\":0,\"strict_shape\":false}";
    const bst_ulong* shape = nullptr;
    bst_ulong dim = 0;
    const float* res = nullptr;
    double out = 0;
    if (XGBoosterPredictFromDMatrix(impl_->booster, dm, cfg, &shape, &dim, &res) == 0 && res) out = res[0];
    XGDMatrixFree(dm);
    return out;
}

std::string XgbRegressor::serialize() const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!trained_) return "";
    bst_ulong len = 0;
    const char* buf = nullptr;
    if (XGBoosterSaveModelToBuffer(impl_->booster, "{\"format\":\"json\"}", &len, &buf) != 0) return "";
    std::ostringstream o;
    o << "NEXUSXGB 1 " << nFeatures_ << " " << rounds_ << "\n";
    return o.str() + std::string(buf, len);
}

bool XgbRegressor::deserialize(const std::string& text, XgbRegressor& out) {
    size_t nl = text.find('\n');
    if (nl == std::string::npos) return false;
    std::istringstream in(text.substr(0, nl));
    std::string magic;
    int version = 0, rounds = 0;
    size_t nf = 0;
    if (!(in >> magic >> version >> nf >> rounds) || magic != "NEXUSXGB" || version != 1 || nf == 0) return false;
    const std::string json = text.substr(nl + 1);
    std::lock_guard<std::mutex> lock(out.mutex_);
    BoosterHandle b = nullptr;
    if (XGBoosterCreate(nullptr, 0, &b) != 0) return false;
    if (XGBoosterLoadModelFromBuffer(b, json.data(), json.size()) != 0) { XGBoosterFree(b); return false; }
    if (out.impl_->booster) XGBoosterFree(out.impl_->booster);
    out.impl_->booster = b;
    out.nFeatures_ = nf;
    out.rounds_ = rounds;
    out.trained_ = true;
    return true;
}

#else   // ---- built without libxgboost: honest stub ------------------------------------------

struct XgbRegressor::Impl {};
XgbRegressor::XgbRegressor() : impl_(new Impl) {}
XgbRegressor::~XgbRegressor() = default;
bool XgbRegressor::available() { return false; }
std::string XgbRegressor::libraryVersion() { return ""; }
bool XgbRegressor::fit(const std::vector<std::vector<double>>&, const std::vector<double>&, const XgbParams&,
                       std::string& err) {
    err = "NEXUS was built without XGBoost (run scripts/install_xgboost.sh, then rebuild)";
    return false;
}
double XgbRegressor::predict(const std::vector<double>&) const { return 0; }
std::string XgbRegressor::serialize() const { return ""; }
bool XgbRegressor::deserialize(const std::string&, XgbRegressor&) { return false; }

#endif

}  // namespace ml
}  // namespace nexus
