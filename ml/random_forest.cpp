#include "random_forest.h"

#include <algorithm>
#include <cmath>
#include <numeric>
#include <sstream>

namespace nexus {
namespace ml {

namespace {
// xorshift64*: tiny deterministic PRNG, identical on every platform.
struct Rng {
    uint64_t s;
    explicit Rng(uint64_t seed) : s(seed ? seed : 0x9E3779B97F4A7C15ull) {}
    uint64_t next() { s ^= s >> 12; s ^= s << 25; s ^= s >> 27; return s * 2685821657736338717ull; }
    size_t below(size_t n) { return static_cast<size_t>(next() % n); }
};
}  // namespace

struct TreeBuilder {
    const std::vector<std::vector<double>>& X;
    const std::vector<double>& y;
    const ForestParams& p;
    Rng& rng;
    size_t nFeat;
    std::vector<RandomForest::Node>& nodes;

    static double mean(const std::vector<double>& y, const std::vector<size_t>& idx) {
        double s = 0;
        for (size_t i : idx) s += y[i];
        return idx.empty() ? 0 : s / static_cast<double>(idx.size());
    }

    int build(std::vector<size_t>& idx, int depth) {
        int me = static_cast<int>(nodes.size());
        nodes.emplace_back();
        nodes[me].value = mean(y, idx);

        if (depth >= p.maxDepth || static_cast<int>(idx.size()) < 2 * p.minLeaf) return me;

        // random subset of features for this split
        std::vector<size_t> feats(nFeat);
        std::iota(feats.begin(), feats.end(), 0);
        size_t mtry = std::max<size_t>(1, static_cast<size_t>(std::ceil(p.featureFraction * nFeat)));
        for (size_t i = 0; i + 1 < feats.size(); ++i) std::swap(feats[i], feats[i + rng.below(feats.size() - i)]);
        feats.resize(mtry);

        double bestGain = 1e-12, bestThr = 0;
        int bestFeat = -1;
        const double n = static_cast<double>(idx.size());
        double totalSum = 0, totalSq = 0;
        for (size_t i : idx) { totalSum += y[i]; totalSq += y[i] * y[i]; }
        const double parentSse = totalSq - totalSum * totalSum / n;

        std::vector<size_t> sorted = idx;
        for (size_t f : feats) {
            std::sort(sorted.begin(), sorted.end(), [&](size_t a, size_t b) { return X[a][f] < X[b][f]; });
            double lSum = 0, lSq = 0;
            for (size_t k = 0; k + 1 < sorted.size(); ++k) {
                double yy = y[sorted[k]];
                lSum += yy; lSq += yy * yy;
                const double nl = static_cast<double>(k + 1), nr = n - nl;
                if (X[sorted[k]][f] == X[sorted[k + 1]][f]) continue;      // cannot split between equal values
                if (nl < p.minLeaf || nr < p.minLeaf) continue;
                const double rSum = totalSum - lSum, rSq = totalSq - lSq;
                const double sse = (lSq - lSum * lSum / nl) + (rSq - rSum * rSum / nr);
                const double gain = parentSse - sse;
                if (gain > bestGain) {
                    bestGain = gain;
                    bestFeat = static_cast<int>(f);
                    bestThr = (X[sorted[k]][f] + X[sorted[k + 1]][f]) / 2.0;
                }
            }
        }
        if (bestFeat < 0) return me;

        std::vector<size_t> L, R;
        for (size_t i : idx) (X[i][bestFeat] <= bestThr ? L : R).push_back(i);
        if (L.empty() || R.empty()) return me;

        nodes[me].feature = bestFeat;
        nodes[me].threshold = bestThr;
        int l = build(L, depth + 1);
        int r = build(R, depth + 1);
        nodes[me].left = l;
        nodes[me].right = r;
        return me;
    }
};

void RandomForest::fit(const std::vector<std::vector<double>>& X, const std::vector<double>& y,
                       const ForestParams& params) {
    trees_.clear();
    nFeatures_ = 0;
    if (X.empty() || X.size() != y.size() || X[0].empty()) return;
    nFeatures_ = X[0].size();
    Rng rng(params.seed);
    for (int t = 0; t < params.nTrees; ++t) {
        std::vector<size_t> sample(X.size());                     // bootstrap sample
        for (auto& s : sample) s = rng.below(X.size());
        Tree tree;
        TreeBuilder b{X, y, params, rng, nFeatures_, tree};
        b.build(sample, 0);
        trees_.push_back(std::move(tree));
    }
}

double RandomForest::predictTree(const Tree& t, const std::vector<double>& x) {
    int n = 0;
    while (t[n].feature >= 0) {
        const size_t f = static_cast<size_t>(t[n].feature);
        n = (f < x.size() && x[f] <= t[n].threshold) ? t[n].left : t[n].right;
    }
    return t[n].value;
}

double RandomForest::predict(const std::vector<double>& x) const {
    if (trees_.empty()) return 0;
    double s = 0;
    for (const auto& t : trees_) s += predictTree(t, x);
    return s / static_cast<double>(trees_.size());
}

std::string RandomForest::serialize() const {
    std::ostringstream o;
    o.precision(17);
    o << "NEXUSRF 1 " << nFeatures_ << " " << trees_.size() << "\n";
    for (const auto& t : trees_) {
        o << t.size() << "\n";
        for (const auto& n : t) o << n.feature << " " << n.threshold << " " << n.left << " " << n.right << " " << n.value << "\n";
    }
    return o.str();
}

bool RandomForest::deserialize(const std::string& text, RandomForest& out) {
    std::istringstream in(text);
    std::string magic;
    int version = 0;
    size_t nf = 0, nt = 0;
    if (!(in >> magic >> version >> nf >> nt) || magic != "NEXUSRF" || version != 1 || nt == 0 || nt > 100000) return false;
    RandomForest f;
    f.nFeatures_ = nf;
    for (size_t t = 0; t < nt; ++t) {
        size_t nn = 0;
        if (!(in >> nn) || nn == 0 || nn > 10000000) return false;
        Tree tree(nn);
        for (auto& n : tree) {
            if (!(in >> n.feature >> n.threshold >> n.left >> n.right >> n.value)) return false;
            if (n.feature >= 0 && (n.left < 0 || n.right < 0 || static_cast<size_t>(n.left) >= nn ||
                                   static_cast<size_t>(n.right) >= nn)) return false;
        }
        f.trees_.push_back(std::move(tree));
    }
    out = std::move(f);
    return true;
}

double meanAbsError(const std::vector<double>& p, const std::vector<double>& t) {
    if (p.empty() || p.size() != t.size()) return 0;
    double s = 0;
    for (size_t i = 0; i < p.size(); ++i) s += std::fabs(p[i] - t[i]);
    return s / static_cast<double>(p.size());
}

double rootMeanSquaredError(const std::vector<double>& p, const std::vector<double>& t) {
    if (p.empty() || p.size() != t.size()) return 0;
    double s = 0;
    for (size_t i = 0; i < p.size(); ++i) s += (p[i] - t[i]) * (p[i] - t[i]);
    return std::sqrt(s / static_cast<double>(p.size()));
}

}  // namespace ml
}  // namespace nexus
