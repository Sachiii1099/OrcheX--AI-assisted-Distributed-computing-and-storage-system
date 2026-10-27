// Random Forest REGRESSOR written from scratch (no Python / ML library needed at
// run time): bagged CART regression trees with random feature subsets,
// variance-reduction splits, deterministic given a seed.
#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace nexus {
namespace ml {

struct ForestParams {
    int nTrees = 60;
    int maxDepth = 12;
    int minLeaf = 2;
    double featureFraction = 0.6;     // fraction of features tried at each split
    uint64_t seed = 42;
};

class RandomForest {
public:
    void fit(const std::vector<std::vector<double>>& X, const std::vector<double>& y,
             const ForestParams& params);
    double predict(const std::vector<double>& x) const;     // mean of the trees
    bool trained() const { return !trees_.empty(); }
    size_t featureCount() const { return nFeatures_; }
    size_t treeCount() const { return trees_.size(); }

    std::string serialize() const;                           // text format, round-trips exactly
    static bool deserialize(const std::string& text, RandomForest& out);

private:
    struct Node {
        int feature = -1;             // -1 = leaf
        double threshold = 0;
        int left = -1, right = -1;
        double value = 0;
    };
    using Tree = std::vector<Node>;

    static double predictTree(const Tree& t, const std::vector<double>& x);

    std::vector<Tree> trees_;
    size_t nFeatures_ = 0;
    friend struct TreeBuilder;
};

double meanAbsError(const std::vector<double>& pred, const std::vector<double>& truth);
double rootMeanSquaredError(const std::vector<double>& pred, const std::vector<double>& truth);

}  // namespace ml
}  // namespace nexus
