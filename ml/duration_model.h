// DurationModel: the model the scheduler actually uses. It is either an XGBoost regressor or
// the from-scratch Random Forest - the kind is always explicit (kindName()) and shown in the
// dashboard, so a Random Forest can never be presented as XGBoost.
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "random_forest.h"
#include "xgb_model.h"

namespace nexus {
namespace ml {

struct DurationModel {
    std::shared_ptr<RandomForest> rf;
    std::shared_ptr<XgbRegressor> xgb;

    bool trained() const { return (rf && rf->trained()) || (xgb && xgb->trained()); }
    std::string kindName() const {
        return xgb && xgb->trained() ? "xgboost" : rf && rf->trained() ? "random_forest" : "none";
    }
    size_t featureCount() const { return xgb && xgb->trained() ? xgb->featureCount() : rf ? rf->featureCount() : 0; }
    double predict(const std::vector<double>& x) const {
        if (xgb && xgb->trained()) return xgb->predict(x);
        if (rf && rf->trained()) return rf->predict(x);
        return 0;
    }
    std::string serialize() const {
        if (xgb && xgb->trained()) return xgb->serialize();
        if (rf && rf->trained()) return rf->serialize();
        return "";
    }
    static bool deserialize(const std::string& text, DurationModel& out) {
        DurationModel m;
        if (text.compare(0, 8, "NEXUSXGB") == 0) {
            m.xgb = std::make_shared<XgbRegressor>();
            if (!XgbRegressor::deserialize(text, *m.xgb)) return false;
        } else {
            m.rf = std::make_shared<RandomForest>();
            if (!RandomForest::deserialize(text, *m.rf)) return false;
        }
        out = m;
        return true;
    }
};

}  // namespace ml
}  // namespace nexus
