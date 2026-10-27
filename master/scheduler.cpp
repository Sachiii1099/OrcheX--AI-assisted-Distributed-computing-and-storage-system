#include "scheduler.h"

#include <algorithm>
#include <cmath>

#include "../common/json.h"
#include "../common/util.h"

namespace nexus {

const char* policyName(Policy p) {
    switch (p) {
        case Policy::FCFS:           return "FCFS";
        case Policy::ROUND_ROBIN:    return "ROUND_ROBIN";
        case Policy::PRIORITY:       return "PRIORITY";
        case Policy::RESOURCE_AWARE: return "RESOURCE_AWARE";
        case Policy::AI_ASSISTED:    return "AI_ASSISTED";
    }
    return "?";
}

bool parsePolicy(const std::string& in, Policy& out) {
    std::string s = upper(trim(in));
    if (s == "FCFS") out = Policy::FCFS;
    else if (s == "ROUND_ROBIN" || s == "RR" || s == "ROUNDROBIN") out = Policy::ROUND_ROBIN;
    else if (s == "PRIORITY") out = Policy::PRIORITY;
    else if (s == "RESOURCE_AWARE" || s == "LEAST_LOADED" || s == "RESOURCE") out = Policy::RESOURCE_AWARE;
    else if (s == "AI_ASSISTED" || s == "AI") out = Policy::AI_ASSISTED;
    else return false;
    return true;
}

void Scheduler::setPolicy(Policy p) {
    std::lock_guard<std::mutex> lock(mutex_);
    policy_ = p;
}
Policy Scheduler::policy() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return policy_;
}
void Scheduler::configure(int hb, double stale) {
    std::lock_guard<std::mutex> lock(mutex_);
    hbTimeout_ = hb;
    staleSec_ = stale;
}

bool Scheduler::isHealthy(const SchedWorker& w) const {
    // The flag alone is never trusted: the heartbeat must also be recent.
    return w.connected && w.hbAgeSec >= 0 && w.hbAgeSec <= hbTimeout_;
}

bool Scheduler::metricsUsable(const SchedWorker& w) const {
    auto ok = [](double v) { return std::isfinite(v) && v >= 0.0 && v <= 100.0; };
    return w.metricsAgeSec >= 0 && w.metricsAgeSec <= staleSec_ && ok(w.cpu) && ok(w.ram) && ok(w.disk) &&
           w.capacity >= 1;
}

double Scheduler::resourceScore(const SchedWorker& w, bool hasLocalData) const {
    const double occupancy = std::min(1.0, static_cast<double>(w.running) / std::max(1, w.capacity));
    const double hbAge = std::min(1.0, std::max(0.0, w.hbAgeSec) / std::max(1, hbTimeout_));
    return 0.35 * w.cpu / 100.0 + 0.20 * w.ram / 100.0 + 0.05 * w.disk / 100.0 +
           0.30 * occupancy + 0.10 * hbAge - (hasLocalData ? 0.15 : 0.0);
}

std::vector<size_t> Scheduler::order(const std::vector<SchedTask>& tasks) const {
    Policy p = policy();
    std::vector<size_t> idx(tasks.size());
    for (size_t i = 0; i < idx.size(); ++i) idx[i] = i;
    const bool byPriority = (p == Policy::PRIORITY || p == Policy::AI_ASSISTED);
    std::stable_sort(idx.begin(), idx.end(), [&](size_t a, size_t b) {
        if (byPriority && tasks[a].priority != tasks[b].priority)
            return tasks[a].priority > tasks[b].priority;          // higher priority first
        return tasks[a].id < tasks[b].id;                          // tie-break: earlier submission
    });
    return idx;
}

namespace {
std::string candJson(const std::vector<SchedWorker>& ws, const std::vector<Candidate>& cs) {
    std::string out = "[";
    for (size_t i = 0; i < ws.size(); ++i) {
        if (i) out += ",";
        json::Obj o;
        o.add("worker", ws[i].id).add("cpu", ws[i].cpu).add("ram", ws[i].ram).add("disk", ws[i].disk)
         .add("running", ws[i].running).add("capacity", ws[i].capacity)
         .add("hb_age_s", ws[i].hbAgeSec < 1e8 ? ws[i].hbAgeSec : -1.0)
         .add("eligible", cs[i].eligible).add("score", cs[i].score).add("note", cs[i].note);
        if (cs[i].predicted >= 0) o.add("predicted_s", cs[i].predicted);
        out += o.build();
    }
    return out + "]";
}
}  // namespace

Decision Scheduler::pickResourceAware(const SchedTask& t, const std::vector<SchedWorker>& ws,
                                      const std::vector<int>& eligible, std::vector<Candidate>& cands,
                                      const char* label) const {
    Decision d;
    int best = -1;
    for (int i : eligible) {
        if (!metricsUsable(ws[i])) {
            cands[i].eligible = false;
            cands[i].note = "rejected: stale or invalid resource metrics";
            continue;
        }
        const bool local = t.localWorkers.count(ws[i].id) != 0;
        cands[i].score = resourceScore(ws[i], local);
        cands[i].note = local ? "holds task data (-0.15)" : "";
        if (best < 0 || cands[i].score < cands[best].score ||
            (cands[i].score == cands[best].score && ws[i].id < ws[best].id))
            best = i;
    }
    if (best < 0) { d.reason = std::string(label) + ": no Worker with fresh, valid metrics"; return d; }
    d.found = true;
    d.workerId = ws[best].id;
    d.score = cands[best].score;
    d.reason = std::string(label) + ": lowest resource score " + fmtDouble(d.score, 3);
    return d;
}

Decision Scheduler::pick(const SchedTask& t, const std::vector<SchedWorker>& workers,
                         const Predictor* predictor) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Stable, id-sorted view: makes FCFS / Round Robin deterministic.
    std::vector<SchedWorker> ws = workers;
    std::sort(ws.begin(), ws.end(), [](const SchedWorker& a, const SchedWorker& b) { return a.id < b.id; });

    std::vector<Candidate> cands(ws.size());
    std::vector<int> eligible, preferred;
    for (size_t i = 0; i < ws.size(); ++i) {
        Candidate& c = cands[i];
        c.worker = ws[i].id;
        if (!isHealthy(ws[i]))                       c.note = "rejected: heartbeat expired / disconnected";
        else if (ws[i].running >= ws[i].capacity)    c.note = "rejected: no free slot";
        else if (t.localityRequired && !t.localWorkers.count(ws[i].id))
                                                     c.note = "rejected: does not hold the task's data block";
        else { c.eligible = true; eligible.push_back(static_cast<int>(i)); }
    }
    // Avoid a Worker that already failed this task, unless it is the only choice.
    for (int i : eligible) if (!t.excluded.count(ws[i].id)) preferred.push_back(i);
    if (!preferred.empty()) {
        for (int i : eligible)
            if (t.excluded.count(ws[i].id)) { cands[i].eligible = false; cands[i].note = "skipped: failed this task before"; }
        eligible = preferred;
    }

    Decision d;
    d.requestedPolicy = policyName(policy_);
    d.effectivePolicy = d.requestedPolicy;

    if (eligible.empty()) {
        d.reason = "no eligible Worker (unhealthy, full, or missing the data block)";
        d.candidates = cands;
        d.candidatesJson = candJson(ws, cands);
        return d;
    }

    switch (policy_) {
        case Policy::FCFS: {
            d.found = true;
            d.workerId = ws[eligible.front()].id;
            d.reason = "FCFS: first eligible Worker by id";
            break;
        }
        case Policy::ROUND_ROBIN: {
            // First eligible Worker whose id comes after the last one used (wrap around).
            int chosen = -1;
            for (int i : eligible) if (ws[i].id > lastRrWorker_) { chosen = i; break; }
            if (chosen < 0) chosen = eligible.front();
            d.found = true;
            d.workerId = ws[chosen].id;
            d.reason = "ROUND_ROBIN: next eligible Worker after '" +
                       (lastRrWorker_.empty() ? std::string("(start)") : lastRrWorker_) + "'";
            lastRrWorker_ = d.workerId;
            break;
        }
        case Policy::PRIORITY: {
            int best = eligible.front();
            auto occ = [&](int i) { return static_cast<double>(ws[i].running) / std::max(1, ws[i].capacity); };
            for (int i : eligible) if (occ(i) < occ(best)) best = i;   // strict < keeps lower id on ties
            d.found = true;
            d.workerId = ws[best].id;
            d.reason = "PRIORITY: task priority " + std::to_string(t.priority) +
                       "; least-occupied Worker (" + fmtDouble(occ(best) * 100, 0) + "% slots used)";
            break;
        }
        case Policy::RESOURCE_AWARE: {
            Decision r = pickResourceAware(t, ws, eligible, cands, "RESOURCE_AWARE");
            r.requestedPolicy = d.requestedPolicy;
            r.effectivePolicy = d.effectivePolicy;
            d = r;
            break;
        }
        case Policy::AI_ASSISTED: {
            // 1. usable metrics + a prediction for every candidate, otherwise fall back.
            std::string whyNot;
            std::vector<int> usable;
            for (int i : eligible) {
                if (!metricsUsable(ws[i])) {
                    cands[i].eligible = false;
                    cands[i].note = "rejected: stale or invalid resource metrics";
                } else usable.push_back(i);
            }
            bool allPredicted = predictor && !usable.empty();
            if (!predictor) whyNot = "no trained model";
            if (predictor) {
                for (int i : usable) {
                    double s = 0;
                    if ((*predictor)(t, ws[i], s) && std::isfinite(s) && s >= 0) cands[i].predicted = s;
                    else { allPredicted = false; if (whyNot.empty()) whyNot = "model could not predict for " + ws[i].id; }
                }
            }
            if (allPredicted) {
                int best = usable.front();
                auto better = [&](int a, int b) {
                    if (cands[a].predicted != cands[b].predicted) return cands[a].predicted < cands[b].predicted;
                    double sa = resourceScore(ws[a], false), sb = resourceScore(ws[b], false);
                    if (sa != sb) return sa < sb;
                    return ws[a].id < ws[b].id;
                };
                for (int i : usable) {
                    cands[i].score = resourceScore(ws[i], false);
                    if (better(i, best)) best = i;
                }
                d.found = true;
                d.workerId = ws[best].id;
                d.score = cands[best].score;
                d.predictedSeconds = cands[best].predicted;
                d.reason = "AI_ASSISTED: shortest predicted duration " + fmtDouble(d.predictedSeconds, 3) + " s";
            } else {
                if (whyNot.empty()) whyNot = "no Worker with usable metrics";
                Decision r = pickResourceAware(t, ws, eligible, cands, "RESOURCE_AWARE (fallback)");
                r.requestedPolicy = d.requestedPolicy;
                r.effectivePolicy = policyName(Policy::RESOURCE_AWARE);
                r.fallback = true;
                if (r.found) r.reason = "AI_ASSISTED fallback -> RESOURCE_AWARE (" + whyNot + "): lowest resource score " +
                                        fmtDouble(r.score, 3);
                d = r;
            }
            break;
        }
    }

    if (d.found) {
        for (size_t i = 0; i < ws.size(); ++i) if (ws[i].id == d.workerId) {
            d.cpu = ws[i].cpu; d.ram = ws[i].ram; d.running = ws[i].running;
            d.capacity = ws[i].capacity; d.cores = ws[i].cores;
            if (d.score == 0 && cands[i].score != 0) d.score = cands[i].score;
        }
    }
    d.candidates = cands;
    d.candidatesJson = candJson(ws, cands);
    return d;
}

}  // namespace nexus
