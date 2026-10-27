// Scheduling policies: FCFS, Round Robin, Priority, Resource-Aware, AI-assisted (+ fallback),
// capacity, stale-heartbeat rejection, locality, retry exclusion.
#include "../master/scheduler.h"
#include "test_util.h"

using namespace nexus;

namespace {
SchedWorker W(const std::string& id, double cpu = 10, double ram = 10, int running = 0, int cap = 4) {
    SchedWorker w;
    w.id = id; w.connected = true; w.hbAgeSec = 1; w.metricsAgeSec = 1;
    w.cpu = cpu; w.ram = ram; w.disk = 10; w.running = running; w.capacity = cap; w.cores = 4;
    return w;
}
SchedTask T(long long id, int prio = 5) {
    SchedTask t;
    t.id = id; t.priority = prio; t.type = "CPU_BURN"; t.scale = 1;
    return t;
}
}  // namespace

int main() {
    TEST_CASE("policy names");
    {
        Policy p;
        CHECK(parsePolicy("fcfs", p) && p == Policy::FCFS);
        CHECK(parsePolicy("RR", p) && p == Policy::ROUND_ROBIN);
        CHECK(parsePolicy("least_loaded", p) && p == Policy::RESOURCE_AWARE);   // legacy name
        CHECK(parsePolicy("ai", p) && p == Policy::AI_ASSISTED);
        CHECK(!parsePolicy("bogus", p));
    }

    TEST_CASE("FCFS: submission order, stable for equal priorities, first eligible worker");
    {
        Scheduler s;
        s.setPolicy(Policy::FCFS);
        std::vector<SchedTask> ts = {T(7, 1), T(3, 9), T(5, 5), T(4, 0)};
        auto order = s.order(ts);
        CHECK_EQ(ts[order[0]].id, 3LL); CHECK_EQ(ts[order[1]].id, 4LL);
        CHECK_EQ(ts[order[2]].id, 5LL); CHECK_EQ(ts[order[3]].id, 7LL);      // priority ignored
        std::vector<SchedWorker> ws = {W("W2"), W("W1"), W("W3")};
        Decision d = s.pick(T(1), ws);
        CHECK(d.found); CHECK_EQ(d.workerId, std::string("W1"));
        ws[1].running = ws[1].capacity;                                       // W1 full -> W2
        CHECK_EQ(s.pick(T(2), ws).workerId, std::string("W2"));
    }

    TEST_CASE("Round Robin: rotates, skips full / unhealthy workers, no starvation");
    {
        Scheduler s;
        s.setPolicy(Policy::ROUND_ROBIN);
        std::vector<SchedWorker> ws = {W("W1"), W("W2"), W("W3")};
        std::string seq;
        for (int i = 0; i < 6; ++i) seq += s.pick(T(i), ws).workerId + " ";
        CHECK_EQ(seq, std::string("W1 W2 W3 W1 W2 W3 "));
        ws[1].running = ws[1].capacity;                                       // W2 full
        seq.clear();
        for (int i = 0; i < 4; ++i) seq += s.pick(T(i), ws).workerId + " ";
        CHECK_EQ(seq, std::string("W1 W3 W1 W3 "));          // cursor was on W3: wraps to W1, W2 skipped
        ws[1].running = 0;
        ws[0].hbAgeSec = 100;                                                 // W1 heartbeat expired
        seq.clear();
        for (int i = 0; i < 4; ++i) seq += s.pick(T(i), ws).workerId + " ";
        CHECK(seq.find("W1") == std::string::npos);
        CHECK(seq.find("W2") != std::string::npos && seq.find("W3") != std::string::npos);
    }

    TEST_CASE("Priority: higher first, ties by submission order, non-preemptive");
    {
        Scheduler s;
        s.setPolicy(Policy::PRIORITY);
        std::vector<SchedTask> ts = {T(1, 3), T(2, 9), T(3, 3), T(4, 9), T(5, 0), T(6, 5)};
        auto o = s.order(ts);
        std::vector<long long> ids;
        for (size_t i : o) ids.push_back(ts[i].id);
        CHECK((ids == std::vector<long long>{2, 4, 6, 1, 3, 5}));
        std::vector<SchedWorker> ws = {W("W1", 10, 10, 3), W("W2", 10, 10, 1)};
        CHECK_EQ(s.pick(T(1, 9), ws).workerId, std::string("W2"));           // least occupied
    }

    TEST_CASE("Resource-aware: lowest score wins, normalised, stale/invalid metrics rejected");
    {
        Scheduler s(9, 9);
        s.setPolicy(Policy::RESOURCE_AWARE);
        std::vector<SchedWorker> ws = {W("W1", 80, 50), W("W2", 20, 30), W("W3", 55, 20)};
        Decision d = s.pick(T(1), ws);
        CHECK(d.found); CHECK_EQ(d.workerId, std::string("W2"));
        CHECK(d.score > 0 && d.score < 1);
        CHECK(s.resourceScore(W("A", 100, 100, 4, 4), false) > s.resourceScore(W("B", 0, 0, 0, 4), false));
        // a free-but-busy-CPU worker vs a loaded-queue worker
        std::vector<SchedWorker> ws2 = {W("W1", 5, 5, 3, 4), W("W2", 40, 40, 0, 4)};
        CHECK_EQ(s.pick(T(1), ws2).workerId, std::string("W2"));
        // stale metrics -> never chosen even if they look idle
        ws[1].metricsAgeSec = 60;
        CHECK_EQ(s.pick(T(1), ws).workerId, std::string("W3"));
        ws[2].cpu = 250;                                                      // invalid reading
        CHECK_EQ(s.pick(T(1), ws).workerId, std::string("W1"));
        ws[0].metricsAgeSec = -1;                                             // never reported
        CHECK(!s.pick(T(1), ws).found);
        // ACTIVE flag alone is not enough: expired heartbeat => unhealthy
        std::vector<SchedWorker> ws3 = {W("W1", 1, 1)};
        ws3[0].hbAgeSec = 9.5;
        CHECK(!s.pick(T(1), ws3).found);
        ws3[0].hbAgeSec = 8.9;
        CHECK(s.pick(T(1), ws3).found);
        ws3[0].connected = false;
        CHECK(!s.pick(T(1), ws3).found);
    }

    TEST_CASE("capacity: a full worker is never assigned");
    {
        Scheduler s;
        for (Policy p : {Policy::FCFS, Policy::ROUND_ROBIN, Policy::PRIORITY, Policy::RESOURCE_AWARE, Policy::AI_ASSISTED}) {
            s.setPolicy(p);
            std::vector<SchedWorker> ws = {W("W1", 1, 1, 2, 2), W("W2", 90, 90, 1, 2)};
            Decision d = s.pick(T(1), ws);
            CHECK(d.found); CHECK_EQ(d.workerId, std::string("W2"));
            ws[1].running = 2;
            CHECK(!s.pick(T(2), ws).found);
        }
    }

    TEST_CASE("data locality: block tasks only run on replica holders; resource-aware prefers them");
    {
        Scheduler s;
        s.setPolicy(Policy::RESOURCE_AWARE);
        SchedTask t = T(1);
        t.localityRequired = true;
        t.localWorkers = {"W2", "W3"};
        std::vector<SchedWorker> ws = {W("W1", 1, 1), W("W2", 50, 50), W("W3", 60, 60)};
        CHECK_EQ(s.pick(t, ws).workerId, std::string("W2"));
        ws[1].hbAgeSec = 99;                                                  // holder offline
        CHECK_EQ(s.pick(t, ws).workerId, std::string("W3"));
        ws[2].hbAgeSec = 99;
        CHECK(!s.pick(t, ws).found);                                          // no holder online: wait, don't run blind
        // soft preference (not required): the -0.15 locality bonus beats a slightly lighter remote worker
        SchedTask soft = T(2);
        soft.localWorkers = {"W2"};
        std::vector<SchedWorker> ws2 = {W("W1", 30, 30), W("W2", 40, 40)};
        CHECK_EQ(s.pick(soft, ws2).workerId, std::string("W2"));
    }

    TEST_CASE("retry: a worker that already failed the task is avoided if another is available");
    {
        Scheduler s;
        s.setPolicy(Policy::FCFS);
        SchedTask t = T(1);
        t.excluded = {"W1"};
        std::vector<SchedWorker> ws = {W("W1"), W("W2")};
        CHECK_EQ(s.pick(t, ws).workerId, std::string("W2"));
        std::vector<SchedWorker> only = {W("W1")};
        CHECK_EQ(s.pick(t, only).workerId, std::string("W1"));                // sole worker: allowed
    }

    TEST_CASE("AI-assisted: picks the shortest predicted duration, falls back without a model");
    {
        Scheduler s;
        s.setPolicy(Policy::AI_ASSISTED);
        std::vector<SchedWorker> ws = {W("W1", 10, 10), W("W2", 90, 90), W("W3", 50, 50)};
        // fake model: duration grows with CPU, but W2 is "fast" for this task
        Predictor model = [](const SchedTask&, const SchedWorker& w, double& s2) {
            s2 = w.id == "W2" ? 0.5 : 1.0 + w.cpu / 50.0;
            return true;
        };
        Decision d = s.pick(T(1), ws, &model);
        CHECK(d.found); CHECK(!d.fallback);
        CHECK_EQ(d.workerId, std::string("W2"));
        CHECK(d.predictedSeconds > 0.49 && d.predictedSeconds < 0.51);
        CHECK_EQ(d.effectivePolicy, std::string("AI_ASSISTED"));

        Decision nomodel = s.pick(T(1), ws, nullptr);                         // no trained model
        CHECK(nomodel.found); CHECK(nomodel.fallback);
        CHECK_EQ(nomodel.effectivePolicy, std::string("RESOURCE_AWARE"));
        CHECK_EQ(nomodel.workerId, std::string("W1"));                        // = what RESOURCE_AWARE picks
        CHECK_EQ(nomodel.requestedPolicy, std::string("AI_ASSISTED"));

        Predictor broken = [](const SchedTask&, const SchedWorker& w, double& s2) { s2 = 1; return w.id != "W3"; };
        Decision b = s.pick(T(1), ws, &broken);                               // model fails for one worker
        CHECK(b.found); CHECK(b.fallback);
        CHECK(!b.reason.empty());
    }

    TEST_CASE("decision record carries the metrics it was based on");
    {
        Scheduler s;
        s.setPolicy(Policy::RESOURCE_AWARE);
        std::vector<SchedWorker> ws = {W("W1", 12, 34)};
        Decision d = s.pick(T(1), ws);
        CHECK(d.candidatesJson.find("\"worker\":\"W1\"") != std::string::npos);
        CHECK(d.candidatesJson.find("\"cpu\":12") != std::string::npos);
        CHECK(d.cpu == 12 && d.ram == 34);
        CHECK(!d.reason.empty());
    }
    return testResult("test_scheduler");
}
