// CPU affinity (Linux sched_setaffinity) and the deterministic benchmark workload plan.
// Affinity results are verified by reading the mask back from the kernel, never assumed.
#include <sched.h>
#include <algorithm>

#include "../master/benchmark.h"
#include "../worker/cpu_affinity.h"
#include "test_util.h"

using namespace nexus;

int main() {
    TEST_CASE("CPU list parsing and formatting");
    {
        std::vector<int> v;
        std::string err;
        CHECK(parseCpuList("0,2-3", v, err));
        CHECK((v == std::vector<int>{0, 2, 3}));
        CHECK(parseCpuList("5", v, err) && (v == std::vector<int>{5}));
        CHECK(parseCpuList("3,1,1", v, err) && (v == std::vector<int>{1, 3}));       // sorted, de-duplicated
        CHECK(!parseCpuList("", v, err));
        CHECK(!parseCpuList("a", v, err));
        CHECK(!parseCpuList("3-1", v, err));
        CHECK(!parseCpuList("1,,2", v, err));
        CHECK(!parseCpuList("-1", v, err));
        CHECK_EQ(formatCpuList({0, 1, 2, 5}), std::string("0-2,5"));
        CHECK_EQ(formatCpuList({4}), std::string("4"));
        CHECK_EQ(workerIndexFromId("W1"), 0);
        CHECK_EQ(workerIndexFromId("node-7"), 6);
        CHECK_EQ(workerIndexFromId("alpha"), -1);
    }

    const std::vector<int> original = currentAffinity();
    CHECK(!original.empty());
    if (!original.empty()) {
        TEST_CASE("not requested: nothing is changed and nothing is claimed");
        {
            AffinityResult r = applyCpuAffinity("none", 0);
            CHECK_EQ(r.status, std::string("NOT_REQUESTED"));
            CHECK(!r.pinned());
            CHECK((currentAffinity() == original));
        }
        TEST_CASE("invalid / unavailable requests FAIL honestly and leave the mask untouched");
        {
            AffinityResult r1 = applyCpuAffinity("banana", 0);
            CHECK_EQ(r1.status, std::string("FAILED"));
            CHECK(!r1.pinned());
            AffinityResult r2 = applyCpuAffinity("9999", 0);
            CHECK_EQ(r2.status, std::string("FAILED"));
            CHECK(r2.detail.find("NOT pinned") != std::string::npos);
            CHECK((currentAffinity() == original));
        }
        TEST_CASE("explicit CPU: applied and verified with sched_getaffinity");
        {
            const int cpu = original.back();
            AffinityResult r = applyCpuAffinity(std::to_string(cpu), 0);
            CHECK_EQ(r.status, std::string("APPLIED"));
            CHECK(r.pinned());
            CHECK((r.cpus == std::vector<int>{cpu}));
            CHECK((currentAffinity() == std::vector<int>{cpu}));      // the kernel agrees
            std::printf("  pinned to CPU %d, kernel reports %s\n", cpu, formatCpuList(currentAffinity()).c_str());
        }
        // restore, then test partial and auto
        {
            cpu_set_t set;
            CPU_ZERO(&set);
            for (int c : original) CPU_SET(c, &set);
            sched_setaffinity(0, sizeof set, &set);
        }
        TEST_CASE("partially valid list: usable CPUs applied, status PARTIAL with a warning");
        {
            AffinityResult r = applyCpuAffinity(std::to_string(original.front()) + ",9999", 0);
            CHECK_EQ(r.status, std::string("PARTIAL"));
            CHECK((r.cpus == std::vector<int>{original.front()}));
            CHECK(r.detail.find("9999") != std::string::npos);
        }
        {
            cpu_set_t set;
            CPU_ZERO(&set);
            for (int c : original) CPU_SET(c, &set);
            sched_setaffinity(0, sizeof set, &set);
        }
        TEST_CASE("auto: Worker k gets its own distinct CPU while CPUs last, then shares (PARTIAL)");
        {
            const int n = static_cast<int>(original.size());
            std::vector<int> used;
            for (int k = 0; k < n; ++k) {
                cpu_set_t set;
                CPU_ZERO(&set);
                for (int c : original) CPU_SET(c, &set);
                sched_setaffinity(0, sizeof set, &set);
                AffinityResult r = applyCpuAffinity("auto", k);
                CHECK_EQ(r.status, std::string("APPLIED"));
                CHECK_EQ(r.cpus.size(), 1u);
                used.push_back(r.cpus.empty() ? -1 : r.cpus[0]);
            }
            std::sort(used.begin(), used.end());
            CHECK(std::unique(used.begin(), used.end()) == used.end());   // all distinct
            AffinityResult over = applyCpuAffinity("auto", n);            // more Workers than CPUs
            CHECK_EQ(over.status, std::string("PARTIAL"));
            CHECK(over.detail.find("SHARES") != std::string::npos);
        }
    }

    TEST_CASE("benchmark workload plan is deterministic and policy independent");
    {
        BenchmarkSpec s;
        std::string err;
        CHECK(BenchmarkRunner::parseSpec("all", "MIXED", 30, 1, 3, "varied", 7, 1, 0, 120, s, err));
        CHECK_EQ(s.policies.size(), 5u);
        CHECK_EQ(s.policies[0], std::string("FCFS"));
        auto a = BenchmarkRunner::plan(s), b = BenchmarkRunner::plan(s);
        CHECK_EQ(a.size(), 30u);
        bool same = a.size() == b.size();
        bool varied = false;
        for (size_t i = 0; same && i < a.size(); ++i) {
            same = a[i].type == b[i].type && a[i].scale == b[i].scale && a[i].priority == b[i].priority;
            if (a[i].priority != a[0].priority) varied = true;
            CHECK(a[i].scale >= 1 && a[i].scale <= 3 && a[i].priority >= 0 && a[i].priority <= 9);
        }
        CHECK(same);
        CHECK(varied);
        BenchmarkSpec s2 = s;
        s2.seed = 8;
        auto c = BenchmarkRunner::plan(s2);
        bool differs = false;
        for (size_t i = 0; i < a.size(); ++i) if (a[i].type != c[i].type || a[i].scale != c[i].scale || a[i].priority != c[i].priority) differs = true;
        CHECK(differs);                                                   // another seed => another workload
        BenchmarkSpec u = s;
        u.priorities = "uniform";
        for (const auto& t : BenchmarkRunner::plan(u)) CHECK_EQ(t.priority, 5);
    }

    TEST_CASE("benchmark request validation");
    {
        BenchmarkSpec s;
        std::string err;
        CHECK(!BenchmarkRunner::parseSpec("FCFS,NOPE", "MIXED", 10, 1, 2, "varied", 1, 1, 0, 60, s, err));
        CHECK(!BenchmarkRunner::parseSpec("FCFS", "NOPE", 10, 1, 2, "varied", 1, 1, 0, 60, s, err));
        CHECK(!BenchmarkRunner::parseSpec("FCFS", "MIXED", 0, 1, 2, "varied", 1, 1, 0, 60, s, err));
        CHECK(!BenchmarkRunner::parseSpec("FCFS", "MIXED", 10, 3, 2, "varied", 1, 1, 0, 60, s, err));
        CHECK(!BenchmarkRunner::parseSpec("FCFS", "MIXED", 10, 1, 2, "weird", 1, 1, 0, 60, s, err));
        CHECK(!BenchmarkRunner::parseSpec("FCFS", "BLOCK_CHECKSUM", 10, 1, 1, "varied", 1, 1, 0, 60, s, err));   // needs dataset
        CHECK(BenchmarkRunner::parseSpec("FCFS", "BLOCK_CHECKSUM", 10, 1, 1, "varied", 1, 1, 3, 60, s, err));
        CHECK(BenchmarkRunner::parseSpec("rr,fcfs,rr", "CPU_BURN", 10, 1, 5, "uniform", 1, 2, 0, 60, s, err));
        CHECK_EQ(s.policies.size(), 2u);                                  // duplicates removed, aliases normalised
        CHECK_EQ(s.policies[0], std::string("ROUND_ROBIN"));
    }
    return testResult("test_affinity");
}
