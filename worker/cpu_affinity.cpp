#include "cpu_affinity.h"

#include <sched.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstring>
#include <set>

#include "../common/util.h"

namespace nexus {

bool parseCpuList(const std::string& spec, std::vector<int>& out, std::string& err) {
    out.clear();
    std::set<int> seen;
    for (const std::string& raw : splitOn(spec, ',')) {
        const std::string part = trim(raw);
        if (part.empty()) { err = "empty element in CPU list '" + spec + "'"; return false; }
        auto num = [&](const std::string& s, int& v) {
            if (s.empty() || s.size() > 5) return false;
            for (char c : s) if (!std::isdigit(static_cast<unsigned char>(c))) return false;
            v = std::stoi(s);
            return true;
        };
        size_t dash = part.find('-');
        int a = 0, b = 0;
        if (dash == std::string::npos) {
            if (!num(part, a)) { err = "'" + part + "' is not a CPU number"; return false; }
            b = a;
        } else if (!num(trim(part.substr(0, dash)), a) || !num(trim(part.substr(dash + 1)), b) || b < a) {
            err = "'" + part + "' is not a valid CPU range";
            return false;
        }
        for (int c = a; c <= b; ++c) if (seen.insert(c).second) out.push_back(c);
    }
    std::sort(out.begin(), out.end());
    return !out.empty();
}

std::string formatCpuList(const std::vector<int>& cpus) {
    std::string s;
    for (size_t i = 0; i < cpus.size();) {
        size_t j = i;
        while (j + 1 < cpus.size() && cpus[j + 1] == cpus[j] + 1) ++j;
        if (!s.empty()) s += ",";
        s += std::to_string(cpus[i]);
        if (j > i) s += "-" + std::to_string(cpus[j]);
        i = j + 1;
    }
    return s;
}

std::vector<int> currentAffinity() {
    std::vector<int> out;
    cpu_set_t set;
    CPU_ZERO(&set);
    if (sched_getaffinity(0, sizeof set, &set) != 0) return out;
    for (int c = 0; c < CPU_SETSIZE; ++c) if (CPU_ISSET(c, &set)) out.push_back(c);
    return out;
}

int workerIndexFromId(const std::string& id) {
    size_t e = id.size();
    size_t b = e;
    while (b > 0 && std::isdigit(static_cast<unsigned char>(id[b - 1]))) --b;
    if (b == e || e - b > 6) return -1;
    return std::max(0, std::stoi(id.substr(b)) - 1);
}

AffinityResult applyCpuAffinity(const std::string& specIn, int workerIndex) {
    AffinityResult r;
    const std::string spec = trim(specIn);
    r.requested = spec.empty() ? "none" : spec;
    r.allowed = currentAffinity();
    r.cpus = r.allowed;
    std::string lower = spec;
    for (auto& c : lower) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    if (lower.empty() || lower == "none" || lower == "off") {
        r.status = "NOT_REQUESTED";
        r.detail = "no pinning requested; the OS scheduler may use CPUs " + formatCpuList(r.allowed);
        return r;
    }
    if (r.allowed.empty()) {
        r.status = "FAILED";
        r.detail = "cannot read the current CPU affinity mask: " + std::string(std::strerror(errno));
        return r;
    }

    std::vector<int> want;
    if (lower == "auto") {
        // one dedicated CPU: the workerIndex-th allowed CPU (wraps if there are more Workers than CPUs)
        if (workerIndex < 0) workerIndex = 0;
        want.push_back(r.allowed[static_cast<size_t>(workerIndex) % r.allowed.size()]);
        if (static_cast<size_t>(workerIndex) >= r.allowed.size())
            r.detail = "WARNING: worker index " + std::to_string(workerIndex) + " >= " + std::to_string(r.allowed.size()) +
                       " allowed CPUs, so this Worker SHARES a core with another one. ";
    } else {
        std::string err;
        std::vector<int> parsed;
        if (!parseCpuList(spec, parsed, err)) {
            r.status = "FAILED";
            r.detail = "invalid NEXUS_WORKER_CPU_AFFINITY: " + err + " - Worker is NOT pinned";
            return r;
        }
        std::vector<int> bad;
        for (int c : parsed) {
            if (std::find(r.allowed.begin(), r.allowed.end(), c) != r.allowed.end()) want.push_back(c);
            else bad.push_back(c);
        }
        if (want.empty()) {
            r.status = "FAILED";
            r.detail = "none of the requested CPUs (" + formatCpuList(parsed) + ") is available; allowed CPUs are " +
                       formatCpuList(r.allowed) + " - Worker is NOT pinned";
            return r;
        }
        if (!bad.empty())
            r.detail = "WARNING: unavailable CPU(s) " + formatCpuList(bad) + " ignored. ";
    }

    cpu_set_t set;
    CPU_ZERO(&set);
    for (int c : want) CPU_SET(c, &set);
    if (sched_setaffinity(0, sizeof set, &set) != 0) {
        r.status = "FAILED";
        r.detail += "sched_setaffinity failed: " + std::string(std::strerror(errno)) + " - Worker is NOT pinned";
        return r;
    }
    r.cpus = currentAffinity();                 // read back what the kernel really applied
    if (r.cpus == want) {
        // a shared core ("auto" with more Workers than CPUs) or ignored ids make it PARTIAL, not APPLIED
        r.status = r.detail.rfind("WARNING", 0) == 0 ? "PARTIAL" : "APPLIED";
        r.detail += "pinned to CPU(s) " + formatCpuList(r.cpus) + " (verified with sched_getaffinity)";
    } else {
        r.status = "FAILED";
        r.detail += "kernel reports mask " + formatCpuList(r.cpus) + " instead of the requested " + formatCpuList(want);
    }
    return r;
}

}  // namespace nexus
