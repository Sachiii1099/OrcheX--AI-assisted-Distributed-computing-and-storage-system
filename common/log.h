// Thread-safe console logger. Every thread (socket handlers, heartbeat loop,
// CLI) prints through this so lines never interleave mid-message. The last
// few hundred lines are also kept in memory so the dashboard can show them.
#pragma once

#include <chrono>
#include <cstdio>
#include <ctime>
#include <deque>
#include <mutex>
#include <string>
#include <vector>

namespace nexus {

struct LogLine { long long seq; long long tsMs; std::string tag, msg; };

class LogBuffer {
public:
    static LogBuffer& instance() { static LogBuffer b; return b; }
    void push(const std::string& tag, const std::string& msg) {
        std::lock_guard<std::mutex> lock(m_);
        long long ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch()).count();
        lines_.push_back({++seq_, ts, tag, msg});
        if (lines_.size() > kMax) lines_.pop_front();
    }
    std::vector<LogLine> since(long long afterSeq, size_t limit) const {
        std::lock_guard<std::mutex> lock(m_);
        std::vector<LogLine> out;
        for (const auto& l : lines_) if (l.seq > afterSeq) out.push_back(l);
        if (out.size() > limit) out.erase(out.begin(), out.end() - static_cast<long>(limit));
        return out;
    }
private:
    static constexpr size_t kMax = 500;
    mutable std::mutex m_;
    std::deque<LogLine> lines_;
    long long seq_ = 0;
};

// log("MASTER", "hello")           ->  [MASTER] hello
// log("SCHEDULER", "line1\nline2") ->  [SCHEDULER]
//                                      line1
//                                      line2
inline void log(const std::string& tag, const std::string& msg) {
    static std::mutex m;
    LogBuffer::instance().push(tag, msg);
    std::lock_guard<std::mutex> lock(m);
    if (msg.find('\n') == std::string::npos)
        std::printf("[%s] %s\n", tag.c_str(), msg.c_str());
    else
        std::printf("[%s]\n%s\n", tag.c_str(), msg.c_str());
    std::fflush(stdout);
}

}  // namespace nexus
