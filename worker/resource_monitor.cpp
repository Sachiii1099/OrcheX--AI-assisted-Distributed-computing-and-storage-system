#include "resource_monitor.h"

#include <sys/statvfs.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>

namespace nexus {

namespace {

// Reads "Key:   12345 kB" lines from /proc/meminfo; returns kB or -1.
long long meminfoKb(const std::string& wanted) {
    std::ifstream f("/proc/meminfo");
    std::string key;
    long long value;
    std::string unit;
    while (f >> key >> value) {
        std::getline(f, unit);                       // rest of line ("kB")
        if (key == wanted + ":") return value;
    }
    return -1;
}

}  // namespace

bool ResourceMonitor::supported() {
#if defined(__linux__)
    return true;
#else
    return false;
#endif
}

ResourceMonitor::CpuTimes ResourceMonitor::readCpuTimes() {
    CpuTimes t;
    std::ifstream f("/proc/stat");
    std::string label;
    if (!(f >> label) || label != "cpu") return t;
    // user nice system idle iowait irq softirq steal ...
    unsigned long long v[8] = {0};
    for (int i = 0; i < 8 && (f >> v[i]); ++i) {}
    const unsigned long long idle = v[3] + v[4];     // idle + iowait
    unsigned long long total = 0;
    for (int i = 0; i < 8; ++i) total += v[i];
    t.total = total;
    t.busy = total - idle;
    t.valid = total > 0;
    return t;
}

ResourceMonitor::ResourceMonitor(std::string diskPath) : diskPath_(std::move(diskPath)) {
    prev_ = readCpuTimes();
    // Give the first sample() a real interval to measure over.
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
}

ResourceSample ResourceMonitor::sample() {
    ResourceSample s;

    // ---- CPU: busy-time fraction since the previous sample -------------
    {
        std::lock_guard<std::mutex> lock(mutex_);
        CpuTimes now = readCpuTimes();
        if (now.valid && prev_.valid && now.total > prev_.total) {
            double busy  = static_cast<double>(now.busy - prev_.busy);
            double total = static_cast<double>(now.total - prev_.total);
            s.cpuPercent = std::min(100.0, std::max(0.0, 100.0 * busy / total));
        }
        if (now.valid) prev_ = now;
    }

    // ---- RAM -----------------------------------------------------------
    long long total = meminfoKb("MemTotal");
    long long avail = meminfoKb("MemAvailable");
    if (avail < 0) {                                  // older kernels: approximate
        long long f = meminfoKb("MemFree"), b = meminfoKb("Buffers"), c = meminfoKb("Cached");
        if (f >= 0) avail = f + std::max<long long>(b, 0) + std::max<long long>(c, 0);
    }
    if (total > 0 && avail >= 0) {
        s.ramPercent = std::min(100.0, std::max(0.0, 100.0 * (total - avail) / total));
        s.ramAvailableMb = avail / 1024;
    }

    // ---- Disk (filesystem that holds the Worker's storage) --------------
    struct statvfs fs;
    if (::statvfs(diskPath_.c_str(), &fs) == 0 && fs.f_blocks > 0) {
        double used = static_cast<double>(fs.f_blocks - fs.f_bfree);
        double usable = used + static_cast<double>(fs.f_bavail);   // same maths as `df`
        if (usable > 0) s.diskPercent = std::min(100.0, 100.0 * used / usable);
    }

    // ---- load average + this process's resident memory ------------------
    {
        std::ifstream f("/proc/loadavg");
        double l1 = 0;
        if (f >> l1) s.load1 = l1;
    }
    {
        std::ifstream f("/proc/self/statm");
        long long pages = 0, rss = 0;
        if (f >> pages >> rss) s.processRssMb = static_cast<double>(rss) * static_cast<double>(::sysconf(_SC_PAGESIZE)) / (1024.0 * 1024.0);
    }
    return s;
}

int ResourceMonitor::cpuCores() const {
    long n = ::sysconf(_SC_NPROCESSORS_ONLN);
    return n > 0 ? static_cast<int>(n) : 1;
}

long long ResourceMonitor::totalMemoryMb() const {
    long long kb = meminfoKb("MemTotal");
    return kb > 0 ? kb / 1024 : 0;
}

}  // namespace nexus
