// Word-count building blocks shared by the Worker (map / reduce tasks), the
// Master (MapReduce coordinator) and the tests.
//
// Tokenisation: lower-cased ASCII runs of [a-z0-9]. Everything else separates words.
// Map output is hash-partitioned into R partitions (R = number of reducers):
//   partition(word) = FNV-1a(word) mod R
// Wire/DB text format of intermediate data: lines "word<TAB>count".
// A map task returns all partitions in one string:   #P0\n...lines...#P1\n...lines...
#pragma once

#include <cstdint>
#include <cstdlib>
#include <map>
#include <string>
#include <vector>

namespace nexus {
namespace wc {

using Counts = std::map<std::string, uint64_t>;     // ordered => deterministic output

inline void countWords(const std::string& text, Counts& out) {
    std::string cur;
    for (unsigned char ch : text) {
        if ((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9')) cur += static_cast<char>(ch);
        else if (ch >= 'A' && ch <= 'Z') cur += static_cast<char>(ch - 'A' + 'a');
        else if (!cur.empty()) { ++out[cur]; cur.clear(); }
    }
    if (!cur.empty()) ++out[cur];
}

inline uint64_t fnv1a(const std::string& s) {
    uint64_t h = 1469598103934665603ull;
    for (unsigned char c : s) { h ^= c; h *= 1099511628211ull; }
    return h;
}

inline int partitionOf(const std::string& word, int reducers) {
    return reducers <= 1 ? 0 : static_cast<int>(fnv1a(word) % static_cast<uint64_t>(reducers));
}

inline std::string formatLines(const Counts& c) {
    std::string out;
    for (const auto& kv : c) out += kv.first + "\t" + std::to_string(kv.second) + "\n";
    return out;
}

// Adds every "word<TAB>count" line of `text` into `into` (the REDUCE step).
inline void mergeLines(const std::string& text, Counts& into) {
    size_t pos = 0;
    while (pos < text.size()) {
        size_t nl = text.find('\n', pos);
        if (nl == std::string::npos) nl = text.size();
        size_t tab = text.find('\t', pos);
        if (tab != std::string::npos && tab < nl && tab > pos) {
            into[text.substr(pos, tab - pos)] += std::strtoull(text.c_str() + tab + 1, nullptr, 10);
        }
        pos = nl + 1;
    }
}

// Map output: all R partitions concatenated with "#P<i>" headers.
inline std::string encodeMapOutput(const Counts& c, int reducers) {
    if (reducers < 1) reducers = 1;
    std::vector<Counts> parts(static_cast<size_t>(reducers));
    for (const auto& kv : c) parts[static_cast<size_t>(partitionOf(kv.first, reducers))][kv.first] = kv.second;
    std::string out;
    for (int i = 0; i < reducers; ++i) out += "#P" + std::to_string(i) + "\n" + formatLines(parts[static_cast<size_t>(i)]);
    return out;
}

// Inverse of encodeMapOutput: returns the "word\tcount" text of each partition.
inline std::vector<std::string> splitMapOutput(const std::string& data, int reducers) {
    std::vector<std::string> parts(static_cast<size_t>(reducers < 1 ? 1 : reducers));
    int cur = -1;
    size_t pos = 0;
    while (pos < data.size()) {
        size_t nl = data.find('\n', pos);
        if (nl == std::string::npos) nl = data.size();
        if (data.compare(pos, 2, "#P") == 0) {
            cur = std::atoi(data.c_str() + pos + 2);
            if (cur < 0 || cur >= static_cast<int>(parts.size())) cur = -1;
        } else if (cur >= 0 && nl > pos) {
            parts[static_cast<size_t>(cur)].append(data, pos, nl - pos);
            parts[static_cast<size_t>(cur)] += '\n';
        }
        pos = nl + 1;
    }
    return parts;
}

inline uint64_t total(const Counts& c) { uint64_t t = 0; for (const auto& kv : c) t += kv.second; return t; }

}  // namespace wc
}  // namespace nexus
