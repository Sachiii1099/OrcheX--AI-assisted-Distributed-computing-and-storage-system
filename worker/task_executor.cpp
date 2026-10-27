#include "task_executor.h"

#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <unordered_map>
#include <vector>

#include "../common/sha256.h"
#include "../common/wordcount.h"

namespace nexus {

namespace {

// Deterministic pseudo-random generator so results are reproducible.
struct Rng {
    uint64_t s;
    uint64_t next() { s ^= s << 13; s ^= s >> 7; s ^= s << 17; return s; }
};

TaskOutcome fail(const std::string& msg, bool retryable) {
    TaskOutcome o;
    o.error = msg;
    o.retryable = retryable;
    return o;
}

// MAP: read the block from this Worker's disk, verify it, count words, partition by hash.
TaskOutcome blockWordCount(const Params& p, const BlockStore* store) {
    if (!store || !store->ready()) return fail("worker has no block storage", false);
    const std::string blockId = paramStr(p, "block_id");
    const int reducers = static_cast<int>(paramInt(p, "reducers", 1));
    if (reducers < 1 || reducers > 64) return fail("reducers must be 1..64", false);
    std::string data, err;
    if (!store->get(blockId, data, err)) return fail("cannot read block " + blockId + ": " + err, true);
    const std::string expect = paramStr(p, "checksum");
    if (!expect.empty() && sha256Hex(data) != expect) return fail("block " + blockId + " failed checksum verification", true);

    wc::Counts counts;
    wc::countWords(data, counts);
    TaskOutcome o;
    o.ok = true;
    o.result = "block=" + blockId + ", bytes=" + std::to_string(data.size()) +
               ", words=" + std::to_string(wc::total(counts)) + ", unique=" + std::to_string(counts.size());
    o.data = wc::encodeMapOutput(counts, reducers);
    return o;
}

TaskOutcome blockChecksum(const Params& p, const BlockStore* store) {
    if (!store || !store->ready()) return fail("worker has no block storage", false);
    const std::string blockId = paramStr(p, "block_id");
    std::string data, err;
    if (!store->get(blockId, data, err)) return fail("cannot read block " + blockId + ": " + err, true);
    const std::string sum = sha256Hex(data);
    const std::string expect = paramStr(p, "checksum");
    if (!expect.empty() && sum != expect) return fail("block " + blockId + " failed checksum verification", true);
    TaskOutcome o;
    o.ok = true;
    o.result = "block=" + blockId + ", bytes=" + std::to_string(data.size()) + ", sha256=" + sum;
    return o;
}

// REDUCE: merge the "word\tcount" lines of one partition (coming from every mapper).
TaskOutcome reduceWordCount(const Params& p) {
    wc::Counts merged;
    wc::mergeLines(paramStr(p, "inputs"), merged);
    TaskOutcome o;
    o.ok = true;
    o.result = "partition=" + paramStr(p, "partition", "0") + ", unique=" + std::to_string(merged.size()) +
               ", words=" + std::to_string(wc::total(merged));
    o.data = wc::formatLines(merged);
    return o;
}

}  // namespace

TaskOutcome TaskExecutor::run(const std::string& type, int scale, const Params& params,
                              const BlockStore* store) {
    const int cap = (type == "CPU_BURN") ? 100 : 20;
    if (scale < 1) scale = 1;
    if (scale > cap) scale = cap;

    auto t0 = std::chrono::steady_clock::now();
    TaskOutcome out;
    try {
        if (type == "WORD_COUNT")             { out.result = wordCount(scale); out.ok = true; }
        else if (type == "PRIME_COUNT")       { out.result = primeCount(scale); out.ok = true; }
        else if (type == "MATRIX_MULTIPLY")   { out.result = matrixMultiply(scale); out.ok = true; }
        else if (type == "CHECKSUM")          { out.result = checksum(scale); out.ok = true; }
        else if (type == "CPU_BURN")          { out.result = cpuBurn(scale); out.ok = true; }
        else if (type == "BLOCK_WORD_COUNT")  out = blockWordCount(params, store);
        else if (type == "BLOCK_CHECKSUM")    out = blockChecksum(params, store);
        else if (type == "REDUCE_WORD_COUNT") out = reduceWordCount(params);
        else out = fail("unsupported task type: " + type, false);
    } catch (const std::exception& e) {
        out = fail(std::string("exception: ") + e.what(), true);
    }
    out.seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    return out;
}

// Build a synthetic corpus of 10M*scale words and count word frequencies.
std::string TaskExecutor::wordCount(int scale) {
    const size_t nWords = 10000000ull * scale;
    std::vector<std::string> vocab;
    for (int i = 0; i < 5000; ++i) vocab.push_back("word" + std::to_string(i * 7919 % 100003));
    Rng rng{88172645463325252ull};
    std::string text;
    text.reserve(nWords * 9);
    for (size_t i = 0; i < nWords; ++i) { text += vocab[rng.next() % vocab.size()]; text += ' '; }

    std::unordered_map<std::string, uint32_t> freq;
    size_t start = 0, total = 0;
    for (size_t i = 0; i <= text.size(); ++i) {
        if (i == text.size() || text[i] == ' ') {
            if (i > start) { ++freq[text.substr(start, i - start)]; ++total; }
            start = i + 1;
        }
    }
    std::string top; uint32_t best = 0;
    for (const auto& kv : freq)
        if (kv.second > best || (kv.second == best && kv.first < top)) { best = kv.second; top = kv.first; }
    return "words=" + std::to_string(total) + ", unique=" + std::to_string(freq.size()) +
           ", most_frequent=" + top + "(" + std::to_string(best) + ")";
}

// Sieve of Eratosthenes up to 150M*scale.
std::string TaskExecutor::primeCount(int scale) {
    const size_t n = 150000000ull * scale;
    std::vector<bool> composite(n + 1, false);
    for (size_t i = 2; i * i <= n; ++i)
        if (!composite[i])
            for (size_t j = i * i; j <= n; j += i) composite[j] = true;
    size_t count = 0;
    for (size_t i = 2; i <= n; ++i) if (!composite[i]) ++count;
    return "primes_up_to=" + std::to_string(n) + ", count=" + std::to_string(count);
}

// Dense N x N multiplication, N = 1200 * cbrt(scale) (work grows linearly with scale).
std::string TaskExecutor::matrixMultiply(int scale) {
    const int n = static_cast<int>(1200.0 * std::cbrt(static_cast<double>(scale)));
    std::vector<double> a(static_cast<size_t>(n) * n), b(static_cast<size_t>(n) * n), c(static_cast<size_t>(n) * n, 0.0);
    Rng rng{1234567ull};
    for (size_t i = 0; i < a.size(); ++i) {
        a[i] = static_cast<double>(rng.next() % 1000) / 100.0;
        b[i] = static_cast<double>(rng.next() % 1000) / 100.0;
    }
    for (int i = 0; i < n; ++i)
        for (int k = 0; k < n; ++k) {
            const double aik = a[static_cast<size_t>(i) * n + k];
            for (int j = 0; j < n; ++j) c[static_cast<size_t>(i) * n + j] += aik * b[static_cast<size_t>(k) * n + j];
        }
    double trace = 0;
    for (int i = 0; i < n; ++i) trace += c[static_cast<size_t>(i) * n + i];
    char buf[96];
    std::snprintf(buf, sizeof buf, "matrix=%dx%d, trace=%.3f", n, n, trace);
    return buf;
}

// FNV-1a over 400M*scale pseudo-random bytes.
std::string TaskExecutor::checksum(int scale) {
    const uint64_t n = 400000000ull * scale;
    uint64_t h = 1469598103934665603ull;
    Rng rng{42ull};
    for (uint64_t i = 0; i < n; i += 8) {
        uint64_t v = rng.next();
        for (int k = 0; k < 8; ++k) { h ^= (v >> (8 * k)) & 0xFF; h *= 1099511628211ull; }
    }
    char buf[64];
    std::snprintf(buf, sizeof buf, "bytes=%llu, fnv1a=%016llx",
                  static_cast<unsigned long long>(n), static_cast<unsigned long long>(h));
    return buf;
}

// Integer arithmetic on one core for scale x 100 ms of wall-clock time. The
// accumulator is returned so the compiler cannot optimise the work away.
std::string TaskExecutor::cpuBurn(int scale) {
    const auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(100LL * scale);
    uint64_t x = 88172645463325252ull, acc = 0, iters = 0;
    do {
        for (int i = 0; i < 100000; ++i) { x ^= x << 13; x ^= x >> 7; x ^= x << 17; acc += x; }
        iters += 100000;
    } while (std::chrono::steady_clock::now() < end);
    char buf[96];
    std::snprintf(buf, sizeof buf, "iterations=%llu, acc=%016llx",
                  static_cast<unsigned long long>(iters), static_cast<unsigned long long>(acc));
    return buf;
}

}  // namespace nexus
