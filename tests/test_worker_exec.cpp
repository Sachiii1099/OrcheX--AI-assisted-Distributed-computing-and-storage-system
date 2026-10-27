// Worker side: bounded thread pool (multi-core concurrency), task executor, block store.
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "../common/sha256.h"
#include "../common/thread_pool.h"
#include "../common/wordcount.h"
#include "../worker/block_store.h"
#include "../worker/task_executor.h"
#include "test_util.h"

using namespace nexus;

namespace {
std::string tempDir() {
    char tmpl[] = "/tmp/nexus_test_XXXXXX";
    return ::mkdtemp(tmpl);
}
}  // namespace

int main() {
    TEST_CASE("thread pool: never exceeds its limit, really runs in parallel, rejects overflow");
    {
        const size_t N = 4;
        ThreadPool pool(N, 64);
        std::atomic<int> cur{0}, peak{0}, done{0};
        for (int i = 0; i < 16; ++i) {
            bool ok = pool.submit([&] {
                int c = ++cur;
                int p = peak.load();
                while (c > p && !peak.compare_exchange_weak(p, c)) {}
                std::this_thread::sleep_for(std::chrono::milliseconds(60));
                --cur;
                ++done;
            });
            CHECK(ok);
        }
        pool.shutdown();
        CHECK_EQ(done.load(), 16);
        CHECK_EQ(peak.load(), static_cast<int>(N));          // used all N threads at once, never more

        ThreadPool tiny(1, 1);                                // 1 running + 1 queued, 3rd is rejected
        std::atomic<bool> release{false};
        auto blocker = [&] { while (!release) std::this_thread::sleep_for(std::chrono::milliseconds(5)); };
        CHECK(tiny.submit(blocker));
        std::this_thread::sleep_for(std::chrono::milliseconds(50));   // first job is now executing
        CHECK(tiny.submit(blocker));
        CHECK(!tiny.submit(blocker));
        release = true;
        tiny.shutdown();
    }

    TEST_CASE("parallel speed-up: 4 CPU tasks on 4 threads take ~1x, not 4x, the time of one");
    {
        const int ms = 300;
        auto t0 = std::chrono::steady_clock::now();
        ThreadPool pool(4, 8);
        for (int i = 0; i < 4; ++i) pool.submit([&] { TaskExecutor::run("CPU_BURN", ms / 100, {}, nullptr); });
        pool.shutdown();
        double wall = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
        std::printf("  4 x %d ms of CPU work finished in %.2f s wall-clock\n", ms, wall);
        CHECK(wall < 4 * ms / 1000.0 * 0.75);                 // far below the 1.2 s a serial run needs
    }

    TEST_CASE("task executor");
    {
        TaskOutcome o = TaskExecutor::run("CPU_BURN", 1, {}, nullptr);
        CHECK(o.ok); CHECK(o.seconds >= 0.09 && o.seconds < 1.0);
        CHECK(o.result.find("iterations=") == 0);
        TaskOutcome bad = TaskExecutor::run("NOT_A_TASK", 1, {}, nullptr);
        CHECK(!bad.ok); CHECK(!bad.retryable); CHECK(!bad.error.empty());
        TaskOutcome noStore = TaskExecutor::run("BLOCK_CHECKSUM", 1, {{"block_id", "ds1_b0"}}, nullptr);
        CHECK(!noStore.ok);
    }

    TEST_CASE("block store: checksum-verified put, atomic, traversal-proof");
    {
        BlockStore st(tempDir());
        CHECK(st.ready());
        std::string data = "hello distributed world", err;
        std::string sum = sha256Hex(data);
        CHECK(!st.put("ds1_b0", data, std::string(64, '0'), err));          // wrong checksum refused
        CHECK(st.put("ds1_b0", data, sum, err));
        std::string back;
        CHECK(st.get("ds1_b0", back, err) && back == data);
        CHECK(st.verify("ds1_b0", sum) == BlockStore::Status::OK);
        CHECK(st.verify("ds1_b9", sum) == BlockStore::Status::MISSING);
        CHECK_EQ(st.usedBytes(), static_cast<long long>(data.size()));
        // corrupt the file on disk behind the store's back
        std::string p = st.pathFor("ds1_b0", err);
        CHECK(writeFile(p, "tampered"));
        CHECK(st.verify("ds1_b0", sum) == BlockStore::Status::CORRUPT);
        // path traversal attempts
        for (const char* evil : {"../../etc/passwd", "../x", "a/b", "/etc/passwd", "..", "a.b", ""}) {
            CHECK(!st.put(evil, data, sum, err));
            CHECK(!st.get(evil, back, err));
            CHECK(st.pathFor(evil, err).empty());
        }
        CHECK(st.remove("ds1_b0", err));
        CHECK(!st.get("ds1_b0", back, err));
    }

    TEST_CASE("MapReduce word count: map -> partition -> reduce equals a direct count");
    {
        std::string text;
        const char* words[] = {"the", "quick", "Brown", "fox", "jumps", "over", "THE", "lazy", "dog", "it's", "42"};
        for (int i = 0; i < 5000; ++i) { text += words[(i * 7 + i / 3) % 11]; text += (i % 17 == 0) ? "\n" : " "; }
        wc::Counts direct;
        wc::countWords(text, direct);

        // split on whitespace-aligned boundaries into 5 blocks, store them, run the real map tasks
        BlockStore st(tempDir());
        std::string err;
        const int R = 3;
        std::vector<std::string> inputs(R);
        size_t pos = 0;
        int nBlocks = 0;
        while (pos < text.size()) {
            size_t end = std::min(text.size(), pos + text.size() / 5 + 1);
            while (end < text.size() && !std::isspace(static_cast<unsigned char>(text[end - 1]))) ++end;
            std::string blk = text.substr(pos, end - pos);
            std::string id = "ds1_b" + std::to_string(nBlocks++);
            CHECK(st.put(id, blk, sha256Hex(blk), err));
            TaskOutcome m = TaskExecutor::run("BLOCK_WORD_COUNT", 1,
                {{"block_id", id}, {"checksum", sha256Hex(blk)}, {"reducers", std::to_string(R)}}, &st);
            CHECK(m.ok);
            auto parts = wc::splitMapOutput(m.data, R);
            for (int r = 0; r < R; ++r) inputs[r] += parts[r];
            pos = end;
        }
        wc::Counts reduced;
        for (int r = 0; r < R; ++r) {
            TaskOutcome red = TaskExecutor::run("REDUCE_WORD_COUNT", 1, {{"partition", std::to_string(r)}, {"inputs", inputs[r]}}, nullptr);
            CHECK(red.ok);
            wc::mergeLines(red.data, reduced);
            // every word of partition r really hashes to r
            wc::Counts chk;
            wc::mergeLines(red.data, chk);
            for (const auto& kv : chk) CHECK_EQ(wc::partitionOf(kv.first, R), r);
        }
        CHECK(reduced == direct);
        CHECK_EQ(wc::total(reduced), 5000ULL);
        CHECK(direct.count("the") && direct["the"] > 0);                    // "The"/"THE" folded to "the"
    }

    TEST_CASE("task failure paths");
    {
        BlockStore st(tempDir());
        TaskOutcome missing = TaskExecutor::run("BLOCK_WORD_COUNT", 1, {{"block_id", "ds9_b9"}, {"reducers", "2"}}, &st);
        CHECK(!missing.ok); CHECK(missing.retryable);                       // another replica may work
        std::string err;
        std::string data = "some words here";
        st.put("ds1_b0", data, sha256Hex(data), err);
        TaskOutcome wrong = TaskExecutor::run("BLOCK_WORD_COUNT", 1,
            {{"block_id", "ds1_b0"}, {"checksum", std::string(64, 'a')}, {"reducers", "2"}}, &st);
        CHECK(!wrong.ok);                                                    // checksum mismatch detected
        CHECK(wrong.error.find("checksum") != std::string::npos);
        TaskOutcome badR = TaskExecutor::run("BLOCK_WORD_COUNT", 1, {{"block_id", "ds1_b0"}, {"reducers", "0"}}, &st);
        CHECK(!badR.ok); CHECK(!badR.retryable);
    }
    return testResult("test_worker_exec");
}
