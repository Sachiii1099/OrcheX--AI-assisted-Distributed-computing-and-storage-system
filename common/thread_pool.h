// Bounded thread pool: at most `threads` tasks run at once, at most `maxQueue`
// wait. submit() returns false (instead of blocking or growing without limit)
// when the queue is full. This is how a Worker enforces its concurrency limit.
#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace nexus {

class ThreadPool {
public:
    ThreadPool(size_t threads, size_t maxQueue) : maxQueue_(maxQueue) {
        if (threads < 1) threads = 1;
        for (size_t i = 0; i < threads; ++i) workers_.emplace_back([this] { loop(); });
    }
    ~ThreadPool() { shutdown(); }
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    bool submit(std::function<void()> job) {
        {
            std::lock_guard<std::mutex> lock(m_);
            if (stop_ || queue_.size() >= maxQueue_) return false;
            queue_.push_back(std::move(job));
        }
        cv_.notify_one();
        return true;
    }

    // Finishes everything already queued, then joins the threads.
    void shutdown() {
        {
            std::lock_guard<std::mutex> lock(m_);
            if (stop_) return;
            stop_ = true;
        }
        cv_.notify_all();
        for (auto& t : workers_) if (t.joinable()) t.join();
    }

    size_t size() const { return workers_.size(); }
    size_t active() const { return active_.load(); }
    size_t queued() const { std::lock_guard<std::mutex> lock(m_); return queue_.size(); }

private:
    void loop() {
        for (;;) {
            std::function<void()> job;
            {
                std::unique_lock<std::mutex> lock(m_);
                cv_.wait(lock, [this] { return stop_ || !queue_.empty(); });
                if (queue_.empty()) return;          // stop_ && nothing left
                job = std::move(queue_.front());
                queue_.pop_front();
                ++active_;
            }
            try { job(); } catch (...) {}
            --active_;
        }
    }

    size_t maxQueue_;
    mutable std::mutex m_;
    std::condition_variable cv_;
    std::deque<std::function<void()>> queue_;
    std::vector<std::thread> workers_;
    std::atomic<size_t> active_{0};
    bool stop_ = false;
};

}  // namespace nexus
