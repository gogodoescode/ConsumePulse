#pragma once

#include <condition_variable>
#include <mutex>
#include <queue>

// Fixed-capacity thread-safe queue. push() blocks while full, so a slow
// consumer (e.g. a worker stuck retrying a dead DB) applies backpressure
// to whoever is pushing, instead of letting the queue grow without bound.
template <typename T>
class BoundedQueue {
public:
    explicit BoundedQueue(std::size_t capacity) : capacity_(capacity) {}

    // Returns false only if shutdown() was called while waiting for room.
    bool push(T item) {
        std::unique_lock<std::mutex> lock(mutex_);
        notFull_.wait(lock, [&] { return queue_.size() < capacity_ || shutdown_; });
        if (shutdown_) {
            return false;
        }
        queue_.push(std::move(item));
        lock.unlock();
        notEmpty_.notify_one();
        return true;
    }

    // Returns false once shutdown() was called and the queue has drained —
    // that's the signal for a worker to stop pulling and exit.
    bool pop(T& out) {
        std::unique_lock<std::mutex> lock(mutex_);
        notEmpty_.wait(lock, [&] { return !queue_.empty() || shutdown_; });
        if (queue_.empty()) {
            return false;
        }
        out = std::move(queue_.front());
        queue_.pop();
        lock.unlock();
        notFull_.notify_one();
        return true;
    }

    void shutdown() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            shutdown_ = true;
        }
        notEmpty_.notify_all();
        notFull_.notify_all();
    }

private:
    std::size_t capacity_;
    std::queue<T> queue_;
    std::mutex mutex_;
    std::condition_variable notEmpty_;
    std::condition_variable notFull_;
    bool shutdown_ = false;
};
