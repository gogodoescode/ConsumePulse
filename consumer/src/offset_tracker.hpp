#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <set>

// With concurrent workers, messages finish out of order. Committing the
// offset of whatever finished most recently is unsafe: if offset 100
// finishes before 97, committing 100 would let a restart skip 97 — silent
// data loss. This tracks, per partition, the highest contiguous run of
// completed offsets starting at the last committed point, and only ever
// exposes that as safe to commit. Writes are idempotent, so anything
// redelivered above the watermark after a crash is harmless.
class OffsetTracker {
public:
    // No-op if this partition has already been seen — call with the
    // offset of the first message dispatched for a partition in this run.
    void noteSeen(int32_t partition, int64_t offset) {
        std::lock_guard<std::mutex> lock(mutex_);
        nextExpected_.emplace(partition, offset);
    }

    void markComplete(int32_t partition, int64_t offset) {
        std::lock_guard<std::mutex> lock(mutex_);
        int64_t& next = nextExpected_[partition];
        if (offset < next) {
            return;  // already folded into the watermark
        }
        if (offset == next) {
            ++next;
            auto& pending = outOfOrder_[partition];
            while (!pending.empty() && *pending.begin() == next) {
                pending.erase(pending.begin());
                ++next;
            }
        } else {
            outOfOrder_[partition].insert(offset);
        }
    }

    // Partitions whose watermark moved since the last call, mapped to the
    // new commit-to offset (Kafka commit semantics: next offset to read).
    std::map<int32_t, int64_t> advancedSinceLastCommit() {
        std::lock_guard<std::mutex> lock(mutex_);
        std::map<int32_t, int64_t> result;
        for (const auto& [partition, next] : nextExpected_) {
            auto it = lastCommitted_.find(partition);
            if (it == lastCommitted_.end() || it->second != next) {
                result[partition] = next;
                lastCommitted_[partition] = next;
            }
        }
        return result;
    }

private:
    std::mutex mutex_;           // protects all the maps below
    std::map<int32_t, int64_t> nextExpected_;        // lowest offset not yet completed, per partition
    std::map<int32_t, std::set<int64_t>> outOfOrder_;     // offsets we've seen but can't fold into the watermark yet
    std::map<int32_t, int64_t> lastCommitted_;       // the last offset we returned for each partition
};
