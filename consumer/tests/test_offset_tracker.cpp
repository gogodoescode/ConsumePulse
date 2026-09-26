#include <algorithm>
#include <numeric>
#include <random>
#include <vector>

#include <catch2/catch_test_macros.hpp>

#include "../src/offset_tracker.hpp"

// The watermark is the most subtle logic in the consumer and the easiest
// place for a silent-data-loss bug to hide: if it ever commits past an
// offset that hasn't actually been persisted, a restart skips that message
// and nothing anywhere reports an error. These tests pin that invariant.

TEST_CASE("watermark stalls on the oldest outstanding offset", "[offset_tracker]") {
    OffsetTracker tracker;
    tracker.noteSeen(0, 97);

    // 98-100 finish first; 97 is still in flight on another worker.
    tracker.markComplete(0, 100);
    tracker.markComplete(0, 98);
    tracker.markComplete(0, 99);

    auto stalled = tracker.advancedSinceLastCommit();
    REQUIRE((stalled.empty() || stalled[0] == 97));

    // The moment 97 lands, the whole contiguous run collapses in at once
    // and the watermark jumps to 101 (Kafka commits the next offset to read).
    tracker.markComplete(0, 97);
    auto jumped = tracker.advancedSinceLastCommit();
    REQUIRE(jumped.count(0) == 1);
    REQUIRE(jumped[0] == 101);
}

TEST_CASE("duplicate and stale completions never double-advance", "[offset_tracker]") {
    OffsetTracker tracker;
    tracker.noteSeen(0, 10);

    tracker.markComplete(0, 10);
    tracker.markComplete(0, 10);  // redelivered after a crash
    tracker.markComplete(0, 9);   // already folded into the watermark

    auto advanced = tracker.advancedSinceLastCommit();
    REQUIRE(advanced[0] == 11);
}

TEST_CASE("partitions advance independently", "[offset_tracker]") {
    OffsetTracker tracker;
    tracker.noteSeen(0, 0);
    tracker.noteSeen(1, 500);

    tracker.markComplete(0, 0);
    tracker.markComplete(1, 501);  // 500 still outstanding on partition 1

    auto advanced = tracker.advancedSinceLastCommit();
    REQUIRE(advanced[0] == 1);
    REQUIRE((advanced.count(1) == 0 || advanced[1] == 500));
}

TEST_CASE("no redundant commit when nothing moved", "[offset_tracker]") {
    OffsetTracker tracker;
    tracker.noteSeen(0, 0);
    tracker.markComplete(0, 0);

    REQUIRE(tracker.advancedSinceLastCommit().count(0) == 1);
    REQUIRE(tracker.advancedSinceLastCommit().empty());
}

TEST_CASE("randomized completion orders never skip a message", "[offset_tracker]") {
    // The property that actually matters: whatever order workers finish in,
    // the committed watermark must never exceed the first offset that
    // hasn't completed. Anything above that would be silent data loss on
    // restart.
    constexpr int kMessages = 200;
    std::mt19937 rng(12345);  // fixed seed: a failure is reproducible

    for (int trial = 0; trial < 200; ++trial) {
        OffsetTracker tracker;
        tracker.noteSeen(0, 0);

        std::vector<int> order(kMessages);
        std::iota(order.begin(), order.end(), 0);
        std::shuffle(order.begin(), order.end(), rng);

        std::vector<bool> done(kMessages, false);
        for (int i = 0; i < kMessages; ++i) {
            tracker.markComplete(0, order[i]);
            done[order[i]] = true;

            int trueContiguousPrefix = 0;
            while (trueContiguousPrefix < kMessages && done[trueContiguousPrefix]) {
                ++trueContiguousPrefix;
            }

            auto advanced = tracker.advancedSinceLastCommit();
            if (advanced.count(0) == 1) {
                REQUIRE(advanced[0] <= trueContiguousPrefix);
            }
        }
    }
}
