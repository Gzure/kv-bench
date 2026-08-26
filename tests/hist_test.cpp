#include "hist.h"

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <thread>

static void Check(bool condition, const char *expression, int line) {
  if (!condition) {
    std::cerr << "CHECK failed at line " << line << ": " << expression << '\n';
    std::exit(1);
  }
}

#define CHECK(expression) Check((expression), #expression, __LINE__)

static void SnapshotDeltaContainsOnlyNewSamples() {
  kv_hist_t recorded = {}, previous = {}, current = {}, interval = {};
  CHECK(kv_hist_init(&recorded, 1, 1000000, 3) == 0);
  CHECK(kv_hist_init(&previous, 1, 1000000, 3) == 0);
  CHECK(kv_hist_init(&current, 1, 1000000, 3) == 0);
  CHECK(kv_hist_init(&interval, 1, 1000000, 3) == 0);

  kv_hist_record(&recorded, 100);
  kv_hist_record(&recorded, 200);
  kv_hist_merge_snapshot(&current, &recorded);
  kv_hist_copy(&previous, &current);

  kv_hist_record(&recorded, 300);
  kv_hist_record(&recorded, 400);
  kv_hist_reset(&current);
  kv_hist_merge_snapshot(&current, &recorded);
  kv_hist_delta(&interval, &current, &previous);
  CHECK(interval.total_count == 2);
  CHECK(kv_hist_value_at_percentile(&interval, 50.0) >= 300);
  CHECK(kv_hist_value_at_percentile(&interval, 100.0) >= 400);

  kv_hist_copy(&previous, &current);
  kv_hist_delta(&interval, &current, &previous);
  CHECK(interval.total_count == 0);
  CHECK(kv_hist_min(&interval) == 0);
  CHECK(kv_hist_max(&interval) == 0);

  kv_hist_destroy(&recorded);
  kv_hist_destroy(&previous);
  kv_hist_destroy(&current);
  kv_hist_destroy(&interval);
}

static void ConcurrentSnapshotsAreMonotonicAndComplete() {
  constexpr uint64_t kSamples = 100000;
  kv_hist_t recorded = {}, snapshot = {};
  CHECK(kv_hist_init(&recorded, 1, 1000000, 3) == 0);
  CHECK(kv_hist_init(&snapshot, 1, 1000000, 3) == 0);
  std::atomic<bool> done{false};

  std::thread producer([&] {
    for (uint64_t i = 0; i < kSamples; ++i)
      kv_hist_record(&recorded, i % 1000 + 1);
    done.store(true, std::memory_order_release);
  });

  uint64_t last_count = 0;
  while (!done.load(std::memory_order_acquire)) {
    kv_hist_reset(&snapshot);
    kv_hist_merge_snapshot(&snapshot, &recorded);
    CHECK(snapshot.total_count >= last_count);
    last_count = snapshot.total_count;
  }
  producer.join();

  kv_hist_reset(&snapshot);
  kv_hist_merge_snapshot(&snapshot, &recorded);
  CHECK(snapshot.total_count == kSamples);
  kv_hist_destroy(&recorded);
  kv_hist_destroy(&snapshot);
}

int main() {
  SnapshotDeltaContainsOnlyNewSamples();
  ConcurrentSnapshotsAreMonotonicAndComplete();
  std::cout << "hist_test: PASS\n";
  return 0;
}
