#include "urma/event_slot_pool.h"

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <vector>

using kv_bench::EventSlotPool;

static void Check(bool condition, const char *expression, int line) {
  if (!condition) {
    std::cerr << "CHECK failed at line " << line << ": " << expression << '\n';
    std::exit(1);
  }
}

#define CHECK(expression) Check((expression), #expression, __LINE__)

static void DelayedEventSurvivesMoreThanOneRingOfSubmissions() {
  EventSlotPool pool;
  uint64_t delayed = 0;
  CHECK(pool.Acquire(delayed));

  for (uint32_t i = 0; i < EventSlotPool::kSlotCount * 4; ++i) {
    uint64_t token = 0;
    CHECK(pool.Acquire(token));
    pool.Complete(token, true);
    CHECK(pool.Probe(token) == 1);
  }

  pool.Complete(delayed, true);
  CHECK(pool.Probe(delayed) == 1);
}

static void LateCompletionCannotCompleteReusedSlot() {
  EventSlotPool pool;
  uint64_t oldToken = 0;
  CHECK(pool.Acquire(oldToken));
  CHECK(pool.Abort(oldToken));

  uint64_t newToken = 0;
  CHECK(pool.Acquire(newToken));
  CHECK(newToken != oldToken);

  pool.Complete(oldToken, true);
  CHECK(pool.Probe(newToken) == 0);
  pool.Complete(newToken, true);
  CHECK(pool.Probe(newToken) == 1);

  uint64_t failedToken = 0;
  CHECK(pool.Acquire(failedToken));
  pool.Complete(failedToken, false);
  CHECK(pool.Probe(failedToken) == -1);
}

static void ExhaustionBackpressuresInsteadOfOverwriting() {
  EventSlotPool pool;
  std::vector<uint64_t> tokens(EventSlotPool::kSlotCount);
  for (uint64_t &token : tokens) {
    CHECK(pool.Acquire(token));
  }

  uint64_t extra = 0;
  CHECK(!pool.Acquire(extra));

  for (uint64_t token : tokens) {
    pool.Abort(token);
  }
  CHECK(pool.Available() == EventSlotPool::kSlotCount);
}

int main() {
  DelayedEventSurvivesMoreThanOneRingOfSubmissions();
  LateCompletionCannotCompleteReusedSlot();
  ExhaustionBackpressuresInsteadOfOverwriting();
  std::cout << "event_slot_pool_test: PASS\n";
  return 0;
}
