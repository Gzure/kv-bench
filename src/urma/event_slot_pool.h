#ifndef KV_BENCH_EVENT_SLOT_POOL_H
#define KV_BENCH_EVENT_SLOT_POOL_H

#include <array>
#include <atomic>
#include <cstdint>

namespace kv_bench {

/* 单 producer（业务 worker）申请/消费，单 completion 线程完成。
 * token = generation + slot index，迟到 CQE 无法命中新一代事件。 */
class EventSlotPool {
public:
  static constexpr uint32_t kSlotCount = 1024;
  static constexpr uint32_t kSlotBits = 10;
  static constexpr uint64_t kSlotMask = kSlotCount - 1;

  EventSlotPool() {
    for (uint32_t i = 0; i < kSlotCount; ++i) {
      states_[i].store(0, std::memory_order_relaxed);
      generations_[i] = 0;
      freeSlots_[i] = kSlotCount - 1 - i;
    }
  }

  bool Acquire(uint64_t &token) {
    token = 0;
    if (freeCount_ == 0) {
      return false;
    }

    const uint32_t index = freeSlots_[--freeCount_];
    uint32_t generation = ++generations_[index];
    if (generation == 0) {
      generation = ++generations_[index];
    }
    token = (static_cast<uint64_t>(generation) << kSlotBits) | index;

    uint64_t empty = 0;
    if (!states_[index].compare_exchange_strong(empty, Pending(token),
                                                std::memory_order_release,
                                                std::memory_order_relaxed)) {
      freeSlots_[freeCount_++] = index;
      token = 0;
      return false;
    }
    return true;
  }

  void Complete(uint64_t token, bool ok) {
    if (token == 0) {
      return;
    }
    const uint32_t index = Index(token);
    uint64_t pending = Pending(token);
    states_[index].compare_exchange_strong(pending, Done(token, ok),
                                           std::memory_order_acq_rel,
                                           std::memory_order_acquire);
  }

  /* 1=成功，-1=失败，0=尚未完成或 token 已失效。完成态会被消费并归还槽。 */
  int Probe(uint64_t token) {
    if (token == 0) {
      return 0;
    }
    const uint32_t index = Index(token);
    uint64_t value = states_[index].load(std::memory_order_acquire);
    const uint64_t ok = Done(token, true);
    const uint64_t fail = Done(token, false);
    if (value != ok && value != fail) {
      return 0;
    }
    if (!states_[index].compare_exchange_strong(
            value, 0, std::memory_order_acq_rel, std::memory_order_acquire)) {
      return 0;
    }
    Release(index);
    return value == ok ? 1 : -1;
  }

  /* 取消 pending/完成但未消费的同一代事件；旧 token 不会清掉新事件。 */
  bool Abort(uint64_t token) {
    if (token == 0) {
      return false;
    }
    const uint32_t index = Index(token);
    uint64_t value = states_[index].load(std::memory_order_acquire);
    while ((value >> 2) == token) {
      if (states_[index].compare_exchange_weak(
              value, 0, std::memory_order_acq_rel, std::memory_order_acquire)) {
        Release(index);
        return true;
      }
    }
    return false;
  }

  uint32_t Available() const { return freeCount_; }

private:
  static constexpr uint64_t Pending(uint64_t token) { return token << 2; }
  static constexpr uint64_t Done(uint64_t token, bool ok) {
    return (token << 2) | (ok ? 1ULL : 2ULL);
  }
  static constexpr uint32_t Index(uint64_t token) {
    return static_cast<uint32_t>(token & kSlotMask);
  }

  void Release(uint32_t index) { freeSlots_[freeCount_++] = index; }

  std::array<std::atomic<uint64_t>, kSlotCount> states_{};
  std::array<uint32_t, kSlotCount> generations_{};
  std::array<uint32_t, kSlotCount> freeSlots_{};
  uint32_t freeCount_{kSlotCount};
};

static_assert((EventSlotPool::kSlotCount & (EventSlotPool::kSlotCount - 1)) ==
                  0,
              "event slot count must be a power of two");

} // namespace kv_bench

#endif /* KV_BENCH_EVENT_SLOT_POOL_H */
