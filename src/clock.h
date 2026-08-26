/*
 * kv-bench cycle-counter clock（参考 umdk urma_perf / ub_get_clock）。
 *
 * - kv_cycles(): 原始周期计数。ARM64 读 CNTVCT_EL0（与内核 CLOCK_MONOTONIC
 *   vDSO 同一时基，全系统一致、跨核可比、线程迁移不影响数值）；x86_64 读
 *   rdtsc。刻意不加 isb —— urma_perf.c 注释：isb 序列化流水线 ~15-20ns，
 *   不加允许 10~20ns 抖动，换取 ~5ns 读取开销。
 * - kv_now_ns(): cycles -> ns，定点 mult/shift 换算（urma_perf.c 的
 *   urma_cntvct_calc_mult_shift，即内核 clocks_calc_mult_shift 同款算法）。
 *   频率优先读 CNTFRQ_EL0；取不到（EL0 不可读或为 0）则对 CLOCK_MONOTONIC
 *   做最小二乘回归校准（ub_get_clock.c gettime_get_cpu_mhz 的思路）。
 * - kv_clock_init(): 进程启动时调用一次（main 最前面）。mult/shift 状态放在
 *   clock.cpp 单一 TU，避免头文件 static 在各 TU 各持一份的经典坑。
 *
 * 注意：本线程 CPU 时间（CLOCK_THREAD_CPUTIME_ID）不能由周期计数器替代——
 * 周期计数器是墙钟，CPU 时间必须来自内核调度记账，两者之差才是被抢占时间。
 */
#ifndef KV_BENCH_CLOCK_H
#define KV_BENCH_CLOCK_H

#include <stdint.h>
#include <time.h> /* 非 arm/x86 兜底分支的 struct timespec */

#ifdef __cplusplus
extern "C" {
#endif

/* 原始周期计数（无状态，可内联热路径直接调用）。 */
static inline uint64_t kv_cycles(void) {
#if defined(__aarch64__)
  uint64_t c;
  __asm__ __volatile__("mrs %0, cntvct_el0" : "=r"(c) : : "memory");
  return c;
#elif defined(__x86_64__)
  uint32_t lo, hi;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((uint64_t)hi << 32) | lo;
#else
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
#endif
}

/* 校准 mult/shift 并输出 cntvct 频率（Hz）；失败时 kv_now_ns 退回
 * CLOCK_MONOTONIC。main 最前面调用一次即可（线程创建前）。 */
void kv_clock_init(void);

/* 周期计数 -> ns。kv_clock_init 之后无锁、单乘+移位。 */
uint64_t kv_now_ns(void);

#ifdef __cplusplus
}
#endif

#endif /* KV_BENCH_CLOCK_H */
