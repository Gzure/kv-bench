/*
 * kv-bench cycle-counter clock（实现，见 clock.h 头注释）。
 * 移植自 umdk urma_perf.c / ub_get_clock.c 的思路。
 */
#include "clock.h"

#include <stdio.h>
#include <time.h>

namespace {

constexpr uint64_t kNsPerSec = 1000000000ULL;
/* mult 为 uint32_t，防溢出窗口与 urma_perf 一致（600s）。 */
constexpr uint64_t kMaxSec = 600;
constexpr uint32_t kMultBits = 32;

uint64_t g_mult = 0;   /* 0 = 未初始化/校准失败（kv_now_ns 退回 CLOCK_MONOTONIC） */
uint32_t g_shift = 0;
uint64_t g_freq_hz = 0;

uint64_t ReadCntfrq(void) {
#if defined(__aarch64__)
  uint64_t f;
  __asm__ __volatile__("mrs %0, cntfrq_el0" : "=r"(f));
  return f;
#else
  return 0;
#endif
}

/* Linux clocks_calc_mult_shift 同款：给定频率（Hz）求 mult/shift。 */
void CalcMultShift(uint64_t freq) {
  uint64_t tmp;
  uint32_t sftacc = kMultBits;
  uint32_t sft;

  if (freq == 0) {
    g_mult = 0;
    g_shift = 0;
    return;
  }
  /* 求最大安全 mult，避免 (cycles * mult) 溢出 */
  tmp = (kMaxSec * freq) >> kMultBits;
  while (tmp > 0) {
    tmp >>= 1;
    sftacc--;
  }
  /* 求最优 mult/shift 对 */
  for (sft = kMultBits; sft > 0; sft--) {
    tmp = ((kNsPerSec << sft) + freq / 2) / freq;
    if ((tmp >> sftacc) == 0) {
      break;
    }
  }
  g_mult = tmp;
  g_shift = sft;
}

/* 兜底校准：对 CLOCK_MONOTONIC 做最小二乘回归，估计 cycles/ns（参考
 * ub_get_clock.c gettime_get_cpu_mhz）。返回频率（Hz），失败返回 0。 */
uint64_t CalibrateFreq(void) {
  enum { kN = 200 };
  uint64_t xs[kN], ys[kN];

  for (int i = 0; i < kN; i++) {
    struct timespec t0, t1;
    uint64_t c0, c1;
    uint64_t target = (uint64_t)(100 + i * 10) * 1000ULL; /* 100+i*10 us */

    clock_gettime(CLOCK_MONOTONIC, &t0);
    c0 = kv_cycles();
    do {
      clock_gettime(CLOCK_MONOTONIC, &t1);
      int64_t dt = (int64_t)(t1.tv_sec - t0.tv_sec) * (int64_t)1000000000LL +
                   (int64_t)(t1.tv_nsec - t0.tv_nsec);
      if (dt >= (int64_t)target) {
        break;
      }
    } while (1);
    c1 = kv_cycles();
    xs[i] = (uint64_t)((int64_t)(t1.tv_sec - t0.tv_sec) * 1000000000LL +
                       (t1.tv_nsec - t0.tv_nsec));
    ys[i] = c1 - c0;
  }

  /* y = a + b x，b 单位 cycles/ns */
  long double sx = 0, sy = 0, sxx = 0, sxy = 0;
  for (int i = 0; i < kN; i++) {
    long double tx = (long double)xs[i];
    long double ty = (long double)ys[i];
    sx += tx;
    sy += ty;
    sxx += tx * tx;
    sxy += tx * ty;
  }
  if ((long double)kN * sxx <= sx * sx) {
    return 0;
  }
  long double b = ((long double)kN * sxy - sx * sy) /
                  ((long double)kN * sxx - sx * sx);
  if (b <= 0) {
    return 0;
  }
  return (uint64_t)(b * (long double)kNsPerSec);
}

}  // namespace

void kv_clock_init(void) {
  if (g_mult != 0) {
    return;
  }
  g_freq_hz = ReadCntfrq();
  if (g_freq_hz != 0) {
    CalcMultShift(g_freq_hz);
  }
  if (g_mult == 0) {
    g_freq_hz = CalibrateFreq();
    if (g_freq_hz != 0) {
      CalcMultShift(g_freq_hz);
    }
  }
  if (g_mult != 0) {
    fprintf(stderr, "[clock] cntvct freq=%llu Hz mult=%llu shift=%u\n",
            (unsigned long long)g_freq_hz, (unsigned long long)g_mult,
            (unsigned int)g_shift);
  } else {
    fprintf(stderr,
            "[clock] cntvct unavailable, falling back to CLOCK_MONOTONIC\n");
  }
}

uint64_t kv_now_ns(void) {
  if (g_mult == 0) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * kNsPerSec + (uint64_t)ts.tv_nsec;
  }
  return (kv_cycles() * g_mult) >> g_shift;
}
