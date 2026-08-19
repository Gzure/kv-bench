/*
 * kv-bench latency histogram (HdrHistogram-lite).
 *
 * Fixed-memory histogram with 3 significant decimal digits of resolution over
 * [lowest, highest] ns. Encoding follows the standard HdrHistogram layout:
 * sub-bucket (linear, 2^subBucketCountMagnitude entries) + power-of-two
 * bucket segments. Header-only so the bench can embed one instance per load
 * thread without any library dependency.
 */
#ifndef KV_BENCH_HIST_H
#define KV_BENCH_HIST_H

#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct kv_hist {
    int32_t unit_magnitude;
    int32_t sub_bucket_count_magnitude;
    int32_t sub_bucket_half_count_magnitude;
    int32_t sub_bucket_count;
    int32_t sub_bucket_half_count;
    int32_t bucket_count;
    int32_t counts_array_length;
    uint64_t sub_bucket_mask;
    uint64_t lowest_trackable;
    uint64_t highest_trackable;
    uint64_t total_count;
    uint64_t min_value;
    uint64_t max_value;
    uint64_t *counts;
} kv_hist_t;

static inline int32_t kv_hist_leading_zeros64(uint64_t v)
{
#if defined(__GNUC__) || defined(__clang__)
    return (int32_t)__builtin_clzll(v);
#else
    int32_t n = 0;
    while (v != 0) {
        v >>= 1;
        n++;
    }
    return 64 - n;
#endif
}

static inline int32_t kv_hist_floor_log2(uint64_t v)
{
    return (int32_t)(64 - kv_hist_leading_zeros64(v) - 1);
}

static inline int32_t kv_hist_ceil_log2(uint64_t v)
{
    if (v <= 1) {
        return 0;
    }
    int32_t fl = kv_hist_floor_log2(v);
    return (v == (1ULL << fl)) ? fl : fl + 1;
}

static inline int kv_hist_init(kv_hist_t *h, uint64_t lowest, uint64_t highest, int sigfigs)
{
    memset(h, 0, sizeof(*h));
    if (lowest == 0 || highest <= lowest) {
        return -1;
    }
    h->lowest_trackable = lowest;
    h->highest_trackable = highest;
    h->unit_magnitude = kv_hist_floor_log2(lowest);
    uint64_t largest_value_with_single_unit_resolution = 2 * 1000;
    if (sigfigs == 2) {
        largest_value_with_single_unit_resolution = 2 * 100;
    } else if (sigfigs == 1) {
        largest_value_with_single_unit_resolution = 2 * 10;
    } else if (sigfigs == 4) {
        largest_value_with_single_unit_resolution = 2 * 10000;
    } else if (sigfigs == 5) {
        largest_value_with_single_unit_resolution = 2 * 100000;
    }
    h->sub_bucket_count_magnitude = kv_hist_ceil_log2(largest_value_with_single_unit_resolution);
    h->sub_bucket_half_count_magnitude = h->sub_bucket_count_magnitude - 1;
    h->sub_bucket_count = 1 << h->sub_bucket_count_magnitude;
    h->sub_bucket_half_count = h->sub_bucket_count / 2;
    h->sub_bucket_mask = (uint64_t)h->sub_bucket_count - 1;

    // buckets needed to cover highest
    uint64_t trackable = ((uint64_t)h->sub_bucket_count - 1) << h->unit_magnitude;
    int32_t buckets = 1;
    while (trackable < highest) {
        trackable <<= 1;
        buckets++;
    }
    h->bucket_count = buckets;
    h->counts_array_length = (buckets + 1) * h->sub_bucket_half_count;
    h->counts = (uint64_t *)calloc((size_t)h->counts_array_length, sizeof(uint64_t));
    if (h->counts == nullptr) {
        return -1;
    }
    h->min_value = UINT64_MAX;
    h->max_value = 0;
    return 0;
}

static inline void kv_hist_destroy(kv_hist_t *h)
{
    free(h->counts);
    h->counts = nullptr;
    h->counts_array_length = 0;
}

static inline int32_t kv_hist_get_bucket_index(const kv_hist_t *h, uint64_t value)
{
    if (value < (uint64_t)h->sub_bucket_half_count) {
        return 0;
    }
    uint64_t pow2ceiling = (uint64_t)(64 - kv_hist_leading_zeros64(value | h->sub_bucket_mask));
    return (int32_t)pow2ceiling - h->sub_bucket_half_count_magnitude;
}

static inline int32_t kv_hist_get_sub_bucket_index(const kv_hist_t *h, uint64_t value, int32_t bucket_index)
{
    return (int32_t)(value >> (bucket_index + h->unit_magnitude));
}

static inline uint64_t kv_hist_value_from_index(const kv_hist_t *h, int32_t bucket_index, int32_t sub_bucket_index)
{
    return ((uint64_t)sub_bucket_index + 1) << (bucket_index + h->unit_magnitude);
}

static inline void kv_hist_record(kv_hist_t *h, uint64_t value)
{
    if (value < h->lowest_trackable) {
        value = h->lowest_trackable;
    } else if (value > h->highest_trackable) {
        value = h->highest_trackable;
    }
    int32_t bucket_index = kv_hist_get_bucket_index(h, value);
    int32_t sub_bucket_index = kv_hist_get_sub_bucket_index(h, value, bucket_index);
    int32_t index = (bucket_index << h->sub_bucket_half_count_magnitude) + sub_bucket_index;
    if (index < 0 || index >= h->counts_array_length) {
        return;
    }
    h->counts[index]++;
    h->total_count++;
    if (value < h->min_value) {
        h->min_value = value;
    }
    if (value > h->max_value) {
        h->max_value = value;
    }
}

static inline void kv_hist_merge(kv_hist_t *dst, const kv_hist_t *src)
{
    if (src == nullptr || src->counts == nullptr || dst->counts == nullptr) {
        return;
    }
    int32_t n = src->counts_array_length;
    if (n > dst->counts_array_length) {
        n = dst->counts_array_length;
    }
    for (int32_t i = 0; i < n; i++) {
        uint64_t c = src->counts[i];
        if (c != 0) {
            dst->counts[i] += c;
        }
    }
    dst->total_count += src->total_count;
    if (src->min_value < dst->min_value) {
        dst->min_value = src->min_value;
    }
    if (src->max_value > dst->max_value) {
        dst->max_value = src->max_value;
    }
}

static inline uint64_t kv_hist_value_at_percentile(const kv_hist_t *h, double percentile)
{
    if (h->total_count == 0) {
        return 0;
    }
    double requested = percentile;
    if (requested > 100.0) {
        requested = 100.0;
    }
    uint64_t count_at_percentile = (uint64_t)((requested / 100.0) * (double)h->total_count + 0.5);
    if (count_at_percentile == 0) {
        count_at_percentile = 1;
    }
    uint64_t total_to_current = 0;
    for (int32_t i = 0; i < h->counts_array_length; i++) {
        total_to_current += h->counts[i];
        if (total_to_current >= count_at_percentile) {
            int32_t bucket_index = i >> h->sub_bucket_half_count_magnitude;
            int32_t sub_bucket_index = i & (h->sub_bucket_half_count - 1);
            return kv_hist_value_from_index(h, bucket_index, sub_bucket_index);
        }
    }
    return h->max_value;
}

static inline double kv_hist_mean(const kv_hist_t *h)
{
    if (h->total_count == 0) {
        return 0.0;
    }
    double sum = 0.0;
    for (int32_t i = 0; i < h->counts_array_length; i++) {
        uint64_t c = h->counts[i];
        if (c != 0) {
            int32_t bucket_index = i >> h->sub_bucket_half_count_magnitude;
            int32_t sub_bucket_index = i & (h->sub_bucket_half_count - 1);
            sum += (double)kv_hist_value_from_index(h, bucket_index, sub_bucket_index) * (double)c;
        }
    }
    return sum / (double)h->total_count;
}

static inline uint64_t kv_hist_min(const kv_hist_t *h)
{
    return h->total_count == 0 ? 0 : h->min_value;
}

static inline uint64_t kv_hist_max(const kv_hist_t *h)
{
    return h->total_count == 0 ? 0 : h->max_value;
}

#ifdef __cplusplus
}
#endif

#endif /* KV_BENCH_HIST_H */
