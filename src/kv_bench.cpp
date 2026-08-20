#define _GNU_SOURCE

/*
 * kv-bench 业务层：选项 / 亲和(chip) / 打流引擎 / get 请求环与回写 / 统计。
 *
 * 分层（对齐 yuanrong-datasystem）：
 *   业务层(kv_bench.cpp) -> 管理层(urma/urma_manager) -> 资源层(urma/urma_resource) -> liburma
 * 数据通路只走 URMA：
 *   write(Put): 客户端每轮从 bonding 双源 CPU 并发 2 条 WRITE 直写服务器内存
 *               （--dual-mode mirror: 各发满 size, 轮数据量 2*size; split: 各 size/2）。
 *   get(Get):   客户端 WRITE 32B 请求进服务器请求环；服务器回写线程并发 2 条
 *               WRITE（数据 + 完成标志 2-sge）回写客户端缓冲，客户端等双 flag 完成
 *               （对齐 datasystem worker UbWriteHelper -> UrmaWritePayload 模型）。
 * 统计：每线程 HdrHistogram-lite，输出 avg/p50/p90/p99/p999/p9999/pmax + 带宽/IOPS。
 * 亲和：affinity(源/目的固定 CPU + 双 chip)/anti(源随机&目的固定)/none(双随机)。
 * 无 bthread/brpc/protobuf 依赖。
 */

#include <stdlib.h>
#include <stdio.h>
#include <stddef.h>
#include <string.h>
#include <unistd.h>
#include <inttypes.h>
#include <getopt.h>
#include <pthread.h>
#include <malloc.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <time.h>
#include <sched.h>
#include <dirent.h>
#include <sys/syscall.h>
#include <linux/mempolicy.h>
#include "sys/mman.h"
#include "urma/urma_manager.h"
#include "hist.h"

#define PAGE_SHIFT 12
#define PAGE_SIZE (0x1 << PAGE_SHIFT) /* 4KB */
#define DEFAULT_VALUE_SIZE (4UL * 1024 * 1024)
#define MAX_JETTY_COUNT 200
#define MAX_CLIENT_CNT 10
#define POLL_SLEEP_NS 1000L /* 1us */
#define DEFAULT_PORT 13857
#define GET_REQ_SIZE 32
#define RING_SLOTS 4096
#define RING_BYTES ((uint64_t)RING_SLOTS * GET_REQ_SIZE)
#define FLAG_PER_THREAD 16 /* 2 x 8B */
#define DATA_WINDOW_PER_THREAD 4 /* 客户端 data_len = threads * 4 * value_size */
#define SERVER_DATA_WINDOW 4
#define DEFAULT_TIMEOUT_MS 5000
#define MAX_CPUS 1024
#define INVALID_CHIP 0xFFu

/* 操作类型 / 双发模式 / 亲和模式 */
enum { OP_WRITE = 0, OP_GET = 1, OP_MIXED = 2 };
enum { DUAL_MIRROR = 0, DUAL_SPLIT = 1 };
enum { AFF_AFFINITY = 0, AFF_ANTI = 1, AFF_NONE = 2 };

typedef struct argument {
    char *dev_name;
    char *server_ip;
    unsigned int server_port;
    unsigned int trans_mode; /* 0=RM 1=RC 2=UM 3=RS */
    bool multi_path;
    unsigned int tp_type;
    bool event_mode;
    uint64_t value_size;
    uint64_t qps;
    uint64_t duration_sec;
    uint32_t jetty_count;
    int affinity_mode;
    const char *source_cpus;
    const char *destination_cpus;
    bool cacheable;
    uint32_t threads;
    int op;
    uint32_t mixed_ratio;
    int dual_mode;
    uint32_t report_interval;
    uint32_t server_workers; /* 0=auto(=client threads) */
    bool get_fence;
    bool no_mbind;
    uint32_t seed;
    bool fixed_offset;
    int timeout_ms;
} argument_t;

/* 客户端 -> 服务器 请求（seq 放最后，读方 acquire 保证前置字段可见）; 32B packed */
typedef struct get_req {
    uint64_t client_seg_va; /* 客户端 DataArea 起始 VA */
    uint64_t client_off;
    uint32_t size;
    uint32_t flag_off;
    uint64_t seq;
} __attribute__((packed)) get_req_t;

typedef struct context context_t;

typedef struct worker {
    uint32_t id; /* UrmaManager workerId（进程级全局） */
    uint32_t local_index; /* 本连接/本进程内序号（get 环槽位分配用） */
    kv_hist_t hist;
    volatile uint64_t ops;
    volatile uint64_t bytes;
    volatile uint64_t errors;
    uint64_t off; /* 客户端数据偏移（窗口内） */
    uint64_t next_post_ns;
    uint32_t rng;
    uint64_t get_seq;
    uint32_t *seen; /* 服务器 get 回写: RING_SLOTS */
    bool stop;
    pthread_t tid;
    void *run_arg;
} worker_t;

typedef struct conn {
    int fd;
    bool used;
    context_t *ctx;
    std::shared_ptr<kv_bench::UrmaConnection> conn; /* 对端连接（import 结果） */
    worker_t *workers;
    uint32_t worker_count;
    pthread_t tid;
} conn_t;

struct context {
    argument_t args;
    kv_bench::UrmaManager *mgr; /* 管理层（单例/进程） */
    std::shared_ptr<kv_bench::UrmaConnection> conn; /* 客户端单连接 */

    void *va; /* 整个注册缓冲 */
    uint64_t buf_len;
    uint64_t req_bytes; /* 客户端: threads*32 */
    uint64_t data_len; /* 客户端: threads*4*size; 服务器: 4*size */
    uint64_t flag_bytes;
    uint8_t *client_data; /* 客户端: va+req_bytes; 服务器: va+RING_BYTES */
    uint8_t *client_flags; /* 客户端: va+req_bytes+data_len */

    worker_t *workers;
    uint32_t worker_count;
    volatile bool stop;

    /* 服务器 */
    int listen_fd;
    bool server_stop;
    pthread_t sock_thread;
    conn_t conns[MAX_CLIENT_CNT];
    uint64_t *server_flag_slot;
};

/* ---------------- 基础工具 ---------------- */

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

static void sleep_ns(uint64_t ns)
{
    struct timespec ts = {.tv_sec = 0, .tv_nsec = (long)ns};
    nanosleep(&ts, NULL);
}

static uint32_t rng_next(uint32_t *state)
{
    uint32_t x = *state;
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    *state = x;
    return x;
}

static uint64_t parse_size(const char *text)
{
    char *end = NULL;
    uint64_t value = strtoull(text, &end, 0);
    if (end == text) return 0;
    if (*end == 'K' || *end == 'k') value *= 1024ULL;
    else if (*end == 'M' || *end == 'm') value *= 1024ULL * 1024ULL;
    else if (*end == 'G' || *end == 'g') value *= 1024ULL * 1024ULL * 1024ULL;
    else if (*end != '\0') return 0;
    return value;
}

/* ---------------- CPU / NUMA / chip（对齐参考 NumaIdToChipId: 双 chip 模型） ---------------- */

static int g_cpu_numa[MAX_CPUS];
static bool g_cpu_numa_known[MAX_CPUS];

static int read_cpu_numa(int cpu)
{
    if (cpu >= 0 && cpu < MAX_CPUS && g_cpu_numa_known[cpu]) {
        return g_cpu_numa[cpu];
    }
    char path[128];
    DIR *dir = NULL;
    struct dirent *ent = NULL;
    int node = -1;
    snprintf(path, sizeof(path), "/sys/devices/system/cpu/cpu%d", cpu);
    dir = opendir(path);
    if (dir == NULL) {
        return -1;
    }
    while ((ent = readdir(dir)) != NULL) {
        if (strncmp(ent->d_name, "node", 4) == 0 && ent->d_name[4] >= '0' && ent->d_name[4] <= '9') {
            node = atoi(ent->d_name + 4);
            break;
        }
    }
    closedir(dir);
    if (cpu >= 0 && cpu < MAX_CPUS) {
        g_cpu_numa_known[cpu] = (node >= 0);
        g_cpu_numa[cpu] = node;
    }
    return node;
}

static int get_num_numa_nodes(void)
{
    static int count = -1;
    if (count >= 0) {
        return count;
    }
    bool seen[64] = {false};
    int n = 0;
    long ncpu = sysconf(_SC_NPROCESSORS_CONF);
    for (long c = 0; c < ncpu && c < MAX_CPUS; c++) {
        int node = read_cpu_numa((int)c);
        if (node >= 0 && node < 64 && !seen[node]) {
            seen[node] = true;
            n++;
        }
    }
    count = n > 0 ? n : 1;
    return count;
}

/* 双 chip 模型: 前一半 NUMA -> chip 1, 后一半 -> chip 2（对齐参考 NumaIdToChipId） */
static int numa_to_chip(int numa)
{
    int numa_count = get_num_numa_nodes();
    if (numa < 0 || numa_count <= 0) {
        return -1;
    }
    int first_half = (numa_count + 1) / 2;
    return numa < first_half ? 1 : 2;
}

static int cpu_to_chip(int cpu)
{
    return numa_to_chip(read_cpu_numa(cpu));
}

static int pin_thread_to_cpu(int cpu)
{
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    return sched_setaffinity(0, sizeof(set), &set);
}

static int apply_cpu_list(const char *list)
{
    if (list == NULL || *list == '\0') return 0;
    cpu_set_t set;
    CPU_ZERO(&set);
    char *copy = strdup(list);
    if (copy == NULL) return -1;
    for (char *part = strtok(copy, ","); part != NULL; part = strtok(NULL, ",")) {
        char *end = NULL;
        long cpu = strtol(part, &end, 10);
        if (end == part || *end != '\0' || cpu < 0 || cpu >= CPU_SETSIZE) {
            free(copy);
            return -1;
        }
        CPU_SET((int)cpu, &set);
    }
    int ret = sched_setaffinity(0, sizeof(set), &set);
    free(copy);
    return ret;
}

/* 解析 "4,6-8" 到数组，返回个数 */
static int parse_cpu_list(const char *list, int *out, int max_out)
{
    int n = 0;
    if (list == NULL || *list == '\0') return 0;
    char *copy = strdup(list);
    if (copy == NULL) return 0;
    for (char *part = strtok(copy, ","); part != NULL && n < max_out; part = strtok(NULL, ",")) {
        int lo = -1, hi = -1;
        char *dash = strchr(part, '-');
        if (dash != NULL) {
            *dash = '\0';
            lo = atoi(part);
            hi = atoi(dash + 1);
        } else {
            lo = atoi(part);
            hi = lo;
        }
        if (lo < 0 || hi < lo) continue;
        for (int c = lo; c <= hi && n < max_out; c++) {
            out[n++] = c;
        }
    }
    free(copy);
    return n;
}

static int enumerate_all_cpus(int *out, int max_out)
{
    long ncpu = sysconf(_SC_NPROCESSORS_CONF);
    if (ncpu > max_out) ncpu = max_out;
    int n = 0;
    for (long c = 0; c < ncpu; c++) {
        out[n++] = (int)c;
    }
    return n;
}

/* mbind: 把 [addr, addr+len) 绑定到 node（用 syscall 避免 libnuma 依赖） */
/* mbind: 把 [addr, addr+len) 绑定到 node（用 syscall 避免 libnuma 依赖）。
 * addr 需页对齐；掩码用满字长数组，maxnode 取系统实际节点数（≤ MAX_NUMNODES）。 */
static int mbind_to_node(void *addr, size_t len, int node)
{
    int num_nodes = get_num_numa_nodes();
    if (node < 0 || num_nodes <= 0) return -1;
    uintptr_t start = (uintptr_t)addr & ~(uintptr_t)(PAGE_SIZE - 1);
    size_t ext = ((uintptr_t)addr - start) + len;
    unsigned long mask[64] = {0}; /* 512B = 4096 bit，足够 MAX_NUMNODES */
    mask[node / (8 * sizeof(unsigned long))] = 1UL << (node % (8 * sizeof(unsigned long)));
    long rc = syscall(SYS_mbind, (void *)start, ext, MPOL_BIND, mask, num_nodes, 0);
    if (rc != 0) {
        fprintf(stderr, "Warning: mbind(addr=0x%lx len=%zu node=%d) failed, errno=%d (%s)\n",
                (unsigned long)start, ext, node, errno, strerror(errno));
        return -1;
    }
    return 0;
}

/* ---------------- 缓冲布局 ---------------- */

static int layout_client_buffer(context_t *ctx)
{
    const argument_t *args = &ctx->args;
    uint64_t size = args->value_size;
    ctx->req_bytes = (uint64_t)args->threads * GET_REQ_SIZE;
    ctx->data_len = (uint64_t)args->threads * DATA_WINDOW_PER_THREAD * size;
    ctx->flag_bytes = (uint64_t)args->threads * FLAG_PER_THREAD;
    ctx->buf_len = ctx->req_bytes + ctx->data_len + ctx->flag_bytes;
    ctx->client_data = (uint8_t *)ctx->va + ctx->req_bytes;
    ctx->client_flags = ctx->client_data + ctx->data_len;
    return 0;
}

static int layout_server_buffer(context_t *ctx)
{
    const argument_t *args = &ctx->args;
    uint64_t size = args->value_size;
    ctx->req_bytes = 0;
    ctx->data_len = (uint64_t)SERVER_DATA_WINDOW * size;
    ctx->flag_bytes = 0;
    ctx->buf_len = RING_BYTES + ctx->data_len + 16; /* +16 回写标志源槽 */
    ctx->client_data = (uint8_t *)ctx->va + RING_BYTES;
    ctx->client_flags = NULL;
    return 0;
}

/* 分配 + 注册缓冲，并按亲和 mbind 数据区 */
static int setup_buffer(context_t *ctx, bool is_server)
{
    const argument_t *args = &ctx->args;
    if (is_server) {
        layout_server_buffer(ctx);
    } else {
        layout_client_buffer(ctx);
    }
    ctx->va = memalign(PAGE_SIZE, ctx->buf_len);
    if (ctx->va == NULL) {
        fprintf(stderr, "Failed to alloc buffer of %" PRIu64 " bytes\n", ctx->buf_len);
        return -1;
    }
    (void)memset(ctx->va, 0, ctx->buf_len);
    if (!ctx->mgr->RegisterBuffer(ctx->va, ctx->buf_len)) {
        fprintf(stderr, "Failed to register buffer\n");
        return -1;
    }
    if (!args->no_mbind && args->affinity_mode == AFF_AFFINITY) {
        int dst_cpus[MAX_CPUS];
        int n = parse_cpu_list(args->destination_cpus, dst_cpus, MAX_CPUS);
        if (n > 0) {
            int node = read_cpu_numa(dst_cpus[0]);
            if (node >= 0) {
                /* 整缓冲绑定（va 由 memalign 页对齐），保证数据区落在目的 NUMA */
                if (mbind_to_node(ctx->va, ctx->buf_len, node) != 0) {
                    fprintf(stderr, "Warning: mbind to node %d failed, continue without NUMA binding\n", node);
                } else {
                    printf("mbind buffer (%" PRIu64 "B) to node %d\n", ctx->buf_len, node);
                }
            }
        }
    }
    if (is_server) {
        ctx->server_flag_slot = (uint64_t *)((uint8_t *)ctx->va + RING_BYTES + ctx->data_len);
    }
    return 0;
}

/* ---------------- 亲和: 每轮取源/目的 chip ---------------- */

static int my_cpu_list(const argument_t *args, bool is_client, int *out, int max_out)
{
    const char *list = is_client ? args->source_cpus : args->destination_cpus;
    int n = parse_cpu_list(list, out, max_out);
    if (n > 0) {
        return n;
    }
    return enumerate_all_cpus(out, max_out);
}

static int first_dst_chip(const argument_t *args)
{
    int dst_cpus[MAX_CPUS];
    int n = parse_cpu_list(args->destination_cpus, dst_cpus, MAX_CPUS);
    if (n == 0) {
        n = enumerate_all_cpus(dst_cpus, MAX_CPUS);
    }
    for (int i = 0; i < n; i++) {
        int chip = cpu_to_chip(dst_cpus[i]);
        if (chip > 0) return chip;
    }
    return -1;
}

static void pick_round_chips(const argument_t *args, bool is_client, worker_t *w, int *src_a, int *src_b, int *dst)
{
    int all_cpus[MAX_CPUS];
    int n_all = enumerate_all_cpus(all_cpus, MAX_CPUS);
    int mine[MAX_CPUS];
    int n_mine = my_cpu_list(args, is_client, mine, MAX_CPUS);

    int dst_chip = first_dst_chip(args);
    if (dst_chip < 0) {
        dst_chip = (int)(w->id % 2) + 1;
    }

    if (args->affinity_mode == AFF_AFFINITY) {
        int my_chip = -1;
        if (n_mine > 0) {
            my_chip = cpu_to_chip(mine[w->id % n_mine]);
        }
        if (my_chip < 0) {
            my_chip = (int)(w->id % 2) + 1;
        }
        *src_a = my_chip;
        *src_b = (my_chip == 1) ? 2 : 1;
        *dst = dst_chip;
    } else if (args->affinity_mode == AFF_ANTI) {
        int pool[MAX_CPUS];
        int np = n_mine > 0 ? n_mine : n_all;
        for (int i = 0; i < np; i++) pool[i] = n_mine > 0 ? mine[i] : all_cpus[i];
        int c0 = pool[rng_next(&w->rng) % np];
        int c1 = pool[rng_next(&w->rng) % np];
        int ch0 = cpu_to_chip(c0);
        int ch1 = cpu_to_chip(c1);
        *src_a = ch0 > 0 ? ch0 : ((int)(w->id % 2) + 1);
        *src_b = ch1 > 0 ? ch1 : ((*src_a == 1) ? 2 : 1);
        *dst = dst_chip;
    } else {
        int c0 = all_cpus[rng_next(&w->rng) % n_all];
        int c1 = all_cpus[rng_next(&w->rng) % n_all];
        int c2 = all_cpus[rng_next(&w->rng) % n_all];
        int ch0 = cpu_to_chip(c0);
        int ch1 = cpu_to_chip(c1);
        int ch2 = cpu_to_chip(c2);
        *src_a = ch0 > 0 ? ch0 : 1;
        *src_b = ch1 > 0 ? ch1 : 2;
        *dst = ch2 > 0 ? ch2 : 1;
    }
}

/* ---------------- 客户端打流 ---------------- */

static int client_do_write(context_t *ctx, worker_t *w, int src_a, int src_b, int dst_chip)
{
    const argument_t *args = &ctx->args;
    kv_bench::UrmaManager *mgr = ctx->mgr;
    kv_bench::UrmaConnection &conn = *ctx->conn;
    uint32_t size = (uint32_t)args->value_size;
    uint32_t wr_len = (args->dual_mode == DUAL_MIRROR) ? size : size / 2;
    uint64_t off_a = w->off;
    uint64_t off_b = w->off + wr_len;
    uint64_t server_data_len = (uint64_t)SERVER_DATA_WINDOW * size;
    /* 服务器数据区在服务器段 RING_BYTES 之后 */
    uint64_t remote_a = conn.RemoteSegVa() + RING_BYTES + (off_a % server_data_len);
    uint64_t remote_b = conn.RemoteSegVa() + RING_BYTES + (off_b % server_data_len);

    /* 每轮从 jetty 池取一条新的 send lane（对齐 yuanrong AcquireSendLane） */
    std::shared_ptr<kv_bench::UrmaJetty> jetty;
    if (!mgr->AcquireSendLane(jetty)) {
        fprintf(stderr, "[wr] send lane pool exhausted\n");
        return -1;
    }

    uint64_t seq_a, seq_b;
    uint64_t ua = mgr->PostEvent(w->id, seq_a);
    uint64_t ub = mgr->PostEvent(w->id, seq_b);
    if (!mgr->PostWrite(jetty, conn, (uint64_t)ctx->client_data + off_a, remote_a, wr_len,
                        (uint32_t)src_a, (uint32_t)dst_chip, ua)) {
        fprintf(stderr, "[wr] post A failed\n");
        mgr->AbortEvent(w->id, seq_a);
        mgr->ReleaseSendLane(jetty);
        return -1;
    }
    if (!mgr->PostWrite(jetty, conn, (uint64_t)ctx->client_data + off_b, remote_b, wr_len,
                        (uint32_t)src_b, (uint32_t)dst_chip, ub)) {
        fprintf(stderr, "[wr] post B failed\n");
        mgr->AbortEvent(w->id, seq_b);
        (void)mgr->WaitEvent(w->id, seq_a, args->timeout_ms); /* 等 A 完成避免残留 */
        mgr->ReleaseSendLane(jetty);
        return -1;
    }
    bool ok_a = mgr->WaitEvent(w->id, seq_a, args->timeout_ms);
    bool ok_b = mgr->WaitEvent(w->id, seq_b, args->timeout_ms);
    mgr->ReleaseSendLane(jetty);
    return (ok_a && ok_b) ? 0 : -1;
}

static int client_do_get(context_t *ctx, worker_t *w, int src_a, int dst_chip)
{
    const argument_t *args = &ctx->args;
    kv_bench::UrmaManager *mgr = ctx->mgr;
    kv_bench::UrmaConnection &conn = *ctx->conn;
    uint64_t seq = ++w->get_seq;
    uint32_t slot = (uint32_t)(seq % RING_SLOTS);
    get_req_t *req = (get_req_t *)((uint8_t *)ctx->va + (uint64_t)w->id * GET_REQ_SIZE);
    req->client_seg_va = (uint64_t)ctx->client_data;
    req->client_off = w->off;
    req->size = (uint32_t)args->value_size;
    req->flag_off = (uint32_t)w->id * FLAG_PER_THREAD;
    __atomic_store_n(&req->seq, seq, __ATOMIC_RELEASE);

    /* 每轮从 jetty 池取一条新的 send lane 发请求 */
    std::shared_ptr<kv_bench::UrmaJetty> jetty;
    if (!mgr->AcquireSendLane(jetty)) {
        fprintf(stderr, "[get] send lane pool exhausted\n");
        return -1;
    }

    uint64_t seq_e;
    uint64_t ue = mgr->PostEvent(w->id, seq_e);
    if (!mgr->PostWrite(jetty, conn, (uint64_t)req,
                        conn.RemoteSegVa() + (uint64_t)slot * GET_REQ_SIZE, GET_REQ_SIZE,
                        (uint32_t)src_a, (uint32_t)dst_chip, ue)) {
        fprintf(stderr, "[get] post request failed\n");
        mgr->AbortEvent(w->id, seq_e);
        mgr->ReleaseSendLane(jetty);
        return -1;
    }
    if (!mgr->WaitEvent(w->id, seq_e, args->timeout_ms)) {
        mgr->ReleaseSendLane(jetty);
        return -1;
    }
    mgr->ReleaseSendLane(jetty);

    /* 等服务器回写双 flag（服务器 WRITE 落客户端内存，acquire 保证数据可见） */
    uint64_t *flag_a = (uint64_t *)(ctx->client_flags + (uint64_t)w->id * FLAG_PER_THREAD);
    uint64_t *flag_b = flag_a + 1;
    uint64_t deadline = now_ns() + (uint64_t)args->timeout_ms * 1000000ULL;
    bool done = false;
    while (!done) {
        uint64_t va = __atomic_load_n(flag_a, __ATOMIC_ACQUIRE);
        uint64_t vb = __atomic_load_n(flag_b, __ATOMIC_ACQUIRE);
        if (va == seq && vb == seq) {
            done = true;
            break;
        }
        if (now_ns() >= deadline) {
            break;
        }
        sleep_ns(POLL_SLEEP_NS);
    }
    return done ? 0 : -1;
}

static void *client_worker_main(void *arg)
{
    worker_t *w = (worker_t *)arg;
    context_t *ctx = (context_t *)w->run_arg;
    const argument_t *args = &ctx->args;

    /* 亲和: 线程绑定到本线程的源 CPU */
    if (args->affinity_mode == AFF_AFFINITY) {
        int mine[MAX_CPUS];
        int n = my_cpu_list(args, true, mine, MAX_CPUS);
        if (n > 0) {
            int cpu = mine[w->id % n];
            if (pin_thread_to_cpu(cpu) != 0) {
                fprintf(stderr, "[worker %u] pin to cpu %d failed: %s\n", w->id, cpu, strerror(errno));
            } else {
                printf("[worker %u] pinned to cpu %d\n", w->id, cpu);
            }
        }
    }

    uint64_t interval_ns = 0;
    if (args->qps > 0) {
        uint64_t per_thread = args->qps / args->threads;
        if (per_thread == 0) per_thread = 1;
        interval_ns = 1000000000ULL / per_thread;
    }
    uint64_t deadline = now_ns() + args->duration_sec * 1000000000ULL;
    w->next_post_ns = now_ns();
    uint64_t round_bytes = (args->dual_mode == DUAL_MIRROR) ? 2ULL * args->value_size : args->value_size;
    uint64_t window = (uint64_t)args->threads * DATA_WINDOW_PER_THREAD * args->value_size;
    w->off = (uint64_t)w->id * DATA_WINDOW_PER_THREAD * args->value_size;

    while (!w->stop && now_ns() < deadline) {
        uint64_t now = now_ns();
        if (interval_ns > 0) {
            if (now < w->next_post_ns) {
                uint64_t left = w->next_post_ns - now;
                if (left > 50000) {
                    sleep_ns(left - 50000);
                }
                while (now_ns() < w->next_post_ns) {
                }
            }
        }

        int src_a, src_b, dst_chip;
        pick_round_chips(args, true, w, &src_a, &src_b, &dst_chip);
        /* 目的 chip 优先用服务器握手通告值（与服务器 --destination-cpus 一致） */
        if (ctx->conn != nullptr && ctx->conn->peer.dstChip != INVALID_CHIP) {
            dst_chip = (int)ctx->conn->peer.dstChip;
        }

        uint64_t t0 = now_ns();
        int rc;
        if (args->op == OP_WRITE) {
            rc = client_do_write(ctx, w, src_a, src_b, dst_chip);
        } else if (args->op == OP_GET) {
            rc = client_do_get(ctx, w, src_a, dst_chip);
        } else {
            if (rng_next(&w->rng) % 100 < args->mixed_ratio) {
                rc = client_do_write(ctx, w, src_a, src_b, dst_chip);
            } else {
                rc = client_do_get(ctx, w, src_a, dst_chip);
            }
        }
        uint64_t t1 = now_ns();

        if (rc == 0) {
            kv_hist_record(&w->hist, t1 - t0);
            __atomic_add_fetch(&w->ops, 1, __ATOMIC_RELAXED);
            __atomic_add_fetch(&w->bytes, round_bytes, __ATOMIC_RELAXED);
        } else {
            __atomic_add_fetch(&w->errors, 1, __ATOMIC_RELAXED);
        }

        if (!args->fixed_offset) {
            w->off += round_bytes;
            w->off = w->off % window;
        }
        w->next_post_ns = t0 + interval_ns;
    }
    return NULL;
}

/* ---------------- 服务器 get 回写 ---------------- */

static int server_write_back(context_t *ctx, conn_t *conn, worker_t *w, const get_req_t *req,
                             int src_a, int src_b, int dst_chip)
{
    kv_bench::UrmaManager *mgr = ctx->mgr;
    kv_bench::UrmaConnection &remote = *conn->conn;
    uint32_t wr_len = (conn->conn->peer.dualMode == DUAL_MIRROR) ? req->size : req->size / 2;
    uint64_t s_off = req->client_off % ctx->data_len; /* 服务器本地数据源偏移 */
    uint64_t d_off = req->client_off; /* 客户端目标偏移（相对客户端数据区） */
    /* 客户端 flag 地址 = client_seg_va(数据区) + 客户端 data_len + flag_off */
    uint64_t client_data_len = (uint64_t)conn->conn->peer.threads * DATA_WINDOW_PER_THREAD * req->size;
    uint64_t flag_remote = req->client_seg_va + client_data_len + req->flag_off;
    uint64_t flag_local = (uint64_t)ctx->server_flag_slot;

    /* 每轮（每个请求）从 jetty 池取一条新的 send lane 并发回写 */
    std::shared_ptr<kv_bench::UrmaJetty> jetty;
    if (!mgr->AcquireSendLane(jetty)) {
        fprintf(stderr, "[wb] send lane pool exhausted\n");
        return -1;
    }

    uint64_t seq_a, seq_b;
    uint64_t ua = mgr->PostEvent(w->id, seq_a);
    uint64_t ub = mgr->PostEvent(w->id, seq_b);

    bool ok_post = true;
    if (ctx->args.get_fence) {
        /* 数据 WR + fence 标志 WR（每个半条 2 个 WQE） */
        if (!mgr->PostWriteFencedFlag(jetty, remote,
                                      (uint64_t)ctx->client_data + s_off,
                                      req->client_seg_va + d_off, wr_len,
                                      flag_local, flag_remote,
                                      (uint32_t)src_a, (uint32_t)dst_chip, ua)) {
            ok_post = false;
        }
        if (ok_post && !mgr->PostWriteFencedFlag(jetty, remote,
                                                 (uint64_t)ctx->client_data + s_off + wr_len,
                                                 req->client_seg_va + d_off + wr_len, wr_len,
                                                 flag_local, flag_remote + 8,
                                                 (uint32_t)src_b, (uint32_t)dst_chip, ub)) {
            ok_post = false;
        }
    } else {
        /* 单 WQE 2-sge: 数据 + 完成标志 */
        if (!mgr->PostWriteWithFlag(jetty, remote,
                                    (uint64_t)ctx->client_data + s_off,
                                    req->client_seg_va + d_off, wr_len,
                                    flag_local, flag_remote,
                                    (uint32_t)src_a, (uint32_t)dst_chip, ua)) {
            ok_post = false;
        }
        if (ok_post && !mgr->PostWriteWithFlag(jetty, remote,
                                               (uint64_t)ctx->client_data + s_off + wr_len,
                                               req->client_seg_va + d_off + wr_len, wr_len,
                                               flag_local, flag_remote + 8,
                                               (uint32_t)src_b, (uint32_t)dst_chip, ub)) {
            ok_post = false;
        }
    }
    if (!ok_post) {
        mgr->AbortEvent(w->id, seq_a);
        mgr->AbortEvent(w->id, seq_b);
        mgr->ReleaseSendLane(jetty);
        return -1;
    }
    bool ok_a = mgr->WaitEvent(w->id, seq_a, ctx->args.timeout_ms);
    bool ok_b = mgr->WaitEvent(w->id, seq_b, ctx->args.timeout_ms);
    mgr->ReleaseSendLane(jetty);
    return (ok_a && ok_b) ? 0 : -1;
}

static void *server_get_worker_main(void *arg)
{
    worker_t *w = (worker_t *)arg;
    conn_t *conn = (conn_t *)w->run_arg;
    context_t *ctx = conn->ctx;
    const argument_t *args = &ctx->args;

    if (args->affinity_mode == AFF_AFFINITY) {
        int mine[MAX_CPUS];
        int n = my_cpu_list(args, false, mine, MAX_CPUS);
        if (n > 0) {
            int cpu = mine[w->id % n];
            if (pin_thread_to_cpu(cpu) != 0) {
                fprintf(stderr, "[srv-worker %u] pin to cpu %d failed: %s\n", w->id, cpu, strerror(errno));
            }
        }
    }

    get_req_t *ring = (get_req_t *)ctx->va;
    uint32_t g = conn->worker_count;
    while (!w->stop && !ctx->stop) {
        bool worked = false;
        for (uint32_t slot = w->local_index % g; slot < RING_SLOTS; slot += g) {
            uint64_t seq = __atomic_load_n(&ring[slot].seq, __ATOMIC_ACQUIRE);
            if (seq == 0 || seq == w->seen[slot]) {
                continue;
            }
            get_req_t req = ring[slot]; /* 拷贝; seq acquire 已保证前置字段可见 */
            int src_a, src_b, dst_chip;
            pick_round_chips(args, false, w, &src_a, &src_b, &dst_chip);
            /* get 回写目的 = 客户端缓冲 -> 客户端目的 chip（亲和/反亲和使用） */
            if (conn->conn != nullptr && conn->conn->peer.dstChip != INVALID_CHIP) {
                dst_chip = (int)conn->conn->peer.dstChip;
            }
            if (server_write_back(ctx, conn, w, &req, src_a, src_b, dst_chip) == 0) {
                w->seen[slot] = seq;
                __atomic_add_fetch(&w->ops, 1, __ATOMIC_RELAXED);
                __atomic_add_fetch(&w->bytes,
                                   (conn->conn->peer.dualMode == DUAL_MIRROR) ? 2ULL * req.size : req.size,
                                   __ATOMIC_RELAXED);
            } else {
                __atomic_add_fetch(&w->errors, 1, __ATOMIC_RELAXED);
                w->seen[slot] = seq; /* 放弃该请求，避免死循环 */
            }
            worked = true;
        }
        if (!worked) {
            sleep_ns(POLL_SLEEP_NS);
        }
    }
    return NULL;
}

/* ---------------- 统计 ---------------- */

static void print_latency_line_us(const char *tag, const kv_hist_t *h)
{
    uint64_t avg = (uint64_t)kv_hist_mean(h);
    uint64_t p50 = kv_hist_value_at_percentile(h, 50.0);
    uint64_t p90 = kv_hist_value_at_percentile(h, 90.0);
    uint64_t p99 = kv_hist_value_at_percentile(h, 99.0);
    uint64_t p999 = kv_hist_value_at_percentile(h, 99.9);
    uint64_t p9999 = kv_hist_value_at_percentile(h, 99.99);
    printf("%s latency(us): avg=%.3f min=%.3f p50=%.3f p90=%.3f p99=%.3f p999=%.3f p9999=%.3f pmax=%.3f\n",
           tag, (double)avg / 1000.0, (double)kv_hist_min(h) / 1000.0, (double)p50 / 1000.0,
           (double)p90 / 1000.0, (double)p99 / 1000.0, (double)p999 / 1000.0, (double)p9999 / 1000.0,
           (double)kv_hist_max(h) / 1000.0);
}

static const char *op_name(int op)
{
    return op == OP_WRITE ? "write" : (op == OP_GET ? "get" : "mixed");
}

static const char *dual_name(int m)
{
    return m == DUAL_MIRROR ? "mirror" : "split";
}

static const char *aff_name(int m)
{
    return m == AFF_AFFINITY ? "affinity" : (m == AFF_ANTI ? "anti-affinity" : "none");
}

static void print_client_summary(context_t *ctx, double seconds)
{
    const argument_t *args = &ctx->args;
    kv_hist_t merged;
    if (kv_hist_init(&merged, 1, 60ULL * 1000000000ULL, 3) != 0) {
        return;
    }
    uint64_t ops = 0, bytes = 0, errors = 0;
    for (uint32_t i = 0; i < ctx->worker_count; i++) {
        kv_hist_merge(&merged, &ctx->workers[i].hist);
        ops += __atomic_load_n(&ctx->workers[i].ops, __ATOMIC_RELAXED);
        bytes += __atomic_load_n(&ctx->workers[i].bytes, __ATOMIC_RELAXED);
        errors += __atomic_load_n(&ctx->workers[i].errors, __ATOMIC_RELAXED);
    }
    printf("\n==== summary role=client op=%s threads=%u size=%" PRIu64 " dual=%s affinity=%s "
           "jetty_count=%u duration=%.1fs ====\n",
           op_name(args->op), args->threads, args->value_size, dual_name(args->dual_mode),
           aff_name(args->affinity_mode), ctx->mgr->Resource().SendJettyCount(), seconds);
    double iops = seconds > 0 ? (double)ops / seconds : 0.0;
    double bw_mbps = seconds > 0 ? (double)bytes * 8.0 / seconds / 1e6 : 0.0;
    printf("requests=%" PRIu64 " iops=%.2f wr_rate=%.2f bandwidth_mbps=%.2f bytes=%" PRIu64 " errors=%" PRIu64 "\n",
           ops, iops, iops * 2.0, bw_mbps, bytes, errors);
    print_latency_line_us("round", &merged);
    kv_hist_destroy(&merged);
}

static void *client_sampler_main(void *arg)
{
    context_t *ctx = (context_t *)arg;
    const argument_t *args = &ctx->args;
    uint64_t last_ops = 0, last_bytes = 0;
    uint64_t t0 = now_ns();
    uint64_t last_t = t0;
    while (!ctx->stop) {
        sleep((unsigned int)(args->report_interval > 0 ? args->report_interval : 1));
        uint64_t now = now_ns();
        uint64_t ops = 0, bytes = 0, errors = 0;
        for (uint32_t i = 0; i < ctx->worker_count; i++) {
            ops += __atomic_load_n(&ctx->workers[i].ops, __ATOMIC_RELAXED);
            bytes += __atomic_load_n(&ctx->workers[i].bytes, __ATOMIC_RELAXED);
            errors += __atomic_load_n(&ctx->workers[i].errors, __ATOMIC_RELAXED);
        }
        double dt = (double)(now - last_t) / 1e9;
        double elapsed = (double)(now - t0) / 1e9;
        if (dt > 0) {
            printf("[t=%.1fs] ops=%" PRIu64 " iops=%.2f bandwidth_mbps=%.2f errors=%" PRIu64 "\n", elapsed,
                   ops, (double)(ops - last_ops) / dt, (double)(bytes - last_bytes) * 8.0 / dt / 1e6, errors);
        }
        last_ops = ops;
        last_bytes = bytes;
        last_t = now;
    }
    return NULL;
}

/* ---------------- 客户端流程 ---------------- */

static int client_connect_and_exchange(context_t *ctx, const argument_t *args)
{
    struct sockaddr_in addr;
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        fprintf(stderr, "Failed to create socket: %d\n", errno);
        return -1;
    }
    addr.sin_family = AF_INET;
    addr.sin_port = htons(args->server_port);
    addr.sin_addr.s_addr = inet_addr(args->server_ip);
    if (connect(sockfd, (struct sockaddr *)&addr, sizeof(struct sockaddr))) {
        fprintf(stderr, "Failed to connect, err: [%d]%s\n", errno, strerror(errno));
        close(sockfd);
        return -1;
    }

    kv_bench::HandshakeParams params;
    params.threads = args->threads;
    params.opCode = (uint32_t)args->op;
    params.dualMode = (uint32_t)args->dual_mode;
    params.valueSize = (uint32_t)args->value_size;
    params.transMode = args->trans_mode;
    params.dstChip = INVALID_CHIP;
    int dst_chip = first_dst_chip(args);
    if (dst_chip > 0) {
        params.dstChip = (uint32_t)dst_chip;
    }
    if (!ctx->mgr->ExchangeAsClient(sockfd, params, ctx->conn)) {
        fprintf(stderr, "Failed to exchange URMA info with server\n");
        close(sockfd);
        return -1;
    }
    printf("remote dst_chip=%u\n", ctx->conn->peer.dstChip);
    return sockfd;
}

static int create_workers(context_t *ctx, uint32_t count)
{
    ctx->workers = (worker_t *)calloc(count, sizeof(worker_t));
    if (ctx->workers == NULL) return -1;
    ctx->worker_count = count;
    for (uint32_t i = 0; i < count; i++) {
        worker_t *w = &ctx->workers[i];
        uint32_t wid = 0;
        if (!ctx->mgr->RegisterWorker(wid)) {
            return -1;
        }
        w->id = wid;
        w->local_index = i;
        w->run_arg = ctx;
        w->rng = ctx->args.seed + i * 2654435761u;
        if (kv_hist_init(&w->hist, 1, 60ULL * 1000000000ULL, 3) != 0) return -1;
    }
    return 0;
}

static void free_bench_workers(context_t *ctx)
{
    if (ctx->workers != NULL) {
        for (uint32_t i = 0; i < ctx->worker_count; i++) {
            kv_hist_destroy(&ctx->workers[i].hist);
        }
        free(ctx->workers);
        ctx->workers = NULL;
        ctx->worker_count = 0;
    }
}

/* 统一清理：停 manager + 释放 worker/缓冲 + 关闭控制 socket（sockfd<0 时跳过） */
static void destroy_context(context_t *ctx, int sockfd)
{
    if (sockfd >= 0) {
        close(sockfd);
    }
    if (ctx->mgr != nullptr) {
        ctx->mgr->Stop();
        delete ctx->mgr;
        ctx->mgr = nullptr;
    }
    free_bench_workers(ctx);
    if (ctx->va != NULL) {
        free(ctx->va);
        ctx->va = NULL;
    }
    free(ctx);
}

static int run_client(const argument_t *args)
{
    context_t *ctx = (context_t *)calloc(1, sizeof(context_t));
    if (ctx == NULL) return -1;
    ctx->args = *args;
    ctx->stop = false;
    ctx->mgr = new kv_bench::UrmaManager();
    int sockfd = -1;
    pthread_t sampler_thread = 0;

    if (!ctx->mgr->Init(args->dev_name, args->cacheable, args->jetty_count, args->threads, args->event_mode,
                        args->trans_mode)) {
        fprintf(stderr, "failed to init URMA manager\n");
        destroy_context(ctx, sockfd);
        return -1;
    }
    if (setup_buffer(ctx, false) != 0) {
        destroy_context(ctx, sockfd);
        return -1;
    }
    sockfd = client_connect_and_exchange(ctx, args);
    if (sockfd < 0) {
        destroy_context(ctx, sockfd);
        return -1;
    }
    if (create_workers(ctx, args->threads) != 0) {
        destroy_context(ctx, sockfd);
        return -1;
    }

    if (pthread_create(&sampler_thread, NULL, client_sampler_main, ctx) != 0) {
        fprintf(stderr, "Failed to create sampler thread\n");
    }

    uint64_t t0 = now_ns();
    for (uint32_t i = 0; i < ctx->worker_count; i++) {
        if (pthread_create(&ctx->workers[i].tid, NULL, client_worker_main, &ctx->workers[i]) != 0) {
            fprintf(stderr, "Failed to create client worker %u\n", i);
        }
    }
    for (uint32_t i = 0; i < ctx->worker_count; i++) {
        pthread_join(ctx->workers[i].tid, NULL);
    }
    uint64_t t1 = now_ns();

    ctx->stop = true;
    if (sampler_thread != 0) {
        pthread_join(sampler_thread, NULL);
    }

    print_client_summary(ctx, (double)(t1 - t0) / 1e9);

    /* 通知服务器结束（EOF 语义） */
    char sync_msg = 'E';
    (void)write(sockfd, &sync_msg, 1);
    close(sockfd);
    sockfd = -1;

    destroy_context(ctx, sockfd);
    return 0;
}

/* ---------------- 服务器流程 ---------------- */

static int set_socket_nonblock(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        fprintf(stderr, "Failed to get flags of socket, err:%d.\n", errno);
        return -1;
    }
    if (fcntl(fd, F_SETFL, (uint32_t)flags | O_NONBLOCK) == -1) {
        fprintf(stderr, "Failed to set socket to non block, err:%d.\n", errno);
        return -1;
    }
    return 0;
}

static void *server_conn_main(void *arg)
{
    conn_t *conn = (conn_t *)arg;
    context_t *ctx = conn->ctx;

    kv_bench::HandshakeParams params;
    params.threads = ctx->args.threads;
    params.opCode = (uint32_t)ctx->args.op;
    params.dualMode = (uint32_t)ctx->args.dual_mode;
    params.valueSize = (uint32_t)ctx->args.value_size;
    params.transMode = ctx->args.trans_mode;
    params.dstChip = INVALID_CHIP;
    int dst_chip = first_dst_chip(&ctx->args);
    if (dst_chip > 0) {
        params.dstChip = (uint32_t)dst_chip;
    }
    if (!ctx->mgr->ExchangeAsServer(conn->fd, params, conn->conn)) {
        fprintf(stderr, "[conn] Failed to exchange URMA info\n");
        goto out;
    }

    printf("[conn] remote threads=%u op=%s dual=%s dst_chip=%u\n",
           conn->conn->peer.threads, op_name((int)conn->conn->peer.opCode),
           dual_name((int)conn->conn->peer.dualMode), conn->conn->peer.dstChip);

    /* get/mixed: 启动回写线程（每个回写线程独占一条 jetty） */
    if (conn->conn->peer.opCode != OP_WRITE) {
        uint32_t g = ctx->args.server_workers > 0 ? ctx->args.server_workers : conn->conn->peer.threads;
        if (g < 1) g = 1;
        if (g > ctx->mgr->Resource().SendJettyCount()) {
            fprintf(stderr, "[conn] get workers %u > server jettys %u, capped (raise --jetty-count on server)\n",
                    g, ctx->mgr->Resource().SendJettyCount());
            g = ctx->mgr->Resource().SendJettyCount();
        }
        conn->workers = (worker_t *)calloc(g, sizeof(worker_t));
        conn->worker_count = g;
        if (conn->workers == NULL) {
            goto out;
        }
        bool worker_ok = true;
        for (uint32_t i = 0; i < g; i++) {
            worker_t *w = &conn->workers[i];
            uint32_t wid = 0;
            if (!ctx->mgr->RegisterWorker(wid)) {
                worker_ok = false;
                break;
            }
            w->id = wid;
            w->local_index = i;
            w->run_arg = conn;
            w->seen = (uint32_t *)calloc(RING_SLOTS, sizeof(uint32_t));
            w->rng = ctx->args.seed + wid * 2654435761u;
            if (w->seen == NULL || kv_hist_init(&w->hist, 1, 60ULL * 1000000000ULL, 3) != 0) {
                worker_ok = false;
                break;
            }
        }
        if (!worker_ok) {
            for (uint32_t i = 0; i < g; i++) {
                kv_hist_destroy(&conn->workers[i].hist);
                free(conn->workers[i].seen);
            }
            free(conn->workers);
            conn->workers = NULL;
            conn->worker_count = 0;
            goto out;
        }
        for (uint32_t i = 0; i < g; i++) {
            if (pthread_create(&conn->workers[i].tid, NULL, server_get_worker_main, &conn->workers[i]) != 0) {
                fprintf(stderr, "[conn] failed to start get worker %u\n", i);
            }
        }
        printf("[conn] started %u get write-back workers (dst_chip=%u)\n", g, conn->conn->peer.dstChip);
    }

    /* 等待客户端结束（EOF） */
    {
        char buf[16];
        while (!ctx->server_stop) {
            ssize_t n = read(conn->fd, buf, sizeof(buf));
            if (n <= 0) {
                break;
            }
        }
    }

    if (conn->workers != NULL) {
        for (uint32_t i = 0; i < conn->worker_count; i++) {
            conn->workers[i].stop = true;
        }
        for (uint32_t i = 0; i < conn->worker_count; i++) {
            if (conn->workers[i].tid != 0) {
                pthread_join(conn->workers[i].tid, NULL);
            }
        }
        for (uint32_t i = 0; i < conn->worker_count; i++) {
            kv_hist_destroy(&conn->workers[i].hist);
            free(conn->workers[i].seen);
        }
        free(conn->workers);
        conn->workers = NULL;
        conn->worker_count = 0;
    }
out:
    conn->conn.reset();
    close(conn->fd);
    conn->used = false;
    return NULL;
}

static void *server_sock_thread_main(void *arg)
{
    context_t *ctx = (context_t *)arg;
    while (!ctx->server_stop) {
        int fd = accept(ctx->listen_fd, NULL, NULL);
        if (fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                usleep(1000);
                continue;
            }
            fprintf(stderr, "Failed to accept connection\n");
            return NULL;
        }
        conn_t *conn = NULL;
        for (uint32_t i = 0; i < MAX_CLIENT_CNT; i++) {
            if (!ctx->conns[i].used) {
                conn = &ctx->conns[i];
                break;
            }
        }
        if (conn == NULL) {
            fprintf(stderr, "Too many connections\n");
            close(fd);
            continue;
        }
        (void)memset(conn, 0, sizeof(*conn));
        conn->used = true;
        conn->fd = fd;
        conn->ctx = ctx;
        if (pthread_create(&conn->tid, NULL, server_conn_main, conn) != 0) {
            fprintf(stderr, "Failed to create conn thread\n");
            conn->used = false;
            close(fd);
            continue;
        }
    }
    return NULL;
}

static int server_listen(context_t *ctx, const argument_t *args)
{
    int enable = 1;
    ctx->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (ctx->listen_fd < 0) {
        fprintf(stderr, "Failed to create socket_fd, err: [%d]%s.\n", errno, strerror(errno));
        return -1;
    }
    if (setsockopt(ctx->listen_fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) < 0) {
        fprintf(stderr, "Failed to setsockopt, err: [%d]%s.\n", errno, strerror(errno));
        (void)close(ctx->listen_fd);
        return -1;
    }
    if (set_socket_nonblock(ctx->listen_fd)) {
        (void)close(ctx->listen_fd);
        return -1;
    }
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr("0.0.0.0");
    addr.sin_port = htons(args->server_port);
    if (bind(ctx->listen_fd, (struct sockaddr *)&addr, sizeof(struct sockaddr)) != 0) {
        fprintf(stderr, "Failed to bind, err: [%d]%s.\n", errno, strerror(errno));
        (void)close(ctx->listen_fd);
        return -1;
    }
    if (listen(ctx->listen_fd, MAX_CLIENT_CNT)) {
        fprintf(stderr, "Failed to listen, err: [%d]%s.\n", errno, strerror(errno));
        (void)close(ctx->listen_fd);
        return -1;
    }
    ctx->server_stop = false;
    if (pthread_create(&ctx->sock_thread, NULL, server_sock_thread_main, ctx) != 0) {
        fprintf(stderr, "Failed to create server thread\n");
        (void)close(ctx->listen_fd);
        return -1;
    }
    printf("server listening on port %u (dev=%s, seg va=0x%" PRIx64 " len=%" PRIu64 ")\n",
           args->server_port, args->dev_name, (uint64_t)ctx->va, ctx->buf_len);
    return 0;
}

static int run_server(const argument_t *args)
{
    context_t *ctx = (context_t *)calloc(1, sizeof(context_t));
    if (ctx == NULL) return -1;
    ctx->args = *args;
    ctx->stop = false;
    ctx->mgr = new kv_bench::UrmaManager();

    if (!ctx->mgr->Init(args->dev_name, args->cacheable, args->jetty_count, 1, args->event_mode,
                        args->trans_mode)) {
        fprintf(stderr, "failed to init URMA manager\n");
        destroy_context(ctx, -1);
        return -1;
    }
    if (setup_buffer(ctx, true) != 0) {
        destroy_context(ctx, -1);
        return -1;
    }
    /* 目的亲和: 服务器（write 目的）固定到 destination_cpus; 反亲和同样固定目的侧 */
    if (args->affinity_mode != AFF_NONE) {
        if (apply_cpu_list(args->destination_cpus) != 0) {
            fprintf(stderr, "failed to apply destination CPU affinity\n");
            destroy_context(ctx, -1);
            return -1;
        }
        printf("server process pinned to %s\n",
               args->destination_cpus != NULL ? args->destination_cpus : "(all)");
    }

    if (server_listen(ctx, args) != 0) {
        destroy_context(ctx, -1);
        return -1;
    }

    printf("Type enter to exit...\n");
    (void)getchar();

    ctx->server_stop = true;
    pthread_join(ctx->sock_thread, NULL);
    for (uint32_t i = 0; i < MAX_CLIENT_CNT; i++) {
        if (ctx->conns[i].used && ctx->conns[i].tid != 0) {
            pthread_join(ctx->conns[i].tid, NULL);
        }
    }
    close(ctx->listen_fd);

    destroy_context(ctx, -1);
    return 0;
}

/* ---------------- 参数解析 ---------------- */

static struct option g_long_options[] = {
    {"trans-mode", required_argument, NULL, 'm'},
    {"dev-name", required_argument, NULL, 'd'},
    {"server-ip", required_argument, NULL, 'i'},
    {"server-port", required_argument, NULL, 'p'},
    {"tp-type", required_argument, NULL, 't'},
    {"multi-path", required_argument, NULL, 'u'},
    {"event-mode", no_argument, NULL, 'e'},
    {"value-size", required_argument, NULL, 1000},
    {"qps", required_argument, NULL, 1001},
    {"duration", required_argument, NULL, 1002},
    {"jetty-count", required_argument, NULL, 1003},
    {"affinity-mode", required_argument, NULL, 1004},
    {"source-cpus", required_argument, NULL, 1005},
    {"destination-cpus", required_argument, NULL, 1006},
    {"cacheable", no_argument, NULL, 1007},
    {"threads", required_argument, NULL, 1008},
    {"op", required_argument, NULL, 1009},
    {"mixed-ratio", required_argument, NULL, 1010},
    {"dual-mode", required_argument, NULL, 1011},
    {"report-interval", required_argument, NULL, 1012},
    {"server-workers", required_argument, NULL, 1013},
    {"get-fence", no_argument, NULL, 1014},
    {"no-mbind", no_argument, NULL, 1015},
    {"seed", required_argument, NULL, 1016},
    {"fixed-offset", no_argument, NULL, 1017},
    {"timeout-ms", required_argument, NULL, 1018},
    {NULL, 0, NULL, 0}
};

static void usage(void)
{
    printf("Usage:\n");
    printf("  -m, --trans-mode <mode>    urma mode: 0 for RM, 1 for RC, 2 for UM, 3 for RS (default 0)\n");
    printf("  -d, --dev-name <dev>       device name, e.g. bonding0 (default bonding0)\n");
    printf("  -i, --server-ip <ip>       server ip address given only by client\n");
    printf("  -p, --server-port <port>   listen on/connect to port <port> (default %d)\n", DEFAULT_PORT);
    printf("  -t, --tp-type <type>       0 for URMA_RTP, 1 for URMA_CTP, 2 for URMA_UTP\n");
    printf("  -u, --multi-path           use multipath instead of single path (default false)\n");
    printf("  -e, --event-mode           use wait_jfc/ack/rearm event mode (default false)\n");
    printf("      --value-size <bytes>   per-WR payload, e.g. 4M/8M (default 4M)\n");
    printf("      --qps <qps>            client target QPS in rounds/sec (0 = as fast as possible)\n");
    printf("      --duration <seconds>   client run duration (default 10)\n");
    printf("      --jetty-count <n>      min send Jetty count, 1..200 (default 1; threads may raise it)\n");
    printf("      --affinity-mode <m>    affinity | anti | none (default none)\n");
    printf("      --source-cpus <list>   client CPU list, e.g. 4,5\n");
    printf("      --destination-cpus <list> server CPU list, e.g. 8,9\n");
    printf("      --cacheable            register/import cacheable memory\n");
    printf("      --threads <n>          client load threads (default 1)\n");
    printf("      --op <op>              write | get | mixed (default write)\n");
    printf("      --mixed-ratio <pct>    write percentage in mixed mode (default 50)\n");
    printf("      --dual-mode <m>        mirror | split (default mirror): 每轮从 bonding 双源 CPU 并发 2 条 WR\n");
    printf("      --report-interval <s>  periodic report interval (default 1)\n");
    printf("      --server-workers <n>   server get write-back workers per conn (0=auto=client threads)\n");
    printf("      --get-fence            get 回写拆成 data WR + fence flag WR\n");
    printf("      --no-mbind             disable NUMA mbind of the destination data area\n");
    printf("      --seed <n>             random seed for anti/none affinity (default 42)\n");
    printf("      --fixed-offset         always use offset 0 (hot-cache test) instead of cycling\n");
    printf("      --timeout-ms <ms>      completion timeout (default 5000)\n");
}

static int validate_input_params(argument_t *args, bool tp_type_input_flag, bool multi_path_input_flag)
{
    if (args->dev_name == NULL || args->value_size == 0 || args->value_size > UINT32_MAX ||
        args->duration_sec == 0 || args->jetty_count == 0 || args->jetty_count > MAX_JETTY_COUNT) {
        fprintf(stderr, "Invalid device, value size, duration, or jetty count\n");
        return -1;
    }
    if (args->threads == 0 || args->threads > 512) {
        fprintf(stderr, "Invalid thread count %u\n", args->threads);
        return -1;
    }
    if (args->op < OP_WRITE || args->op > OP_MIXED) {
        fprintf(stderr, "Invalid op\n");
        return -1;
    }
    if (args->dual_mode != DUAL_MIRROR && args->dual_mode != DUAL_SPLIT) {
        fprintf(stderr, "Invalid dual mode\n");
        return -1;
    }
    if (args->dual_mode == DUAL_SPLIT && (args->value_size % 2) != 0) {
        fprintf(stderr, "split dual mode requires even value-size\n");
        return -1;
    }
    if (args->mixed_ratio > 100) {
        fprintf(stderr, "Invalid mixed ratio\n");
        return -1;
    }
    if (args->trans_mode > 3) {
        fprintf(stderr, "Invalid trans mode %d\n", args->trans_mode);
        return -1;
    }
    if (args->tp_type > 2) {
        fprintf(stderr, "Invalid tp type %d\n", args->tp_type);
        return -1;
    }

    if (strncmp(args->dev_name, "bonding", strlen("bonding")) == 0) {
        if (tp_type_input_flag) {
            fprintf(stderr, "Warning: TP type should not be set for bonding device.\n");
        }
        /* bonding + RM 要求 multi-path：自动开启，无需手动加 -u */
        if (args->trans_mode == 0 && !args->multi_path) {
            fprintf(stderr, "Info: bonding device with RM trans-mode requires multi-path, enabling it automatically.\n");
            args->multi_path = true;
        }
        if (args->trans_mode != 0 && args->trans_mode != 1) {
            fprintf(stderr, "Error: bonding device only supports RM+multi-path or RC (-m 1), got trans-mode %u.\n",
                    args->trans_mode);
            return -1;
        }
    } else {
        if (multi_path_input_flag) {
            fprintf(stderr, "Error: Multi path should not be set for non-bonding device.\n");
            return -1;
        }
        if (!(((args->trans_mode != 2) && (args->tp_type != 2)) || (args->trans_mode == 2 && args->tp_type == 2))) {
            fprintf(stderr, "Error: This combination of tp-type and trans-mode is invalid on non-bonding device.\n");
            return -1;
        }
    }
    return 0;
}

static int parse_arguments(int argc, char *argv[], argument_t *args)
{
    if (argc == 1) {
        usage();
        return -1;
    }
    memset(args, 0, sizeof(*args));
    args->server_port = DEFAULT_PORT;
    args->trans_mode = 0;
    args->value_size = DEFAULT_VALUE_SIZE;
    args->qps = 0;
    args->duration_sec = 10;
    args->jetty_count = 1;
    args->affinity_mode = AFF_NONE;
    args->threads = 1;
    args->op = OP_WRITE;
    args->mixed_ratio = 50;
    args->dual_mode = DUAL_MIRROR;
    args->report_interval = 1;
    args->server_workers = 0;
    args->seed = 42;
    args->timeout_ms = DEFAULT_TIMEOUT_MS;

    bool multi_path_input_flag = false;
    bool tp_type_input_flag = false;

    while (1) {
        int c = getopt_long(argc, argv, "m:d:i:p:t:ue", g_long_options, NULL);
        if (c == -1) {
            break;
        }
        switch (c) {
            case 'm':
                args->trans_mode = (unsigned int)strtoul(optarg, NULL, 0);
                break;
            case 'd':
                args->dev_name = strdup(optarg);
                if (args->dev_name == NULL) {
                    fprintf(stderr, "failed to allocate memory.\n");
                }
                break;
            case 'i':
                args->server_ip = strdup(optarg);
                if (args->server_ip == NULL) {
                    fprintf(stderr, "failed to allocate memory.\n");
                }
                break;
            case 'p':
                args->server_port = (unsigned int)strtoul(optarg, NULL, 0);
                break;
            case 't':
                args->tp_type = (unsigned int)strtoul(optarg, NULL, 0);
                tp_type_input_flag = true;
                break;
            case 'u':
                args->multi_path = true;
                multi_path_input_flag = true;
                break;
            case 'e':
                args->event_mode = true;
                break;
            case 1000:
                args->value_size = parse_size(optarg);
                break;
            case 1001:
                args->qps = strtoull(optarg, NULL, 0);
                break;
            case 1002:
                args->duration_sec = strtoull(optarg, NULL, 0);
                break;
            case 1003:
                args->jetty_count = (uint32_t)strtoul(optarg, NULL, 0);
                break;
            case 1004: {
                if (strcmp(optarg, "affinity") == 0) args->affinity_mode = AFF_AFFINITY;
                else if (strcmp(optarg, "anti") == 0 || strcmp(optarg, "anti-affinity") == 0) args->affinity_mode = AFF_ANTI;
                else if (strcmp(optarg, "none") == 0) args->affinity_mode = AFF_NONE;
                else {
                    fprintf(stderr, "Invalid affinity mode: %s\n", optarg);
                    return -1;
                }
                break;
            }
            case 1005:
                args->source_cpus = optarg;
                break;
            case 1006:
                args->destination_cpus = optarg;
                break;
            case 1007:
                args->cacheable = true;
                break;
            case 1008:
                args->threads = (uint32_t)strtoul(optarg, NULL, 0);
                break;
            case 1009: {
                if (strcmp(optarg, "write") == 0) args->op = OP_WRITE;
                else if (strcmp(optarg, "get") == 0) args->op = OP_GET;
                else if (strcmp(optarg, "mixed") == 0) args->op = OP_MIXED;
                else {
                    fprintf(stderr, "Invalid op: %s\n", optarg);
                    return -1;
                }
                break;
            }
            case 1010:
                args->mixed_ratio = (uint32_t)strtoul(optarg, NULL, 0);
                break;
            case 1011: {
                if (strcmp(optarg, "mirror") == 0) args->dual_mode = DUAL_MIRROR;
                else if (strcmp(optarg, "split") == 0) args->dual_mode = DUAL_SPLIT;
                else {
                    fprintf(stderr, "Invalid dual mode: %s\n", optarg);
                    return -1;
                }
                break;
            }
            case 1012:
                args->report_interval = (uint32_t)strtoul(optarg, NULL, 0);
                break;
            case 1013:
                args->server_workers = (uint32_t)strtoul(optarg, NULL, 0);
                break;
            case 1014:
                args->get_fence = true;
                break;
            case 1015:
                args->no_mbind = true;
                break;
            case 1016:
                args->seed = (uint32_t)strtoul(optarg, NULL, 0);
                break;
            case 1017:
                args->fixed_offset = true;
                break;
            case 1018:
                args->timeout_ms = (int)strtol(optarg, NULL, 0);
                break;
            default:
                usage();
                return -1;
        }
    }

    if (optind < argc) {
        usage();
        return -1;
    }
    if (args->dev_name == NULL) {
        args->dev_name = strdup("bonding0");
        if (args->dev_name == NULL) return -1;
    }
    return validate_input_params(args, tp_type_input_flag, multi_path_input_flag);
}

int main(int argc, char *argv[])
{
    argument_t args;
    int ret;

    ret = parse_arguments(argc, argv, &args);
    if (ret != 0) {
        goto main_exit;
    }
    if (args.server_ip != NULL) {
        ret = run_client(&args);
    } else {
        ret = run_server(&args);
    }

main_exit:
    if (args.dev_name != NULL) {
        free(args.dev_name);
    }
    if (args.server_ip != NULL) {
        free(args.server_ip);
    }
    return ret;
}
