#define _GNU_SOURCE

/*
 * kv-bench 业务层：选项 / 亲和(chip) / 打流引擎 / 统计。
 *
 * 分层（对齐 yuanrong-datasystem）：
 *   业务层(kv_bench.cpp) -> 管理层(urma/urma_manager) ->
 * 资源层(urma/urma_resource) -> liburma 数据通路只走 URMA： write(Put):
 * 分片流水线（一次请求 80MB = 20 × 4MB 分片，每分片独立 jetty，亲和分片
 * src==dst 交替 chip1/chip2，--concurrency 请求并发，jetty 池驱动发送）。
 * get(Get): 客户端直接 READ 服务器数据区（URMA_OPC_READ）。 统计：每线程
 * HdrHistogram-lite，输出 avg/p50/p90/p99/p999/p9999/pmax + 带宽/IOPS。
 * 亲和：affinity(分片源==目的==chip)/anti(源随机&目的固定)/none(双随机)。
 * 无 bthread/brpc/protobuf 依赖。
 */

#include "hist.h"
#include "sys/mman.h"
#include "urma/urma_manager.h"
#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <inttypes.h>
#include <linux/mempolicy.h>
#include <malloc.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sched.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>


#define PAGE_SHIFT 12
#define PAGE_SIZE (0x1 << PAGE_SHIFT) /* 4KB */
#define DEFAULT_VALUE_SIZE (4UL * 1024 * 1024)
#define MAX_JETTY_COUNT 200
#define MAX_CLIENT_CNT 10
#define POLL_SLEEP_NS 1000L /* 1us */
#define DEFAULT_PORT 13857
#define DATA_WINDOW_PER_THREAD                                                 \
  4 /* 客户端 data_len = threads * 4 * value_size（get/mixed 用） */
#define SERVER_DATA_WINDOW 4

/* write 流水线模型：一个请求 = 同一 8MB buffer 发 10 次（chip 交替
 * 1,2,1,2...），每次发送拆 2 条 4MB WR，【每条 4MB WR 一个独立 jetty】；
 * 10 次全完成（20 条 WR）= 请求完成；带宽按 8MB 数据/请求统计 */
#define KV_WR_SIZE (4UL * 1024 * 1024)   /* 单条 WR 4MB */
#define KV_WR_PER_SEND 2                 /* 每次发送 8M = 2 条 4M WR */
#define KV_SEND_SIZE (KV_WR_SIZE * KV_WR_PER_SEND)  /* 8MB（同一 buffer） */
#define KV_SENDS_PER_REQ 10              /* 每请求发 10 次（chip 交替） */
#define KV_WR_PER_REQ (KV_SENDS_PER_REQ * KV_WR_PER_SEND) /* 20 条 WR/请求 */
#define KV_REQ_BYTES (KV_SEND_SIZE * KV_SENDS_PER_REQ) /* 80MB 传输字节 */
#define KV_MAX_CONCURRENCY 10            /* --concurrency 上限：在飞请求数 */
#define KV_MAX_WR_SLOTS KV_MAX_CONCURRENCY /* 槽 = 一个请求（20 条 WR） */
#define KV_MAX_INFLIGHT_REQS KV_MAX_CONCURRENCY

/* 服务器数据区：容纳最大窗口（10 请求 × 10 次 × 8MB = 800MB），固定 */
#define SERVER_PIPE_BYTES                                                      \
  ((uint64_t)KV_SENDS_PER_REQ * KV_MAX_CONCURRENCY * KV_SEND_SIZE)

#define DEFAULT_TIMEOUT_MS 5000
#define MAX_CPUS 1024
#define INVALID_CHIP 0xFFu
#define ROUND_UP(x, a) (((x) + (uint64_t)(a) - 1) & ~((uint64_t)(a) - 1))

/* 操作类型 / 亲和模式 */
enum { OP_WRITE = 0, OP_GET = 1, OP_MIXED = 2 };
enum { AFF_AFFINITY = 0, AFF_ANTI = 1, AFF_NONE = 2 };

typedef struct argument {
  char *dev_name;
  char *server_ip;
  unsigned int server_port;
  unsigned int trans_mode; /* 0=RM 1=RC 2=UM 3=RS */
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
  int concurrency; /* write 在飞请求数 1..10，默认 1 */
  int single_chip; /* 单 chip 场景：0=双 chip 交替；1/2=只用该 chip（src==dst） */
  int poll_cpu;    /* 轮询线程绑核；-1 = 自动选空闲核 */
  int op;
  uint32_t mixed_ratio;
  uint32_t report_interval;
  bool mbind; /* NUMA 绑定，默认关（Kunpeng/部分平台 mbind 不可用），--mbind
                 显式开启 */
  bool drv_ext; /* bonding chip 路由（has_drv_ext + src/dst chip），默认关 */
  bool import_rtp; /* import 对端 jetty 用普通 RTP 路径（跳过
                      bondp/CTP），默认关 */
  uint32_t seed;
  bool fixed_offset;
  int timeout_ms;
  bool query_chips; /* 只查询 chip 路由选择（不初始化 URMA），打印后退出 */
} argument_t;

typedef struct context context_t;

/* write 流水线：一个在飞单元 = 一个请求（同一 8M buffer 发 10 次 =
 * 20 条 4M WR，每条 4M WR 独立 jetty，chip 按发送次数交替） */
typedef struct wr_slot {
  std::shared_ptr<kv_bench::UrmaJetty> jetty[KV_WR_PER_REQ]; /* 每条 WR 独立 lane */
  uint64_t seq[KV_WR_PER_REQ];            /* 20 条 WR 的事件 seq */
  bool done[KV_WR_PER_REQ];               /* 各 WR 完成标志（持久） */
  uint8_t done_cnt;                       /* 已完成 WR 数（=20 请求完成） */
  uint64_t req_seq;                       /* 请求全局序号 */
  uint64_t post_ns;                       /* 第 1 条 WR post 时间（时延起点） */
  bool active;
} wr_slot_t;

typedef struct worker {
  uint32_t id;          /* UrmaManager workerId（进程级全局） */
  uint32_t local_index; /* 本连接/本进程内序号 */
  kv_hist_t hist;       /* 批时延直方图（10 请求全完成记一次） */
  kv_hist_t hist_req;   /* 请求级时延直方图（单个 8M 请求完成记一次） */
  volatile uint64_t ops;
  volatile uint64_t bytes;
  volatile uint64_t errors;
  uint64_t off; /* 客户端数据偏移（窗口内） */
  uint64_t next_post_ns;
  uint32_t rng;
  bool stop;
  pthread_t tid;
  void *run_arg;
  /* write 分片流水线状态 */
  wr_slot_t wr_slots[KV_MAX_WR_SLOTS];
  uint32_t active_slots[KV_MAX_WR_SLOTS]; /* 在飞槽下标（收完成只遍历这些） */
  uint32_t active_count;                  /* 在飞槽数 */
  uint32_t free_slots[KV_MAX_WR_SLOTS];   /* 空闲槽栈（发送 O(1) 取槽） */
  uint32_t free_count;                    /* 空闲槽数 */
  uint64_t req_start[KV_MAX_CONCURRENCY]; /* 每在飞请求的开始时间（请求 id % N） */
  uint64_t req_done[KV_MAX_CONCURRENCY];  /* 每在飞请求已完成组数 */
} worker_t;

typedef struct conn {
  int fd;
  bool used;
  context_t *ctx;
  std::shared_ptr<kv_bench::UrmaConnection> conn; /* 对端连接（import 结果） */
  pthread_t tid;
} conn_t;

struct context {
  argument_t args;
  kv_bench::UrmaManager *mgr; /* 管理层（单例/进程） */
  std::shared_ptr<kv_bench::UrmaConnection> conn; /* 客户端单连接 */

  void *va; /* 整个注册缓冲 */
  uint64_t buf_len;
  uint64_t data_len;     /* 客户端: 每线程数据窗口; 服务器: 数据区 */
  uint8_t *client_data;  /* 数据区起始（客户端读缓冲/写源；服务器数据源） */

  worker_t *workers;
  uint32_t worker_count;
  volatile bool stop;
  volatile bool fatal; /* 首错即中断（客户端打流） */

  /* 服务器 */
  int listen_fd;
  bool server_stop;
  pthread_t sock_thread;
  conn_t conns[MAX_CLIENT_CNT];
};

/* ---------------- 基础工具 ---------------- */

static uint64_t now_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

static void sleep_ns(uint64_t ns) {
  struct timespec ts = {.tv_sec = 0, .tv_nsec = (long)ns};
  nanosleep(&ts, NULL);
}

static uint32_t rng_next(uint32_t *state) {
  uint32_t x = *state;
  x ^= x << 13;
  x ^= x >> 17;
  x ^= x << 5;
  *state = x;
  return x;
}

static uint64_t parse_size(const char *text) {
  char *end = NULL;
  uint64_t value = strtoull(text, &end, 0);
  if (end == text)
    return 0;
  if (*end == 'K' || *end == 'k')
    value *= 1024ULL;
  else if (*end == 'M' || *end == 'm')
    value *= 1024ULL * 1024ULL;
  else if (*end == 'G' || *end == 'g')
    value *= 1024ULL * 1024ULL * 1024ULL;
  else if (*end != '\0')
    return 0;
  return value;
}

/* ---------------- CPU / NUMA / chip（对齐参考 NumaIdToChipId: 双 chip 模型）
 * ---------------- */

static int g_cpu_numa[MAX_CPUS];
static bool g_cpu_numa_known[MAX_CPUS];

static int read_cpu_numa(int cpu) {
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
    if (strncmp(ent->d_name, "node", 4) == 0 && ent->d_name[4] >= '0' &&
        ent->d_name[4] <= '9') {
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

static int get_num_numa_nodes(void) {
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

/* 双 chip 模型: 前一半 NUMA -> chip 1, 后一半 -> chip 2（对齐参考
 * NumaIdToChipId） */
static int numa_to_chip(int numa) {
  int numa_count = get_num_numa_nodes();
  if (numa < 0 || numa_count <= 0) {
    return -1;
  }
  int first_half = (numa_count + 1) / 2;
  return numa < first_half ? 1 : 2;
}

/* chip → 第一个匹配的 NUMA 节点（numa_to_chip 反查；单 chip 内存亲和用） */
static int chip_to_first_numa(int chip) {
  int numa_count = get_num_numa_nodes();
  for (int n = 0; n < numa_count; n++) {
    if (numa_to_chip(n) == chip) {
      return n;
    }
  }
  return -1;
}

static int cpu_to_chip(int cpu) { return numa_to_chip(read_cpu_numa(cpu)); }

static int pin_thread_to_cpu(int cpu) {
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(cpu, &set);
  return sched_setaffinity(0, sizeof(set), &set);
}

static int apply_cpu_list(const char *list) {
  if (list == NULL || *list == '\0')
    return 0;
  cpu_set_t set;
  CPU_ZERO(&set);
  char *copy = strdup(list);
  if (copy == NULL)
    return -1;
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
static int parse_cpu_list(const char *list, int *out, int max_out) {
  int n = 0;
  if (list == NULL || *list == '\0')
    return 0;
  char *copy = strdup(list);
  if (copy == NULL)
    return 0;
  for (char *part = strtok(copy, ","); part != NULL && n < max_out;
       part = strtok(NULL, ",")) {
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
    if (lo < 0 || hi < lo)
      continue;
    for (int c = lo; c <= hi && n < max_out; c++) {
      out[n++] = c;
    }
  }
  free(copy);
  return n;
}

static int enumerate_all_cpus(int *out, int max_out) {
  long ncpu = sysconf(_SC_NPROCESSORS_CONF);
  if (ncpu > max_out)
    ncpu = max_out;
  int n = 0;
  for (long c = 0; c < ncpu; c++) {
    out[n++] = (int)c;
  }
  return n;
}

/* mbind: 把 [addr, addr+len) 绑定到 node（用 syscall 避免 libnuma 依赖） */
/* mbind: 把 [addr, addr+len) 绑定到 node。对齐参考
 * DistributeMemoryAcrossNumaNodeList (yuanrong numa_util.cpp)：
 *   - MPOL_PREFERRED（首选节点，非强制，cpuset 受限也不会 EINVAL）
 *   - maxnode=32（固定大值，避开小 maxnode 时内核拷贝 0 字节的怪癖）
 *   - 1GB 分块 + 逐页 touch 触发实际分配 */
static int mbind_to_node(void *addr, size_t len, int node) {
  if (node < 0)
    return -1;
  long pageSizeRaw = getpagesize();
  if (pageSizeRaw <= 0)
    return -1;
  uint64_t pageSize = (uint64_t)pageSizeRaw;
  uintptr_t begin = (uintptr_t)addr;
  uintptr_t end = begin + len;
  uintptr_t alignedBegin = (begin / pageSize) * pageSize;
  uintptr_t alignedEnd = ((end + pageSize - 1) / pageSize) * pageSize;
  uint64_t alignedLen = alignedEnd - alignedBegin;
  constexpr uint64_t kChunkBytes = 1ULL << 30; /* 1GB */
  constexpr unsigned long kMaxNumaNodeCount = 32;
  unsigned long mask = 1UL << (unsigned long)node;
  uint8_t *p = (uint8_t *)alignedBegin;
  for (uint64_t off = 0; off < alignedLen;) {
    uint64_t chunk = alignedLen - off;
    if (chunk > kChunkBytes)
      chunk = kChunkBytes;
    long rc = syscall(SYS_mbind, p + off, chunk, MPOL_PREFERRED, &mask,
                      kMaxNumaNodeCount, 0);
    if (rc != 0) {
      fprintf(stderr,
              "Warning: mbind(node=%d) failed, errno=%d (%s); NUMA binding "
              "skipped\n",
              node, errno, strerror(errno));
      return -1;
    }
    /* 逐页 touch，让页面落在首选节点 */
    for (uint64_t pg = 0; pg < chunk; pg += pageSize) {
      volatile uint8_t *t = p + off + pg;
      *t = *t;
    }
    off += chunk;
  }
  return 0;
}

/* ---------------- 缓冲布局 ---------------- */

/* 注意：这里只计算各区域【尺寸】；client_data 等【指针】必须在
 * ctx->va = memalign() 之后计算（setup_buffer 内），否则会用 NULL
 * 基址算出垃圾指针。 */
static int layout_client_buffer(context_t *ctx) {
  const argument_t *args = &ctx->args;
  /* write：每线程数据区 = concurrency 块 8M buffer（重叠请求各占一块）；
   * get/mixed：value_size 窗口 */
  uint64_t window_reqs = (args->op == OP_WRITE && args->concurrency > 0)
                             ? (uint64_t)args->concurrency
                             : 1;
  uint64_t data_len = (args->op == OP_WRITE)
                          ? (uint64_t)args->threads * window_reqs * KV_SEND_SIZE
                          : (uint64_t)args->threads * DATA_WINDOW_PER_THREAD *
                                args->value_size;
  ctx->data_len = data_len;
  ctx->buf_len = ROUND_UP(ctx->data_len, PAGE_SIZE);
  return 0;
}

static int layout_server_buffer(context_t *ctx) {
  const argument_t *args = &ctx->args;
  uint64_t size = args->value_size;
  /* 服务器数据区：容纳 write 分片流水线最大窗口（10 并发 × 80MB = 800MB）；
   * get 为数据源（客户端 READ） */
  ctx->data_len =
      (args->op == OP_WRITE) ? SERVER_PIPE_BYTES
                             : (uint64_t)SERVER_DATA_WINDOW * size;
  ctx->buf_len = ROUND_UP(ctx->data_len, PAGE_SIZE);
  return 0;
}

/* 前向声明：定义在下方"亲和"区 */
static int my_cpu_list(const argument_t *args, bool is_client, int *out,
                       int max_out);

/* 分配 + 注册缓冲，并按亲和 mbind 数据区 */
static int setup_buffer(context_t *ctx, bool is_server) {
  const argument_t *args = &ctx->args;
  if (is_server) {
    layout_server_buffer(ctx);
  } else {
    layout_client_buffer(ctx);
  }
  ctx->va = memalign(PAGE_SIZE, ctx->buf_len);
  if (ctx->va == NULL) {
    fprintf(stderr, "Failed to alloc buffer of %" PRIu64 " bytes\n",
            ctx->buf_len);
    return -1;
  }
  (void)memset(ctx->va, 0, ctx->buf_len);
  /* va 就绪后再计算区域指针：数据区从缓冲起始 */
  ctx->client_data = (uint8_t *)ctx->va;
  if (!ctx->mgr->RegisterBuffer(ctx->va, ctx->buf_len)) {
    fprintf(stderr, "Failed to register buffer\n");
    return -1;
  }
  if (args->mbind && args->affinity_mode == AFF_AFFINITY) {
    int node = -1;
    if (args->single_chip >= 1 && args->single_chip <= 2) {
      /* 单 chip 内存亲和：绑到该 chip 对应的 NUMA 节点 */
      node = chip_to_first_numa(args->single_chip);
    } else {
      int dst_cpus[MAX_CPUS];
      int n = my_cpu_list(args, false, dst_cpus, MAX_CPUS); /* 含自动选择 */
      if (n > 0) {
        node = read_cpu_numa(dst_cpus[0]);
      }
    }
    if (node >= 0) {
      /* 整缓冲绑定（va 由 memalign 页对齐），保证数据区落在目的 NUMA */
      if (mbind_to_node(ctx->va, ctx->buf_len, node) != 0) {
        fprintf(stderr,
                "Warning: mbind to node %d failed, continue without NUMA "
                "binding\n",
                node);
      } else {
        printf("mbind buffer (%" PRIu64 "B) to node %d\n", ctx->buf_len,
               node);
      }
    }
  }
  return 0;
}

/* ---------------- 亲和: 每轮取源/目的 chip ---------------- */

/* 未指定 cpus 时按亲和模式自动选择：
 * affinity：从 chip1/chip2 各取 max(1, 目标数/2) 个 CPU（双 chip 均匀，保证
 * 双口打满；目标数 = 客户端线程数，服务器上限 8）；其它模式：全部 CPU。 */
static int auto_select_cpus(const argument_t *args, bool is_client, int *out,
                            int max_out) {
  if (args->affinity_mode != AFF_AFFINITY) {
    return enumerate_all_cpus(out, max_out);
  }
  int all[MAX_CPUS];
  int n_all = enumerate_all_cpus(all, MAX_CPUS);
  int c1[MAX_CPUS], c2[MAX_CPUS];
  int n1 = 0, n2 = 0;
  for (int i = 0; i < n_all; i++) {
    int chip = cpu_to_chip(all[i]);
    if (chip == 1)
      c1[n1++] = all[i];
    else if (chip == 2)
      c2[n2++] = all[i];
  }
  uint32_t target = args->threads > 0 ? (uint32_t)args->threads : 1;
  if (!is_client && target > 8)
    target = 8; /* 服务器：mbind 目标 node / first_dst_chip 用，够用即可 */
  uint32_t per = (target + 1) / 2;
  if (per < 1)
    per = 1;
  /* 从每个 chip 的 CPU 池随机选（seed 驱动，同 seed 可复现），
   * 避免固定选到最小 CPU（如 cpu 0）导致热点 */
  uint32_t rng = args->seed ^ 0x9e3779b9u;
  for (int i = 0; i < n1 && (uint32_t)i < per; i++) {
    int j = i + (int)(rng_next(&rng) % (uint32_t)(n1 - i));
    int tmp = c1[i];
    c1[i] = c1[j];
    c1[j] = tmp;
  }
  for (int i = 0; i < n2 && (uint32_t)i < per; i++) {
    int j = i + (int)(rng_next(&rng) % (uint32_t)(n2 - i));
    int tmp = c2[i];
    c2[i] = c2[j];
    c2[j] = tmp;
  }
  int n = 0;
  for (int i = 0; i < n1 && (uint32_t)i < per && n < max_out; i++)
    out[n++] = c1[i];
  for (int i = 0; i < n2 && (uint32_t)i < per && n < max_out; i++)
    out[n++] = c2[i];
  if (n == 0)
    return enumerate_all_cpus(out, max_out); /* 单 chip 兜底 */
  return n;
}

static int my_cpu_list(const argument_t *args, bool is_client, int *out,
                       int max_out) {
  const char *list = is_client ? args->source_cpus : args->destination_cpus;
  int n = parse_cpu_list(list, out, max_out);
  if (n > 0) {
    return n;
  }
  return auto_select_cpus(args, is_client, out, max_out);
}

static int first_dst_chip(const argument_t *args) {
  int dst_cpus[MAX_CPUS];
  int n = my_cpu_list(args, false, dst_cpus, MAX_CPUS); /* 含自动选择 */
  for (int i = 0; i < n; i++) {
    int chip = cpu_to_chip(dst_cpus[i]);
    if (chip > 0)
      return chip;
  }
  return -1;
}

/* 从不在 worker CPU 列表中的核里随机选一个（轮询线程用，seed 驱动可复现） */
static int auto_poll_cpu(const argument_t *args, bool is_client) {
  int used[MAX_CPUS];
  int n_used = my_cpu_list(args, is_client, used, MAX_CPUS);
  int free_cpus[MAX_CPUS];
  int n_free = 0;
  int all[MAX_CPUS];
  int n_all = enumerate_all_cpus(all, MAX_CPUS);
  for (int i = 0; i < n_all; i++) {
    bool in_used = false;
    for (int j = 0; j < n_used; j++) {
      if (all[i] == used[j]) {
        in_used = true;
        break;
      }
    }
    if (!in_used)
      free_cpus[n_free++] = all[i];
  }
  if (n_free == 0)
    return -1;
  uint32_t rng = args->seed ^ 0x51ed270bu;
  return free_cpus[rng_next(&rng) % (uint32_t)n_free];
}

static void pick_round_chips(const argument_t *args, bool is_client,
                             worker_t *w, int *src_a, int *src_b, int *dst) {
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
    for (int i = 0; i < np; i++)
      pool[i] = n_mine > 0 ? mine[i] : all_cpus[i];
    int c0 = pool[rng_next(&w->rng) % np];
    int ch0 = cpu_to_chip(c0);
    *src_a = ch0 > 0 ? ch0 : ((int)(w->id % 2) + 1);
    /* 源 B 优先从对侧 chip 的 CPU 里随机，保证双源分属两 chip（bonding 双源
     * 不退化）；对侧无可用 CPU（单 chip 拓扑）时回退全池随机 */
    int c1 = -1;
    int other = (ch0 > 0) ? ((ch0 == 1) ? 2 : 1) : -1;
    if (other > 0) {
      int cands[MAX_CPUS];
      int nc = 0;
      for (int i = 0; i < np && nc < MAX_CPUS; i++) {
        if (cpu_to_chip(pool[i]) == other) {
          cands[nc++] = pool[i];
        }
      }
      if (nc > 0) {
        c1 = cands[rng_next(&w->rng) % nc];
      }
    }
    if (c1 < 0) {
      c1 = pool[rng_next(&w->rng) % np];
    }
    int ch1 = cpu_to_chip(c1);
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

/* write 分片 chip 分配：每分片一对 (src, dst)。
 * affinity：源==目的==chip，分片在请求内序号 j → chip = j%2 ? 2 : 1（20 分片
 * 交替打散 = 10+10 均匀分两 chip）；anti：源随机、目的固定；none：全随机。 */
/* write 8M 请求 chip 分配：请求内 2 条 4M WR 同一 chip（src==dst）。
 * affinity：请求间交替 — 第 1 个 8M chip1、第 2 个 8M chip2（一批 10 个 = 5+5）；
 * anti：src 随机、dst 固定；none：全随机。 */
/* 请求内第 send_idx 次发送的 chip 分配（src==dst）：
 * affinity：10 次交替 — 第 1 次 chip1、第 2 次 chip2、第 3 次 chip1...（5+5）；
 * --single-chip：全部固定单 chip；anti：src 随机、dst 固定；none：全随机。 */
static void pick_send_chip(const argument_t *args, worker_t *w,
                           uint32_t send_idx, int *src, int *dst) {
  int dst_chip = first_dst_chip(args);
  if (dst_chip < 0) {
    dst_chip = (int)(w->id % 2) + 1;
  }
  if (args->affinity_mode == AFF_AFFINITY) {
    *src = *dst = (args->single_chip >= 1 && args->single_chip <= 2)
                      ? args->single_chip
                      : ((send_idx % 2 == 0) ? 1 : 2);
  } else if (args->affinity_mode == AFF_ANTI) {
    int all_cpus[MAX_CPUS];
    int n_all = enumerate_all_cpus(all_cpus, MAX_CPUS);
    int mine[MAX_CPUS];
    int n_mine = my_cpu_list(args, true, mine, MAX_CPUS);
    int np = n_mine > 0 ? n_mine : n_all;
    int c0 = (n_mine > 0 ? mine : all_cpus)[rng_next(&w->rng) % np];
    int ch0 = cpu_to_chip(c0);
    *src = ch0 > 0 ? ch0 : ((int)(w->id % 2) + 1);
    *dst = dst_chip;
  } else {
    int all_cpus[MAX_CPUS];
    int n_all = enumerate_all_cpus(all_cpus, MAX_CPUS);
    int c0 = all_cpus[rng_next(&w->rng) % n_all];
    int c1 = all_cpus[rng_next(&w->rng) % n_all];
    int ch0 = cpu_to_chip(c0);
    int ch1 = cpu_to_chip(c1);
    *src = ch0 > 0 ? ch0 : 1;
    *dst = ch1 > 0 ? ch1 : 1;
  }
  /* --drv-ext 关闭：全部 INVALID_CHIP（不走 chip 路由，与真实运行一致，
   * --query-chips 显示同样的覆盖后值） */
  if (!args->drv_ext) {
    *src = *dst = (int)INVALID_CHIP;
  }
}

/* ---------------- 客户端打流 ---------------- */

static int client_do_write(context_t *ctx, worker_t *w, int src_a, int src_b,
                           int dst_chip) {
  const argument_t *args = &ctx->args;
  kv_bench::UrmaManager *mgr = ctx->mgr;
  kv_bench::UrmaConnection &conn = *ctx->conn;
  uint32_t size = (uint32_t)args->value_size;
  uint32_t wr_len = size;
  uint64_t off_a = w->off;
  uint64_t off_b = w->off + wr_len;
  uint64_t server_data_len = (uint64_t)SERVER_DATA_WINDOW * size;
  uint64_t remote_a = conn.RemoteSegVa() + (off_a % server_data_len);
  uint64_t remote_b = conn.RemoteSegVa() + (off_b % server_data_len);

  /* 每轮从 jetty 池取一条新的 send lane（对齐 yuanrong AcquireSendLane） */
  std::shared_ptr<kv_bench::UrmaJetty> jetty;
  if (!mgr->AcquireSendLane(jetty)) {
    fprintf(stderr, "[wr] send lane pool exhausted\n");
    return -1;
  }

  uint64_t seq_a, seq_b;
  uint64_t ua = mgr->PostEvent(w->id, seq_a);
  uint64_t ub = mgr->PostEvent(w->id, seq_b);
  if (!mgr->PostWrite(jetty, conn, (uint64_t)ctx->client_data + off_a, remote_a,
                      wr_len, (uint32_t)src_a, (uint32_t)dst_chip, ua)) {
    fprintf(stderr, "[wr] post A failed\n");
    mgr->AbortEvent(w->id, seq_a);
    mgr->ReleaseSendLane(jetty);
    return -1;
  }
  if (!mgr->PostWrite(jetty, conn, (uint64_t)ctx->client_data + off_b, remote_b,
                      wr_len, (uint32_t)src_b, (uint32_t)dst_chip, ub)) {
    fprintf(stderr, "[wr] post B failed\n");
    mgr->AbortEvent(w->id, seq_b);
    (void)mgr->WaitEvent(w->id, seq_a,
                         args->timeout_ms); /* 等 A 完成避免残留 */
    mgr->ReleaseSendLane(jetty);
    return -1;
  }
  bool ok_a = mgr->WaitEvent(w->id, seq_a, args->timeout_ms);
  bool ok_b = mgr->WaitEvent(w->id, seq_b, args->timeout_ms);
  if (!ok_a) {
    fprintf(stderr, "[wr] wait A failed (seq=%lu, src_chip=%d, dst_chip=%d)\n",
            (unsigned long)seq_a, src_a, dst_chip);
  }
  if (!ok_b) {
    fprintf(stderr, "[wr] wait B failed (seq=%lu, src_chip=%d, dst_chip=%d)\n",
            (unsigned long)seq_b, src_b, dst_chip);
  }
  mgr->ReleaseSendLane(jetty);
  return (ok_a && ok_b) ? 0 : -1;
}

/* ---------------- write 流水线（请求 = 同一 8M buffer 发 10 次） ------------ */

/* post 一个请求到指定槽：同一 8M buffer 发 10 次（chip 交替），每次发送拆
 * 2 条 4M WR、【每条 4M WR 独立取一个 jetty】；20 条 WR 连续全部 post（不等
 * CQE），post_ns = 第 1 条 WR post 前（时延起点）。 */
static int post_one_req(context_t *ctx, worker_t *w, uint32_t slot_idx,
                        uint64_t req_seq) {
  const argument_t *args = &ctx->args;
  kv_bench::UrmaManager *mgr = ctx->mgr;
  kv_bench::UrmaConnection &conn = *ctx->conn;
  /* 数据区窗口：concurrency 块 8M buffer（重叠请求各占一块） */
  uint32_t window_reqs =
      (args->concurrency >= 1 ? (uint32_t)args->concurrency : 1);
  if (window_reqs < 1)
    window_reqs = 1;
  uint64_t base_off = (req_seq % window_reqs) * KV_SEND_SIZE;
  uint64_t remote_base = conn.RemoteSegVa() + (base_off % SERVER_PIPE_BYTES);

  wr_slot_t *s = &w->wr_slots[slot_idx];
  s->req_seq = req_seq;
  s->done_cnt = 0;
  s->active = true;
  s->post_ns = now_ns(); /* 时延起点：第 1 条 WR post 前 */
  /* 20 条 4M WR：send_idx = 第几次发送（chip 交替），half = 8M 前半/后半 */
  for (uint32_t wr = 0; wr < KV_WR_PER_REQ; wr++) {
    uint32_t send_idx = wr / KV_WR_PER_SEND;
    uint32_t half = wr % KV_WR_PER_SEND;
    int src, dst;
    pick_send_chip(args, w, send_idx, &src, &dst);
    std::shared_ptr<kv_bench::UrmaJetty> jetty;
    if (!mgr->AcquireSendLane(jetty)) {
      fprintf(stderr, "[pipe] req %llu lane exhausted at WR %u\n",
              (unsigned long long)req_seq, wr);
      /* 回滚已 post 的 WR */
      for (uint32_t j = 0; j < wr; j++) {
        (void)mgr->WaitEvent(w->id, s->seq[j], args->timeout_ms);
        mgr->ReleaseSendLane(s->jetty[j]);
      }
      s->active = false;
      return -1;
    }
    uint64_t seq = 0;
    uint64_t ue = mgr->PostEvent(w->id, seq);
    if (!mgr->PostWrite(
            jetty, conn,
            (uint64_t)ctx->client_data + base_off + (uint64_t)half * KV_WR_SIZE,
            remote_base + (uint64_t)send_idx * KV_SEND_SIZE +
                (uint64_t)half * KV_WR_SIZE,
            (uint32_t)KV_WR_SIZE, (uint32_t)src, (uint32_t)dst, ue)) {
      fprintf(stderr,
              "[pipe] req %llu WR %u (send %u) post failed, aborting\n",
              (unsigned long long)req_seq, wr, send_idx);
      mgr->AbortEvent(w->id, seq);
      mgr->ReleaseSendLane(jetty);
      for (uint32_t j = 0; j < wr; j++) {
        (void)mgr->WaitEvent(w->id, s->seq[j], args->timeout_ms);
        mgr->ReleaseSendLane(s->jetty[j]);
      }
      s->active = false;
      return -1;
    }
    s->jetty[wr] = jetty;
    s->seq[wr] = seq;
    s->done[wr] = false;
  }
  w->active_slots[w->active_count++] = slot_idx; /* 登记在飞槽 */
  return 0;
}

/* write 流水线主循环：
 * 请求 = 同一 8M buffer 发 10 次（chip 交替 1,2,1,2...），每次发送拆 2 条 4M WR
 * （每条独立 jetty，20 条 WR/请求）。
/* write 流水线主循环（请求级流水线重叠）：
 * 请求 = 同一 8M buffer 发 10 次（chip 交替 1,2,1,2...），每次发送拆 2 条 4M WR
 * （每条独立 jetty，20 条 WR/请求）。20 条 4M WR 连续全部 post（4M 之间不等
 * CQE）；请求之间不等前一个完成（重叠），在飞请求 ≤ --concurrency（各占一块
 * 8M buffer），完成一个补发一个。时延 = 请求第 1 条 WR post → 最后一条 CQE。 */
static int client_write_pipeline(context_t *ctx, worker_t *w,
                                 uint64_t deadline) {
  const argument_t *args = &ctx->args;
  kv_bench::UrmaManager *mgr = ctx->mgr;
  uint32_t concurrency =
      (args->concurrency >= 1 ? (uint32_t)args->concurrency : 1);
  if (concurrency > KV_MAX_CONCURRENCY)
    concurrency = KV_MAX_CONCURRENCY;
  /* 在飞请求数上限：重叠 = concurrency */
  uint32_t max_inflight_reqs = concurrency;
  uint64_t interval_ns = 0;
  if (args->qps > 0) {
    uint64_t per_thread = args->qps / args->threads;
    if (per_thread == 0)
      per_thread = 1;
    interval_ns = 1000000000ULL / per_thread; /* 请求/秒（每线程） */
  }
  uint64_t next_req_ns = now_ns();

  uint64_t req_seq = 0;  /* 请求全局序号 */
  uint32_t req_active = 0; /* 在飞请求数（≤ concurrency） */

  while (!w->stop && !ctx->fatal && now_ns() < deadline) {
    bool progressed = false;

    /* 1. 收完成：只遍历在飞槽列表；每槽探测 20 条 WR（done[i] 持久化，
     * ProbeEvent 成功即复位槽不能依赖单轮结果），20 条全完成 = 请求完成 */
    uint32_t k = 0;
    while (k < w->active_count) {
      uint32_t idx = w->active_slots[k];
      wr_slot_t *s = &w->wr_slots[idx];
      for (uint32_t i = 0; i < KV_WR_PER_REQ; i++) {
        if (s->done[i])
          continue;
        int r = mgr->ProbeEvent(w->id, s->seq[i]);
        if (r == -1) {
          fprintf(stderr,
                  "[pipe] req %llu WR %u failed (seq=%llu), aborting\n",
                  (unsigned long long)s->req_seq, i,
                  (unsigned long long)s->seq[i]);
          return -1;
        }
        if (r == 1) {
          s->done[i] = true;
          s->done_cnt++;
          progressed = true;
        }
      }
      if (s->done_cnt < KV_WR_PER_REQ) {
        k++; /* 请求未完成，处理下一个在飞槽 */
        continue;
      }
      /* 请求完成：20 条 WR 全部完成；带宽按传输字节 = 8M × 10 次 = 80M/请求 */
      __atomic_add_fetch(&w->bytes, KV_REQ_BYTES, __ATOMIC_RELAXED);
      for (uint32_t i = 0; i < KV_WR_PER_REQ; i++) {
        mgr->ReleaseSendLane(s->jetty[i]);
        s->jetty[i].reset();
      }
      s->active = false;
      /* 请求级时延：第 1 条 WR post → 最后一条 CQE */
      kv_hist_record(&w->hist_req, now_ns() - s->post_ns);
      __atomic_add_fetch(&w->ops, 1, __ATOMIC_RELAXED);
      if (req_active > 0)
        req_active--;
      /* 完成槽：从在飞列表移除（末尾交换）+ 归还空闲栈 */
      w->free_slots[w->free_count++] = idx;
      w->active_slots[k] = w->active_slots[w->active_count - 1];
      w->active_count--;
      /* 不 k++：换进来的槽仍需处理 */
    }

    /* 2. 发送：有请求就一直发（重叠），在飞请求 ≤ concurrency 或池空 */
    while (now_ns() < deadline && !ctx->fatal) {
      if (req_active >= max_inflight_reqs)
        break; /* 在飞达上限，等请求完成腾名额 */
      if (interval_ns > 0) {
        if (now_ns() < next_req_ns) {
          uint64_t left = next_req_ns - now_ns();
          if (left > 50000)
            sleep_ns(left - 50000);
          while (now_ns() < next_req_ns) {
          }
        }
        next_req_ns = now_ns() + interval_ns;
      }
      /* 取空闲槽（O(1) 栈） */
      if (w->free_count == 0)
        break; /* 槽满（≤ 10，理论不会） */
      uint32_t slot = w->free_slots[--w->free_count];
      if (post_one_req(ctx, w, slot, req_seq) != 0) {
        w->free_slots[w->free_count++] = slot; /* 归还槽 */
        return -1;
      }
      progressed = true;
      req_active++;
      req_seq++;
    }

    if (!progressed)
      sleep_ns(POLL_SLEEP_NS);
  }

  /* 收尾：只遍历在飞槽列表；只等未完成的 WR（done[i] 为 false 的）；
   * 已完成的槽已被 ProbeEvent 复位，再 WaitEvent 会白等超时。释放 jetty 避免残留 */
  while (w->active_count > 0) {
    uint32_t idx = w->active_slots[0];
    wr_slot_t *s = &w->wr_slots[idx];
    for (uint32_t i = 0; i < KV_WR_PER_REQ; i++) {
      if (!s->done[i])
        (void)mgr->WaitEvent(w->id, s->seq[i], args->timeout_ms);
      mgr->ReleaseSendLane(s->jetty[i]);
      s->jetty[i].reset();
    }
    s->active = false;
    w->free_slots[w->free_count++] = idx;
    w->active_slots[0] = w->active_slots[w->active_count - 1];
    w->active_count--;
  }
  return 0;
}

/* get：客户端直接 READ 服务器数据区（value-size 字节）到本地读缓冲 */
static int client_do_get(context_t *ctx, worker_t *w, int src_a, int dst_chip) {
  const argument_t *args = &ctx->args;
  kv_bench::UrmaManager *mgr = ctx->mgr;
  kv_bench::UrmaConnection &conn = *ctx->conn;
  uint32_t size = (uint32_t)args->value_size;
  uint64_t window = (uint64_t)args->threads * DATA_WINDOW_PER_THREAD * size;
  uint64_t off = w->off % window;
  uint64_t remote = conn.RemoteSegVa() + (off % ((uint64_t)SERVER_DATA_WINDOW * size));

  std::shared_ptr<kv_bench::UrmaJetty> jetty;
  if (!mgr->AcquireSendLane(jetty)) {
    fprintf(stderr, "[get] send lane pool exhausted\n");
    return -1;
  }
  uint64_t seq_e;
  uint64_t ue = mgr->PostEvent(w->id, seq_e);
  if (!mgr->PostRead(jetty, conn, (uint64_t)ctx->client_data + off, remote,
                     size, (uint32_t)src_a, (uint32_t)dst_chip, ue)) {
    fprintf(stderr, "[get] post read failed\n");
    mgr->AbortEvent(w->id, seq_e);
    mgr->ReleaseSendLane(jetty);
    return -1;
  }
  bool ok = mgr->WaitEvent(w->id, seq_e, args->timeout_ms);
  mgr->ReleaseSendLane(jetty);
  return ok ? 0 : -1;
}

static void *client_worker_main(void *arg) {
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
        fprintf(stderr, "[worker %u] pin to cpu %d failed: %s\n", w->id, cpu,
                strerror(errno));
      } else {
        printf("[worker %u] pinned to cpu %d\n", w->id, cpu);
      }
    }
  }

  uint64_t interval_ns = 0;
  if (args->qps > 0) {
    uint64_t per_thread = args->qps / args->threads;
    if (per_thread == 0)
      per_thread = 1;
    interval_ns = 1000000000ULL / per_thread;
  }
  uint64_t deadline = now_ns() + args->duration_sec * 1000000000ULL;
  w->next_post_ns = now_ns();
  /* get 每请求读 value-size；mixed 的 write 为 mirror 双 WR（2×size） */
  uint64_t round_bytes = (args->op == OP_GET) ? args->value_size
                                              : 2ULL * args->value_size;
  uint64_t window =
      (uint64_t)args->threads * DATA_WINDOW_PER_THREAD * args->value_size;
  w->off = (uint64_t)w->id * DATA_WINDOW_PER_THREAD * args->value_size;

  /* write：分片流水线（80MB 请求 = 20×4MB 分片，jetty 池驱动，持续到 deadline） */
  if (args->op == OP_WRITE) {
    if (client_write_pipeline(ctx, w, deadline) != 0) {
      __atomic_add_fetch(&w->errors, 1, __ATOMIC_RELAXED);
      if (!ctx->fatal) {
        ctx->fatal = true;
        fprintf(stderr,
                "[fatal] first write request failed (worker %u), aborting...\n",
                w->id);
      }
    }
    return NULL;
  }

  while (!w->stop && !ctx->fatal && now_ns() < deadline) {
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
    if (args->drv_ext && ctx->conn != nullptr &&
        ctx->conn->peer.dstChip != INVALID_CHIP) {
      dst_chip = (int)ctx->conn->peer.dstChip;
    }
    /* 默认不走 bonding chip 路由（has_drv_ext=0 + INVALID
     * chip，对齐参考默认路径）；
     * --drv-ext 显式开启后使用 src/dst chip 路由（自动值，参考 NumaIdToChipId）
     */
    if (!args->drv_ext) {
      src_a = src_b = dst_chip = INVALID_CHIP;
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
      /* 首错即中断：置 fatal，其它线程一并退出 */
      if (!ctx->fatal) {
        ctx->fatal = true;
        fprintf(stderr, "[fatal] first round failed (worker %u), aborting...\n",
                w->id);
      }
      break;
    }

    if (!args->fixed_offset) {
      w->off += round_bytes;
      w->off = w->off % window;
    }
    w->next_post_ns = t0 + interval_ns;
  }
  return NULL;
}

/* ---------------- 统计 ---------------- */

static void print_latency_line_us(const char *tag, const kv_hist_t *h) {
  uint64_t avg = (uint64_t)kv_hist_mean(h);
  uint64_t p50 = kv_hist_value_at_percentile(h, 50.0);
  uint64_t p90 = kv_hist_value_at_percentile(h, 90.0);
  uint64_t p99 = kv_hist_value_at_percentile(h, 99.0);
  uint64_t p999 = kv_hist_value_at_percentile(h, 99.9);
  uint64_t p9999 = kv_hist_value_at_percentile(h, 99.99);
  printf("%s latency(us): avg=%.3f min=%.3f p50=%.3f p90=%.3f p99=%.3f "
         "p999=%.3f p9999=%.3f pmax=%.3f\n",
         tag, (double)avg / 1000.0, (double)kv_hist_min(h) / 1000.0,
         (double)p50 / 1000.0, (double)p90 / 1000.0, (double)p99 / 1000.0,
         (double)p999 / 1000.0, (double)p9999 / 1000.0,
         (double)kv_hist_max(h) / 1000.0);
}

static const char *op_name(int op) {
  return op == OP_WRITE ? "write" : (op == OP_GET ? "get" : "mixed");
}

static const char *aff_name(int m) {
  return m == AFF_AFFINITY ? "affinity"
                           : (m == AFF_ANTI ? "anti-affinity" : "none");
}

static void print_client_summary(context_t *ctx, double seconds) {
  const argument_t *args = &ctx->args;
  kv_hist_t merged, merged_req;
  if (kv_hist_init(&merged, 1, 60ULL * 1000000000ULL, 3) != 0 ||
      kv_hist_init(&merged_req, 1, 60ULL * 1000000000ULL, 3) != 0) {
    return;
  }
  uint64_t ops = 0, bytes = 0, errors = 0;
  for (uint32_t i = 0; i < ctx->worker_count; i++) {
    kv_hist_merge(&merged, &ctx->workers[i].hist);
    kv_hist_merge(&merged_req, &ctx->workers[i].hist_req);
    ops += __atomic_load_n(&ctx->workers[i].ops, __ATOMIC_RELAXED);
    bytes += __atomic_load_n(&ctx->workers[i].bytes, __ATOMIC_RELAXED);
    errors += __atomic_load_n(&ctx->workers[i].errors, __ATOMIC_RELAXED);
  }
  printf("\n==== summary role=client op=%s threads=%u size=%" PRIu64
         " concurrency=%d affinity=%s "
         "jetty_count=%u duration=%.1fs ====\n",
         op_name(args->op), args->threads,
         args->op == OP_WRITE ? KV_SEND_SIZE : args->value_size,
         args->concurrency, aff_name(args->affinity_mode),
         ctx->mgr->Resource().SendJettyCount(), seconds);
  double iops = seconds > 0 ? (double)ops / seconds : 0.0;
  double bw_mb_s =
      seconds > 0 ? (double)bytes / seconds / 1e6 : 0.0; /* 大 B: MB/s */
  double bw_mbps = bw_mb_s * 8.0;                        /* 小 b: Mb/s */
  /* wr_rate：write = 请求 × 20 条 4M WR；带宽按 8M 数据/请求统计 */
  double wr_rate =
      iops * (args->op == OP_WRITE ? (double)KV_WR_PER_REQ : 1.0);
  printf("requests=%" PRIu64
         " iops=%.2f wr_rate=%.2f bandwidth=%.2f MB/s (%.2f Mb/s) "
         "bytes=%" PRIu64 " errors=%" PRIu64 "\n",
         ops, iops, wr_rate, bw_mb_s, bw_mbps, bytes, errors);
  if (args->op == OP_WRITE) {
    print_latency_line_us("request", &merged_req);
  } else {
    print_latency_line_us("request", &merged);
  }
  kv_hist_destroy(&merged);
  kv_hist_destroy(&merged_req);
}

static void *client_sampler_main(void *arg) {
  context_t *ctx = (context_t *)arg;
  const argument_t *args = &ctx->args;
  uint64_t last_ops = 0, last_bytes = 0;
  uint64_t t0 = now_ns();
  uint64_t last_t = t0;
  while (!ctx->stop) {
    sleep(
        (unsigned int)(args->report_interval > 0 ? args->report_interval : 1));
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
      double bw_mb_s = (double)(bytes - last_bytes) / dt / 1e6; /* 大 B: MB/s */
      printf("[t=%.1fs] ops=%" PRIu64
             " iops=%.2f bandwidth=%.2f MB/s (%.2f Mb/s) errors=%" PRIu64 "\n",
             elapsed, ops, (double)(ops - last_ops) / dt, bw_mb_s,
             bw_mb_s * 8.0, errors);
    }
    last_ops = ops;
    last_bytes = bytes;
    last_t = now;
  }
  return NULL;
}

/* ---------------- 客户端流程 ---------------- */

static int client_connect_and_exchange(context_t *ctx, const argument_t *args) {
  struct sockaddr_in addr;
  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0) {
    fprintf(stderr, "Failed to create socket: %d\n", errno);
    return -1;
  }
  addr.sin_family = AF_INET;
  addr.sin_port = htons(args->server_port);
  addr.sin_addr.s_addr = inet_addr(args->server_ip);
  /* 阻塞式连接（无超时，等待内核完成握手） */
  if (connect(sockfd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
    fprintf(stderr, "Failed to connect %s:%u, errno=%d (%s)\n",
            args->server_ip, args->server_port, errno, strerror(errno));
    close(sockfd);
    return -1;
  }

  kv_bench::HandshakeParams params;
  params.threads = args->threads;
  params.opCode = (uint32_t)args->op;
  params.valueSize = (uint32_t)args->value_size;
  params.transMode = args->trans_mode;
  params.dstChip = INVALID_CHIP;
  int dst_chip = first_dst_chip(args);
  if (dst_chip > 0) {
    params.dstChip = (uint32_t)dst_chip;
  }
  if (!ctx->mgr->ExchangeAsClient(sockfd, params, ctx->conn,
                                  args->import_rtp)) {
    fprintf(stderr, "Failed to exchange URMA info with server\n");
    close(sockfd);
    return -1;
  }
  printf("remote dst_chip=%u\n", ctx->conn->peer.dstChip);
  return sockfd;
}

static int create_workers(context_t *ctx, uint32_t count) {
  ctx->workers = new worker_t[count]; /* worker_t 含 shared_ptr，须用 new */
  if (ctx->workers == NULL)
    return -1;
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
    /* 空闲槽栈初始化（write 流水线用） */
    w->free_count = KV_MAX_WR_SLOTS;
    for (uint32_t s = 0; s < KV_MAX_WR_SLOTS; s++) {
      w->free_slots[s] = s;
    }
    if (kv_hist_init(&w->hist, 1, 60ULL * 1000000000ULL, 3) != 0 ||
        kv_hist_init(&w->hist_req, 1, 60ULL * 1000000000ULL, 3) != 0)
      return -1;
  }
  return 0;
}

static void free_bench_workers(context_t *ctx) {
  if (ctx->workers != NULL) {
    for (uint32_t i = 0; i < ctx->worker_count; i++) {
      kv_hist_destroy(&ctx->workers[i].hist);
      kv_hist_destroy(&ctx->workers[i].hist_req);
    }
    delete[] ctx->workers;
    ctx->workers = NULL;
    ctx->worker_count = 0;
  }
}

/* 统一清理：停 manager + 释放 worker/缓冲 + 关闭控制 socket（sockfd<0 时跳过）
 */
static void destroy_context(context_t *ctx, int sockfd) {
  if (sockfd >= 0) {
    close(sockfd);
  }
  /* 先释放对端连接（import 出的 target jetty/segment 需 urma_unimport_*），
   * 必须在 mgr->Stop()（内部 urma_uninit）之前，否则撞已卸载的库 */
  ctx->conn.reset();
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

static int run_client(const argument_t *args) {
  context_t *ctx = (context_t *)calloc(1, sizeof(context_t));
  if (ctx == NULL)
    return -1;
  ctx->args = *args;
  ctx->stop = false;
  ctx->mgr = new kv_bench::UrmaManager();
  int sockfd = -1;
  pthread_t sampler_thread = 0;

  /* 轮询线程绑核必须在 Init 之前（EnsurePollThread 在 Init 内启动）：
   * --poll-cpu 或自动选空闲核（与打流 worker 区分开） */
  ctx->mgr->SetPollCpu(args->poll_cpu >= 0 ? args->poll_cpu
                                           : auto_poll_cpu(args, true));

  /* jetty 池 ≥ max(线程数, write 在飞组窗口：req=10×并发度 / group=并发度) */
  uint32_t min_lanes = args->threads;
  if (args->op == OP_WRITE) {
    /* 池 ≥ 在飞请求 × 20（每请求 20 条 4M WR 各占一条 jetty）：
     * 池 ≥ 在飞请求 × 20（每请求 20 条 4M WR 各占一条 jetty） */
    uint32_t reqs = (args->concurrency >= 1) ? (uint32_t)args->concurrency : 1;
    uint32_t pipe = reqs * KV_WR_PER_REQ;
    if (pipe > min_lanes)
      min_lanes = pipe;
  }
  if (!ctx->mgr->Init(args->dev_name, args->cacheable, args->jetty_count,
                      min_lanes, args->event_mode, args->trans_mode)) {
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
    if (pthread_create(&ctx->workers[i].tid, NULL, client_worker_main,
                       &ctx->workers[i]) != 0) {
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

  if (ctx->fatal) {
    fprintf(stderr,
            "\n==== abort: first round error, run stopped early ====\n");
    /* 通知服务器结束（EOF 语义） */
    char sync_msg = 'E';
    (void)write(sockfd, &sync_msg, 1);
    close(sockfd);
    sockfd = -1;
    destroy_context(ctx, sockfd);
    return -1;
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

static int set_socket_nonblock(int fd) {
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

static void *server_conn_main(void *arg) {
  conn_t *conn = (conn_t *)arg;
  context_t *ctx = conn->ctx;

  kv_bench::HandshakeParams params;
  params.threads = ctx->args.threads;
  params.opCode = (uint32_t)ctx->args.op;
  params.valueSize = (uint32_t)ctx->args.value_size;
  params.transMode = ctx->args.trans_mode;
  params.dstChip = INVALID_CHIP;
  int dst_chip = first_dst_chip(&ctx->args);
  if (dst_chip > 0) {
    params.dstChip = (uint32_t)dst_chip;
  }
  if (!ctx->mgr->ExchangeAsServer(conn->fd, params, conn->conn,
                                  ctx->args.import_rtp)) {
    fprintf(stderr, "[conn] Failed to exchange URMA info\n");
    goto out;
  }

  printf("[conn] remote threads=%u op=%s dst_chip=%u\n",
         conn->conn->peer.threads, op_name((int)conn->conn->peer.opCode),
         conn->conn->peer.dstChip);

  /* get/mixed 为客户端直接 READ 服务器数据，服务器无需回写线程 */

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

out:
  conn->conn.reset();
  close(conn->fd);
  conn->used = false;
  return NULL;
}

static void *server_sock_thread_main(void *arg) {
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

static int server_listen(context_t *ctx, const argument_t *args) {
  int enable = 1;
  ctx->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (ctx->listen_fd < 0) {
    fprintf(stderr, "Failed to create socket_fd, err: [%d]%s.\n", errno,
            strerror(errno));
    return -1;
  }
  if (setsockopt(ctx->listen_fd, SOL_SOCKET, SO_REUSEADDR, &enable,
                 sizeof(int)) < 0) {
    fprintf(stderr, "Failed to setsockopt, err: [%d]%s.\n", errno,
            strerror(errno));
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
  if (bind(ctx->listen_fd, (struct sockaddr *)&addr, sizeof(struct sockaddr)) !=
      0) {
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
  if (pthread_create(&ctx->sock_thread, NULL, server_sock_thread_main, ctx) !=
      0) {
    fprintf(stderr, "Failed to create server thread\n");
    (void)close(ctx->listen_fd);
    return -1;
  }
  printf("server listening on port %u (dev=%s, seg va=0x%" PRIx64
         " len=%" PRIu64 ")\n",
         args->server_port, args->dev_name, (uint64_t)ctx->va, ctx->buf_len);
  return 0;
}

static int run_server(const argument_t *args) {
  context_t *ctx = (context_t *)calloc(1, sizeof(context_t));
  if (ctx == NULL)
    return -1;
  ctx->args = *args;
  ctx->stop = false;
  ctx->mgr = new kv_bench::UrmaManager();

  /* 轮询线程绑核必须在 Init 之前（EnsurePollThread 在 Init 内启动） */
  ctx->mgr->SetPollCpu(args->poll_cpu >= 0 ? args->poll_cpu
                                           : auto_poll_cpu(args, false));

  if (!ctx->mgr->Init(args->dev_name, args->cacheable, args->jetty_count, 1,
                      args->event_mode, args->trans_mode)) {
    fprintf(stderr, "failed to init URMA manager\n");
    destroy_context(ctx, -1);
    return -1;
  }
  if (setup_buffer(ctx, true) != 0) {
    destroy_context(ctx, -1);
    return -1;
  }
  /* 目的亲和: 服务器（write 目的）固定到 destination_cpus; 反亲和同样固定目的侧
   */
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
    {"report-interval", required_argument, NULL, 1012},
    {"mbind", no_argument, NULL, 1015},
    {"no-mbind", no_argument, NULL, 1031},
    {"drv-ext", no_argument, NULL, 1019},
    {"no-drv-ext", no_argument, NULL, 1030},
    {"import-rtp", no_argument, NULL, 1023},
    {"seed", required_argument, NULL, 1016},
    {"fixed-offset", no_argument, NULL, 1017},
    {"timeout-ms", required_argument, NULL, 1018},
    {"query-chips", no_argument, NULL, 1024},
    {"concurrency", required_argument, NULL, 1025},
    {"poll-cpu", required_argument, NULL, 1032},
    {"single-chip", required_argument, NULL, 1026},
    {NULL, 0, NULL, 0}};

static void usage(void) {
  printf("Usage:\n");
  printf("  -m, --trans-mode <mode>    urma mode: 0 for RM, 1 for RC, 2 for "
         "UM, 3 for RS (default 0)\n");
  printf("  -d, --dev-name <dev>       device name, e.g. bonding0 (default "
         "bonding0)\n");
  printf(
      "  -i, --server-ip <ip>       server ip address given only by client\n");
  printf("  -p, --server-port <port>   listen on/connect to port <port> "
         "(default %d)\n",
         DEFAULT_PORT);
  printf("  -e, --event-mode           use wait_jfc/ack/rearm event mode "
         "(default false)\n");
  printf(
      "      --value-size <bytes>   per-WR payload, e.g. 4M/8M (default 4M)\n");
  printf("      --qps <qps>            client target QPS in rounds/sec (0 = as "
         "fast as possible)\n");
  printf("      --duration <seconds>   client run duration (default 10)\n");
  printf("      --jetty-count <n>      min send Jetty count, 1..200 (default "
         "1; threads may raise it)\n");
  printf(
      "      --affinity-mode <m>    affinity | anti | none (default affinity)\n");
  printf("      --source-cpus <list>   client CPU list, e.g. 4,5 (default: auto "
         "per chip)\n");
  printf("      --destination-cpus <list> server CPU list, e.g. 8,9 (default: "
         "auto per chip)\n");
  printf("      --cacheable            register/import cacheable memory\n");
  printf("      --threads <n>          client load threads (default 1)\n");
  printf("      --concurrency <n>      write inflight requests 1..10 (default 1)\n");
  printf("      --single-chip <1|2>    single-chip affinity scenario: all 8M "
         "groups use one chip (src==dst), mbind to that chip's NUMA\n");
  printf("      --poll-cpu <n>         pin URMA poll thread to cpu (default: "
         "auto pick a non-worker cpu)\n");
  printf("      --op <op>              write | get | mixed (default write)\n");
  printf("      --mixed-ratio <pct>    write percentage in mixed mode (default "
         "50)\n");
  printf("      --report-interval <s>  periodic report interval (default 1)\n");
  printf("      --mbind                enable NUMA mbind of the buffer (default "
         "on; --no-mbind to disable)\n");
  printf("      --drv-ext              enable bonding chip routing "
         "(has_drv_ext + src/dst chip, default on; --no-drv-ext to disable)\n");
  printf("      --import-rtp           import remote jetty via plain RTP (skip "
         "bondp/CTP, workaround for driver crash)\n");
  printf("      --seed <n>             random seed for anti/none affinity "
         "(default 42)\n");
  printf("      --fixed-offset         always use offset 0 (hot-cache test) "
         "instead of cycling\n");
  printf("      --timeout-ms <ms>      completion timeout (default 5000)\n");
  printf("      --query-chips          print CPU->NUMA->chip and pick_round_chips "
         "selection, then exit\n");
}

static int validate_input_params(argument_t *args) {
  if (args->dev_name == NULL || args->value_size == 0 ||
      args->value_size > UINT32_MAX || args->duration_sec == 0 ||
      args->jetty_count == 0 || args->jetty_count > MAX_JETTY_COUNT) {
    fprintf(stderr, "Invalid device, value size, duration, or jetty count\n");
    return -1;
  }
  if (args->threads == 0 || args->threads > 512) {
    fprintf(stderr, "Invalid thread count %u\n", args->threads);
    return -1;
  }
  if (args->concurrency < 1 || args->concurrency > KV_MAX_CONCURRENCY) {
    fprintf(stderr, "Invalid concurrency %d (1..%d)\n", args->concurrency,
            KV_MAX_CONCURRENCY);
    return -1;
  }
  if (args->single_chip < 0 || args->single_chip > 2) {
    fprintf(stderr, "Invalid single-chip %d (0=dual chip, 1|2)\n",
            args->single_chip);
    return -1;
  }
  if (args->op < OP_WRITE || args->op > OP_MIXED) {
    fprintf(stderr, "Invalid op\n");
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

  if (strncmp(args->dev_name, "bonding", strlen("bonding")) == 0) {
    /* bonding 仅支持 RM 或 RC（jfs 固定 multi_path=1，对齐 datasystem） */
    if (args->trans_mode != 0 && args->trans_mode != 1) {
      fprintf(stderr,
              "Error: bonding device only supports RM or RC (-m 1), got "
              "trans-mode %u.\n",
              args->trans_mode);
      return -1;
    }
  }
  return 0;
}

static int parse_arguments(int argc, char *argv[], argument_t *args) {
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
  args->affinity_mode = AFF_AFFINITY; /* 默认亲和 */
  args->threads = 1;
  args->concurrency = 1;
  args->single_chip = 0;
  args->poll_cpu = -1;        /* 默认自动选空闲核给轮询线程 */
  args->op = OP_WRITE;
  args->mixed_ratio = 50;
  args->report_interval = 1;
  args->seed = 42;
  args->timeout_ms = DEFAULT_TIMEOUT_MS;
  args->drv_ext = true;  /* 默认开 chip 路由，--no-drv-ext 关闭 */
  args->mbind = true;    /* 默认 NUMA 绑定，--no-mbind 关闭 */

  while (1) {
    int c = getopt_long(argc, argv, "m:d:i:p:e", g_long_options, NULL);
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
      if (strcmp(optarg, "affinity") == 0)
        args->affinity_mode = AFF_AFFINITY;
      else if (strcmp(optarg, "anti") == 0 ||
               strcmp(optarg, "anti-affinity") == 0)
        args->affinity_mode = AFF_ANTI;
      else if (strcmp(optarg, "none") == 0)
        args->affinity_mode = AFF_NONE;
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
      if (strcmp(optarg, "write") == 0)
        args->op = OP_WRITE;
      else if (strcmp(optarg, "get") == 0)
        args->op = OP_GET;
      else if (strcmp(optarg, "mixed") == 0)
        args->op = OP_MIXED;
      else {
        fprintf(stderr, "Invalid op: %s\n", optarg);
        return -1;
      }
      break;
    }
    case 1010:
      args->mixed_ratio = (uint32_t)strtoul(optarg, NULL, 0);
      break;
    case 1012:
      args->report_interval = (uint32_t)strtoul(optarg, NULL, 0);
      break;
    case 1015:
      args->mbind = true;
      break;
    case 1031:
      args->mbind = false;
      break;
    case 1019:
      args->drv_ext = true;
      break;
    case 1030:
      args->drv_ext = false;
      break;
    case 1023:
      args->import_rtp = true;
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
    case 1024:
      args->query_chips = true;
      break;
    case 1025:
      args->concurrency = (int)strtol(optarg, NULL, 0);
      break;
    case 1026:
      args->single_chip = (int)strtol(optarg, NULL, 0);
      break;
    case 1032:
      args->poll_cpu = (int)strtol(optarg, NULL, 0);
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
    if (args->dev_name == NULL)
      return -1;
  }
  return validate_input_params(args);
}

/* --query-chips: 只查询 chip 路由选择（不初始化 URMA）。打印
 * CPU->NUMA->chip 拓扑、source/destination 列表的 chip，以及三种亲和模式下
 * pick_round_chips 会选出的 src_a/src_b/dst，便于核对配置。 */
static int run_chip_query(const argument_t *args) {
  /* 只打印结果：实际选中的 source/destination CPU（含自动选择）+ chip 路由结果 */
  printf("== source-cpus (client, %s) ==\n",
         args->source_cpus ? "explicit" : "auto-selected");
  {
    int cpus[MAX_CPUS];
    int n = my_cpu_list(args, true, cpus, MAX_CPUS); /* 含自动选择 */
    for (int i = 0; i < n; i++) {
      printf("  cpu %4d -> numa %2d -> chip %d\n", cpus[i],
             read_cpu_numa(cpus[i]), cpu_to_chip(cpus[i]));
    }
  }
  printf("== destination-cpus (server, %s) ==\n",
         args->destination_cpus ? "explicit" : "auto-selected");
  {
    int cpus[MAX_CPUS];
    int n = my_cpu_list(args, false, cpus, MAX_CPUS); /* 含自动选择 */
    for (int i = 0; i < n; i++) {
      printf("  cpu %4d -> numa %2d -> chip %d\n", cpus[i],
             read_cpu_numa(cpus[i]), cpu_to_chip(cpus[i]));
    }
  }
  printf("== first_dst_chip = %d ==\n", first_dst_chip(args));

  static const char *mode_names[] = {"affinity", "anti", "none"};
  uint32_t workers = args->threads > 0 ? args->threads : 1;
  for (int m = AFF_AFFINITY; m <= AFF_NONE; m++) {
    argument_t tmp = *args; /* pick_round_chips 按 args->affinity_mode 分支 */
    tmp.affinity_mode = m;
    printf("== pick_round_chips (mode=%s, threads=%u, seed=%u) ==\n",
           mode_names[m], workers, tmp.seed);
    for (uint32_t i = 0; i < workers; i++) {
      worker_t w{};
      w.id = i;
      w.rng = tmp.seed + i * 2654435761u;
      int sa, sb, dst;
      pick_round_chips(&tmp, true, &w, &sa, &sb, &dst);
      /* 与真实运行一致：--drv-ext 关闭时覆盖为 INVALID_CHIP */
      if (!tmp.drv_ext) {
        sa = sb = dst = (int)INVALID_CHIP;
      }
      printf("  worker %u: src_a=%d src_b=%d dst=%d\n", i, sa, sb, dst);
    }
    printf("== write 8M 请求 chip 分配 (mode=%s, 每请求发 %d 次, 每次 8M WR "
           "同 chip) ==\n",
           mode_names[m], KV_SENDS_PER_REQ);
    for (uint32_t i = 0; i < workers; i++) {
      worker_t w{};
      w.id = i;
      w.rng = tmp.seed + i * 2654435761u;
      printf("  worker %u: ", i);
      for (uint32_t j = 0; j < KV_SENDS_PER_REQ; j++) {
        int src, dst;
        pick_send_chip(&tmp, &w, j, &src, &dst);
        printf("%d%s", src, (j + 1 < KV_SENDS_PER_REQ) ? "," : "");
      }
      printf(" (每次发送 chip, src==dst)\n");
    }
  }
  return 0;
}

int main(int argc, char *argv[]) {
  argument_t args{};
  int ret;

  ret = parse_arguments(argc, argv, &args);
  if (ret != 0) {
    goto main_exit;
  }
  if (args.query_chips) {
    ret = run_chip_query(&args);
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
