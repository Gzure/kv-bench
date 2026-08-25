# kv-bench

独立的跨节点 URMA KV 打流工具（参考 yuanrong-datasystem 的 URMA 实现精简）。
不启动、连接或依赖 YuanRong worker；两端都由同一个 `kv-bench` 二进制启动，
TCP 只用于交换 URMA segment/Jetty descriptor 与 bench 参数，数据面全部走 URMA。
无 bthread/brpc/protobuf/TBB 依赖。

```text
kv-bench (server)                    kv-bench --server-ip <server> ...
    |                                      |
    +-- descriptor control TCP -----------+
    +=========== URMA data plane =========+
```

## 构建

需要先构建 UMDK 并得到 `liburma`：

```bash
cmake -S . -B build -DUMDK_ROOT=/path/to/umdk
cmake --build build -j
```

`UMDK_ROOT` 只提供 URMA 头文件和库；YuanRong 源码仓不参与构建。

**注意：编译用的 UMDK 头必须与运行机器上的 `liburma*` 库同源（同版本）**。
`bondp_rjetty_t` 结构体在不同 UMDK 版本间布局有差异（新版含
`urma_bond_jetty_ext_t`），头文件与 `liburma_ubagg.so` 版本不匹配时，
`bondp_import_jetty` 会按错误布局解析结构体导致段错误（gdb 栈：
`bondp_import_jetty` ← `urma_import_jetty`）。

**实测踩坑**：在 26.06 机器上编译、把二进制直接传到 25.12.0-B105 机器上运行，
CTP import 段错误，且表现为"进程内存布局敏感"——仅给 `argument_t` 加 3 个无用
int 字段（或任何改布局的提交）就崩，改回去又可能不崩，极难排查。**真根因是
头库版本不匹配**（26.06 头编译的 `bondp_rjetty_t` 被 25.12 库按旧布局解析）。
修复：在目标机器上编译（或 `-DUMDK_ROOT` 指向与运行库同版本的头），或统一
各节点 UMDK 版本。

**import 默认 CTP 模式**（对齐 datasystem：bondp rjetty + `tp_type=CTP` +
`has_drv_ext=1`，失败自动回退 RTP）。以下场景可双端加 `--import-rtp` 显式走
普通 RTP import 绕行（RTP 路径用普通 `urma_rjetty_t`，跨版本稳定，不经过
`bondp_import_jetty`）：
- 头库版本不匹配、暂时无法重新编译时；
- UMDK 版本不一致导致 CTP 路径崩溃时。

## 分层（对齐 yuanrong-datasystem）

```text
业务层   src/kv_bench.cpp            选项/亲和(chip)/打流引擎/get 直接 READ/统计
管理层   src/urma/urma_manager.*     UrmaManager: init/设备发现/握手(交换+import)/AcquireSendLane/
                                     PostWrite 系列(内联 PostJettyRw)/轮询线程+事件槽/清理
资源层   src/urma/urma_resource.*    UrmaResource + RAII 句柄(context/jfce/jfc/jfr/jetty/segment)
池对象   src/urma/urma_send_lane.h   SendJettyPool（对齐 yuanrong：Add/PopIdle/Release/Remove/GetStats，
                                     线程安全内建；FIFO 空闲队列保证每轮取"新的"空闲 jetty）
Provider liburma (umdk)              urma_* API
```

数据面调用链：

```text
kv_bench 打流线程
  -> UrmaManager::AcquireSendLane                   (每轮从 Jetty 池取一条 lane)
  -> UrmaManager::PostWrite / PostWriteWithFlag     (管理 API)
  -> PostJettyRw / PostJettyRwWithFlag              (urma_manager.cpp 内联，对齐 YuanRong PostJettyRw)
  -> urma_post_jetty_send_wr()
  -> UrmaManager 轮询线程 urma_poll_jfc()           (完成事件槽通知)
  -> UrmaManager::ReleaseSendLane                   (lane 归还 Jetty 池)
```

## 快速开始

服务端：

```bash
./build/kv-bench --dev-name bonding0 --server-port 13857 \
  --value-size 4M --jetty-count 16 --affinity-mode affinity --destination-cpus 8,9
```

客户端（write 带宽/时延）：

```bash
./build/kv-bench --dev-name bonding0 --server-ip 10.0.0.20 --server-port 13857 \
  --value-size 4M --threads 16 --qps 0 --duration 30 \
  --jetty-count 16 --affinity-mode affinity --source-cpus 4,5 --destination-cpus 8,9
```

`--server-ip` 存在时进程是 client；没有该参数时是 server。两端的 `--dev-name`
应使用同一类 URMA/bonding 设备；`--destination-cpus` 两端保持一致（客户端用它计算目的 chip）。

## 打流模型（write：请求级流水线重叠）

**一个 KV 请求 = 同一 local 8MB buffer 写入 remote 80MB**（10 个连续且不重叠的
8MB 区间；chip 交替：第 1 次 chip1、第 2 次
chip2、第 3 次 chip1...，5+5），**每次发送拆 2 条 4MB WR，每条 4MB WR 独立取一个
jetty**（20 条 WR/请求）。**20 条 4M WR 连续全部 post（4M 之间不等 CQE）；请求之间
不等前一个完成（重叠）**，在飞请求 ≤ `--concurrency`（各占 local 8MB + remote
80MB），完成一个补发一个：

- 时延 = **第 1 条 WR post → 最后一条 CQE**（`request latency`）。
- 带宽 = **80MB 传输字节/请求**（同一 8M 数据发 10 次 = 10×8M）。
- **`--concurrency`**：在飞请求数（1..10，重叠度）。
- **`--single-chip 1|2`**：单 chip 场景——所有发送固定走该 chip（src==dst），
  `--mbind` 时缓冲绑到该 chip 对应的 NUMA 节点（测单 chip 极限 + 内存亲和）。

### 并发流程图（concurrency=N）

```
   ┌─────────────────────────── 主循环 ───────────────────────────┐
   │                                                              │
   │  ① 收完成：ProbeEvent 扫描在飞槽，20 条 WR 全完成 → 请求完成    │
   │     (bytes += 80M, 记 request latency, 释放 20 jetty, 腾窗口) │
   │                    │                                         │
   │                    ▼                                         │
   │  ② 发送：在飞请求 < N 且池有 jetty → post 新请求                │
   │     ┌─ post_one_req：local 8M 写 remote 80M ──────────────┐   │
   │     │  for wr in 0..19:                                   │   │
   │     │    取 1 个 jetty                                    │   │
   │     │    PostEvent + PostWrite(4M, chip=wr/2%2?2:1)      │   │
   │     │    （20 条 WR 连续 post，不等 CQE）                  │   │
   │     └────────────────────────────────────────────────────┘   │
   └───────────────┬───────────────────────────────▲──────────────┘
                   │ 在飞请求达 N / 池空 → break 等   │
                   └──────── 完成补发 ───────────────┘

时间线（concurrency=4，请求 r0..r3 重叠，每请求 20 条 WR 在飞）:

      r0: |======= 20 WR =======|
      r1:   |======= 20 WR =======|
      r2:     |======= 20 WR =======|
      r3:       |======= 20 WR =======|
  在飞: 4 个请求 × 20 WR = 80 条 4M WR（local 4×8M，remote 4×80M）
  完成: r0 完成 → 释放 → 补发 r4（窗口恒 4 请求）
```

## 操作类型

- `--op write`（Put，默认）：客户端分片流水线直写服务器内存（上述模型）。
- `--op get`（Get）：客户端**直接 READ 服务器数据区**（`URMA_OPC_READ`，一次读 `value-size`
  字节到本地读缓冲），CQE 完成记一次时延。服务器仅为数据源（预置数据），无回写线程。
- `--op mixed`：按 `--mixed-ratio`（write 占比%）混合（write 部分沿用旧的 mirror 双 WR 模型）。

## 亲和（bonding）

- `affinity`（**默认**）：**请求源==目的==同一 chip**（请求序号 `%2` 交替 chip1/chip2；
  `--single-chip` 时全部请求固定单 chip）、源线程绑定 `--source-cpus`、目的固定
  `--destination-cpus`。**不传 `--source-cpus`/`--destination-cpus` 时自动选择**：
  亲和模式下从 chip1/chip2 各取一半 CPU（客户端按线程数，服务器 ≤8）。
- `anti`/`anti-affinity`：源随机（每请求随机 chip），目的固定。
- `none`：两端都不绑定，源/目的 chip 每请求随机。

CPU 亲和（线程绑定 + mbind）恒生效；**bonding chip 路由（WR 的 `has_drv_ext` +
`src/dst_chip_id`）默认开启**（`--no-drv-ext` 关闭，对齐参考默认路径）；
**NUMA mbind 默认开启**（`--no-mbind` 关闭；部分平台/Kunpeng 不支持 mbind 会报
EINVAL 警告后继续）。
`--seed` 控制随机；`--fixed-offset` 恒压同一地址（热缓存测试）。

## 统计输出

- 每线程 HdrHistogram-lite（ns 精度，3 位有效数字），汇总输出
  `avg/min/p50/p90/p99/p999/p9999/pmax`（us）。**write 输出 `request latency`**：
  一个 8M 请求从第 1 条 WR post → 第 20 条 CQE 完成的时延。
- `--poll-cpu <n>`：URMA 轮询线程绑核（默认自动选一个非 worker 核，避免与
  打流线程争抢）。
- `--report-interval <s>` 周期打印瞬时 IOPS/带宽。
- 汇总行含 requests/IOPS/WR 速率（write = 20×IOPS，get = 1×IOPS）/带宽（MB/s）/errors。

```text
==== summary role=client op=write threads=16 size=4194304 concurrency=4 affinity=affinity jetty_count=200 duration=30.0s ====
requests=900000 iops=30000.00 wr_rate=600000.00 bandwidth=25165.82 MB/s (201326.59 Mb/s) bytes=75497472000 errors=0
request latency(us): avg=5333.33 min=1624.00 p50=5000.00 p90=6100.00 p99=8025.00 p999=17670.00 p9999=30480.00 pmax=208400.00
```

## Jetty 线性度扫描

**每个 8MB 组从 send Jetty 池取一条新的 jetty**（对齐 yuanrong `AcquireSendLane` 模型）：
池按 FIFO 空闲队列轮转 + in-use 标记分配，用后归还。**jetty 池容量 = 同时在飞组上限**
（池越大组并发越高，带宽/时延随之变化），用于观察 Jetty 数量对带宽/时延的线性度：
用于观察 Jetty 数量对带宽/时延的线性度：

```bash
for n in 1 2 4 8 16 32 64 128 200; do
  ./build/kv-bench --dev-name bonding0 --server-ip 10.0.0.20 \
    --server-port 13857 --value-size 4194304 --jetty-count "$n" \
    --qps 0 --duration 30 --affinity-mode affinity \
    --source-cpus 4,5 | tee result-$n.txt
done
```

池大小自动取 `max(jetty_count, threads, 10×并发度)`（保证并发度内的在飞组都有 jetty 可取）。

## 其它参数

| 参数 | 说明 |
| --- | --- |
| `--trans-mode` | 0=RM(默认) 1=RC 2=UM 3=RS |
| `--import-rtp` | import 对端 jetty 走普通 RTP 路径（默认 bondp/CTP；版本不匹配导致 bondp 崩溃时的绕行） |
| `--event-mode` | 使用 wait_jfc/ack/rearm 事件模式而非忙轮询 |
| `--cacheable` | 注册/导入 cacheable 段（默认 non-cacheable） |
| `--timeout-ms` | 完成等待超时（默认 5000） |
