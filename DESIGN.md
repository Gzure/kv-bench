# kv-bench 设计文档（基于 URMA）

> 通用 URMA 性能测试工具：带宽 / 时延，模拟 KV 打流。
> URMA 实现参考同级目录 `yuanrong-datasystem`（`src/datasystem/common/rdma/*`），
> 剥离 bthread/brpc/protobuf/TBB 等第三方依赖，只保留"KV 调 URMA"的代码模型。

---

## 1. 目标

| 能力 | 说明 |
| --- | --- |
| 带宽测试 | 全速打流，统计吞吐（MB/s）与 IOPS |
| 时延测试 | 固定 QPS 节流打流，统计 avg / p50 / p90 / p99 / p999 / p9999 / pmax |
| 操作类型 | `write`（KV Put：客户端 WRITE 直写服务器内存）、`get`（KV Get：**服务器 WRITE 回写客户端缓冲**，对齐 datasystem worker `UbWriteHelper → UrmaWritePayload` 模型，见 §6.1） |
| 可配置 | 包大小 `--size`、线程数 `--threads`、时长 `--duration`、目标 QPS `--qps` |
| bonding 亲和 | `affinity`（源/目的都固定 CPU）、`anti-affinity`（源随机 & 目的固定）、`non-affinity`（源/目的都随机） |
| 依赖 | 仅 liburma + pthread；无 bthread/brpc/protobuf/TBB/gflags |

---

## 2. 参考实现分析（yuanrong-datasystem）

### 2.1 分层架构（参考 `docs/urma_call_flow.md`）

```
业务层      object_cache client/worker（Get/Put 回写、迁移）        ← 本工具剥离
封装层      fast_transport_manager_wrapper.{h,cpp}                ← 剥离（仅保留调用模型）
管理层      UrmaManager 单例 (urma_manager.{h,cpp})               ← 精简保留
资源层      UrmaResource (urma_resource.{h,cpp})                  ← 精简保留
Provider 层 liburma (umdk) urma_api.h，经 ds_urma_* shim 直链    ← 保留
```

### 2.2 关键流程（对应实现位置）

| 流程 | 参考实现 | 要点 |
| --- | --- | --- |
| 初始化 | `UrmaManager::Init` L246 / `UrmaResource::Init` L724 | `urma_init` → 设备候选（env `DS_URMA_DEV_NAME`，默认 `bonding_dev_0`，失败依次尝试其它 bonding 设备）→ `create_context(device, eidIndex)` → `create_jfce` → `create_jfc`（depth=max_jfc_depth）→ bonding 设备 `ChangeBondingBalanceMode`（BALANCE/PORT）→ `PreFillSendJettyPool`（默认 200 个 send jetty lane）→ 启动 `UrmaPollJfc` 轮询线程 |
| 握手 | `ProcessHandshakePeer` L2473 / `FinalizeOutboundConnection` L1622 | 交换双方 eid/uasid/本地 jettyId/segment(va,len)/host:port/instanceId；`ImportTargetJetty`（L1583，`ds_urma_get_rjetty` 委托 blob 或 legacy `bondp_rjetty`，tp_type=URMA_CTP，`flag.bs.has_drv_ext=1`）+ `import_seg` |
| 写 | `UrmaWritePayload` L1926 → `UrmaWritePayloadImpl` L1951 → `UrmaWriteImpl` L1724 | 查连接 → `GetRemoteSeg` → `GetOrRegisterSegment`（本地注册缓存）→ 分块（≤ `GetMaxWriteSize` = `FLAGS_urma_max_write_size_mb`×1MB 与设备能力取小）→ `PostJettyRw`（L1678：`urma_bondp_jfs_wr` 带 `src_chip_id/dst_chip_id`，`flag.bs.has_drv_ext=1` 开启跨 chip 路由）→ `ds_urma_post_jetty_send_wr`，CQE `user_ctx=reqId` |
| 读 | `UrmaRead` L2079 | 同上，`URMA_OPC_READ`，src/dst sge 对调 |
| 完成 | `ServerEventHandleThreadMain` L912 → `PollJfcWait` L1409 → `CheckCompletionRecordStatus` L1335 → `CheckAndNotify` L963 | busy-poll `ds_urma_poll_jfc`（最多 10 次，无事件 `nanosleep(1us)`）或事件模式 `wait_jfc+poll+ack+rearm`；完成事件 map（reqId→UrmaEvent，CV 等待，`WaitToFinish` L1229） |
| 亲和 | `PostJettyRw` + `GetAffinitySrcChipId` | chip id 来自 NUMA 映射（`NumaIdToChipId`）；`urma_bondp_jfs_wr_t` = `urma_jfs_wr_t base + int src_chip_id + int dst_chip_id` |

### 2.3 关键 ABI 类型（`urma_abi_compat.h` 镜像真实 `urma_api.h`）

- 句柄：`urma_device_t / urma_context_t / urma_jfce_t / urma_jfc_t / urma_jfr_t / urma_jetty_t / urma_target_jetty_t / urma_target_seg_t`
- 配置：`urma_jfc_cfg_t / urma_jfr_cfg_t / urma_jetty_cfg_t / urma_seg_cfg_t`
- WR：`urma_jfs_wr_t{opcode, flag(complete_enable), tjetty, user_ctx, rw{src,dst}, next}`、`urma_bondp_jfs_wr_t{base, src_chip_id, dst_chip_id}`、`urma_sge_t{addr,len,tseg}`、`urma_sg_t{sge,num_sge}`
- 完成：`urma_cr_t{status, completion_len, local_id(jettyId), user_ctx, opcode}`
- 常量：`URMA_OPC_WRITE=0 / URMA_OPC_READ=1`、`URMA_CR_SUCCESS=0`、`URMA_SUCCESS=0`、`URMA_EAGAIN`、`URMA_TM_RM`、`URMA_TYPICAL_MIN_RNR_TIMER=0x10`、`URMA_TYPICAL_RNR_RETRY=6`
- 资源规模：send jetty `JETTY_SIZE=256`，recv jetty `RECV_JETTY_JFS_DEPTH=32`，reqId 40-bit mask

### 2.4 剥离清单

| 参考依赖 | 剥离方案 |
| --- | --- |
| bthread/brpc（RPC 握手） | 自研 TCP 控制通道 + POD 序列化（见 §5） |
| protobuf（meta_transport.pb：UrmaHandshakeReqPb/RspPb、UrmaRemoteAddrPb） | POD 结构体 + 固定字节序 |
| TBB concurrent_hash_map | `std::mutex + std::unordered_map` |
| ThreadPool(RetireJfs)、UrmaAsyncEventHandler | bench 场景下 CQE 错误即打日志 + 标记失败；不实现 jetty 异步退休/补池 |
| OsXprtPipln / pipeline H2D / GatherWrite | 删除 |
| 内存分配器/shm arena/CUDA 注册/`InitMemoryBufferPool` | 直接 `mmap` + `register_seg` |
| flags 框架、metrics、perf 线程、故障恢复/降级（fallback TCP、success rate tracker） | 删除；CLI 自解析 |

---

## 3. 总体架构

```
kv-bench/
├── CMakeLists.txt
├── README.md
├── DESIGN.md
├── include/kvbench/
│   ├── options.h            CLI 选项解析（含 cpulist 解析）
│   ├── log.h                极简日志（级别 + 前缀，stderr）
│   ├── affinity.h           CPU 绑定 / chip id 解析 / NUMA 缓冲分配
│   ├── histogram.h          HdrHistogram-lite 时延直方图（3 位有效精度）
│   ├── stats.h              每线程计数器 + 周期合并 + 报表
│   ├── handshake.h          TCP 控制通道 + POD 消息
│   ├── urma_provider.h      ds_urma_* 直链 shim + ABI 类型（镜像 urma_api.h）
│   ├── urma_resource.h      简化资源层（context/jfce/jfc/jfr/segment/jetty 池）
│   ├── urma_manager.h       简化管理层（Init/握手/Write/Read/轮询/事件）
│   └── bench/
│       ├── kv_worker.h      单打流线程（QPS 节流 + 时延采样）
│       ├── client.h         客户端主控（起线程、聚合统计、收尾）
│       └── server.h         服务器（注册缓冲、握手、可选亲和绑定）
└── src/
    ├── main.cpp             入口：server | client 子命令
    ├── options.cpp / log.cpp / affinity.cpp / histogram.cpp / stats.cpp / handshake.cpp
    ├── urma_provider.cpp / urma_resource.cpp / urma_manager.cpp
    └── bench/kv_worker.cpp / client.cpp / server.cpp
```

线程模型（无 bthread，全部 std::thread）：

- **客户端**：1 个主控线程 + N 个打流线程 + 1 个 URMA 轮询线程（进程级共享 JFC）+ 1 个统计采样线程。
- **服务器**：1 个握手服务线程（每客户端连接一个）+ 可选 M 个亲和绑定线程（占位 CPU，稳定目的侧拓扑）+ **G 个 get 回写工作线程**（get 模式下轮询请求环并执行 `UrmaWritePayload` 回写，见 §6.1）+ 1 个 URMA 轮询线程。

---

## 4. URMA 层设计（对照参考精简）

### 4.1 `urma_provider.{h,cpp}`

- 直接 `-lurma` 链接（与参考一致：`urma_dlopen_util.cpp` 非 mock 路径就是直链），`ds_urma_*` shim 逐个转发到 `urma_*`。
- 头文件：真实环境 `#include <ub/umdk/urma/urma_api.h>`；为便于无 SDK 环境下编译检查，提供 `include/kvbench/abi/urma_abi_compat.h`（从参考 `urma_mock/abi/urma_abi_compat.h` 精简，仅保留本工具用到的类型，用宏 `KV_BENCH_USE_ABI_COMPAT` 切换）。

### 4.2 `urma_resource.{h,cpp}` —— 资源层

职责（对齐参考 `urma_resource`，去掉故障生命周期）：

```
class UrmaResource {
  Status Init(const std::string &devName, int eidIndex);  // 同参考 Init L724 流程
  // 句柄
  urma_context_t *ctx(); urma_jfce_t *jfce(); urma_jfc_t *jfc();
  uint64_t MaxWriteSize(); uint64_t MaxReadSize();        // 设备能力取小
  // segment
  Status RegisterSegment(uint64_t va, uint64_t len);      // 返回已注册 segment（缓存 map）
  // jetty
  Status CreateJetty(JettyType type, shared_ptr<UrmaJetty>& out);   // send: 共享 JFR; recv: 独立 JFR
  Status GetOrCreateSharedRecvJetty(shared_ptr<UrmaJetty>& out);    // 进程级 recv jetty（握手发布用）
  Status AcquireSendJetty(shared_ptr<UrmaJetty>& out);              // 从池取（--jetty-pool 个）
  void   ReleaseSendJetty(const shared_ptr<UrmaJetty>& jetty);
  // import（对端）
  Status ImportTargetJetty(const UrmaInfo& remote, unique_ptr<UrmaTargetJetty>& out); // 同参考 L1583
  Status ImportSegment(const urma_seg_t& seg, unique_ptr<UrmaRemoteSegment>& out);
};
```

- `UrmaJetty` 精简封装：持有 `urma_jetty_t*`、jettyId、共享 JFR；**去掉** PostGate 生命周期状态机（bench 不做故障退休），仅保留极简计数（若未来需要可再加）。
- send jetty 池：`SendJettyPool`（照搬参考 `urma_send_lane.h` 的池实现，去掉 lease 的 retire/force-release 分支，只留 acquire/release + 计数）。
- 参考默认池大小 200；本工具默认 `--jetty-pool = max(threads, 32)`，可配。

### 4.3 `urma_manager.{h,cpp}` —— 管理层

```
class UrmaManager {
  Status Init(const Options& opt);              // 参考 L246：urma_init→设备候选→Resource::Init→注册 bench 缓冲→起轮询线程
  Status StartHandshakeAsServer(TcpChannel& ch); // 参考 ProcessHandshakePeer L2473（TCP 版）
  Status FinalizeHandshakeAsClient(TcpChannel& ch);// 参考 FinalizeOutboundConnection L1622
  // KV 数据通路
  struct WriteOpt {                     // get 回写用：第二 src sge（完成标志）
    uint64_t extraSgeAddr = 0;          // 客户端 FlagArea 槽位地址
    uint32_t extraSgeLen = 0;           // 8（标志）
    bool     fence = false;             // --get-fence 回退：数据/标志分两条 WR
  };
  Status Write(const RemoteAddr& dst, uint64_t localOff, uint32_t size,
               int srcChip, int dstChip, bool blocking,
               const WriteOpt& opt = {});               // 参考 UrmaWritePayload(L1926)+UrmaWriteImpl(L1724)
  Status WaitToFinish(uint64_t reqId, int64_t timeoutMs);  // 参考 L1229
  void Stop();                                              // 停轮询线程 + 清理 + urma_uninit
};
```

关键实现要点（照搬参考语义，去掉业务杂质）：

1. **写**：查连接 map（key=对端 host:port，握手时建立）→ `connection->GetRemoteSeg(segVa)` → `GetOrRegisterSegment(本地)` → 分块循环：`GenerateReqId()`（40-bit）→ `CreateEvent`（reqId→事件，记 t_post）→ 池取 jetty lane → `PostJettyRw(URMA_OPC_WRITE, bondp_wr{src_chip_id,dst_chip_id, has_drv_ext})` → 失败清理事件/lane。
2. **get 回写**：复用同一条 Write 通路；`WriteOpt.extraSge` 时构造 2 个 src sge（数据 + 完成标志，`urma_sg_t.num_sge=2`，对齐参考 `UrmaGatherWrite` 多 sge 能力），`--get-fence` 时拆为数据 WR + fence 标志 WR。
3. **轮询线程**：`PollJfcWait` 精简版——`ds_urma_poll_jfc(jfc, 64, crs)`，无事件 `nanosleep(1us)` 退避；收到 CQE 按 `user_ctx=reqId` 找到事件：成功记完成时间并 `NotifyAll`；失败打日志 + 事件标记失败。（事件模式 `wait_jfc/ack/rearm` 保留为 `--event-mode` 选项，与参考一致。）
4. **事件**：`std::unordered_map<uint64_t, UrmaEvent>` + `std::mutex` + `std::condition_variable`；`UrmaEvent` 记录 `t_create / t_complete / status / dataSize`（替代参考的 tbbEventMap_ + Event/EventWaiter）。
5. **握手**：消息内容对齐参考 `UrmaJfrInfo` + segment 列表（见 §5），import 逻辑逐行照搬：
   - 服务器侧收到对端信息：`ImportRemoteJetty`（建连接 + import target jetty + import 对端 segment）→ 回包携带本地 recv jetty id + 本地 segment(s)。
   - 客户端侧：收服务器信息 → 建连接 + import target jetty + import segments → 回包本地信息 → 服务器再 import 客户端 jetty/segment。
   - 可选：采用参考的 rjetty 委托路径（`ds_urma_get_rjetty/put_rjetty` + `import_jetty(blob)`）；若 SDK 不支持则退回 legacy `bondp_rjetty` 构造路径（参考 L1604-1614），代码两条路径都保留，运行时按 `rjettyBuf` 是否非空选择。

### 4.4 连接与地址表示（替代 protobuf）

```
struct UrmaInfo {          // 对齐参考 UrmaJfrInfo
  uint8_t  eid[16];
  uint32_t uasid;
  uint32_t jettyId;        // 本地 recv jetty id（握手发布）
  char     host[64];
  uint16_t port;
  char     instanceId[40];
  uint32_t segCount;       // ≤16
  struct { uint64_t va; uint64_t len; } segs[16];
};
struct RemoteAddr {        // 对齐参考 UrmaRemoteAddrPb 语义
  uint64_t segVa;
  uint32_t segDataOffset;  // 本工具 = 0（每 op 偏移由打流层自算）
  char     host[64];
  uint16_t port;
};
struct UrmaConnection {    // 对齐参考 UrmaConnection
  std::unique_ptr<UrmaTargetJetty> targetJetty;
  UrmaInfo remoteInfo;
  std::unordered_map<uint64_t, std::unique_ptr<UrmaRemoteSegment>> remoteSegs; // segVa→imported
};
```

---

## 5. 握手控制通道（替代 brpc RPC）

纯 TCP + POD（固定布局、小端、magic+version），消息类型：

```
enum MsgType : u8 { HELLO_REQ=1, HELLO_RSP=2, URMA_INFO=3, READY=4, DONE=5, ERROR=6 };
struct MsgHeader { u32 magic; u8 version; u8 type; u16 len; };  // 8B，后跟 payload
```

流程（每客户端一条 TCP 连接）：

```
client                     server
  |-- HELLO_REQ -------------->|   (role=client, bench 参数预览: op/size/threads)
  |<-- HELLO_RSP + URMA_INFO ---|   (server: eid/uasid/recvJettyId/segs/host:port/instanceId)
  |-- URMA_INFO -------------->|   (client: 同上)
  |   (client import server jetty+segs)     (server import client jetty+segs)
  |-- READY ------------------>|
  |========= 打流（URMA 数据面，不走 TCP） =========|
  |-- DONE -------------------->|
```

- 服务器支持多客户端：每连接一个线程，各自独立建连接、独立打流（多客户端模式自然支持）。
- 序列化手写 `ReadExact/WriteExact` + 逐字段 memcpy，无 protobuf。

---

## 6. 打流引擎（KV 模型，模拟 QPS）

### 6.1 数据通路（对应 KV 语义，只走 URMA）

双方各分配并注册一段缓冲：客户端 `mmap(clientSegSize)`、服务器 `mmap(serverSegSize)`（亲和模式下按目的 CPU 的 NUMA node `mbind` 分配），握手时互相 `import_seg`（双向都已具备，write 与 get 共用）。

**write（Put）—— 客户端发起，数据 客户端→服务器：**

```
客户端打流线程:
  t0 = now()
  mgr.Write(serverSegVa + off, clientSegVa + localOff, size, srcChip, dstChip, blocking=true)
  t1 = now()          // CQE 到达即完成
```

对齐参考 `UrmaWritePayload(L1926) → UrmaWriteImpl(L1724) → PostJettyRw(URMA_OPC_WRITE)`。

**get（Get）—— 服务器 WRITE 回写，数据 服务器→客户端**（对齐 datasystem `worker_request_manager.cpp UbWriteHelper → UrmaWritePayload` 模型；因无 RPC 响应通道，用"请求环 + 内存完成标志"替代 RPC 应答）：

```
握手时在服务器目的缓冲中划分: [ RequestRing: R 槽 × sizeof(GetReq) ] [ DataArea ]
客户端缓冲划分:             [ DataArea ] [ FlagArea: 每线程 8B 完成标志 ]

GetReq { u64 clientSegVa; u32 clientOff; u32 size; u64 seq; u32 flagOff; }   // 32B POD

客户端打流线程 (每线程独立 seq):
  t0 = now()
  填 GetReq（targetOff、size、seq、flagOff=线程槽位）
  mgr.Write(serverSegVa + (seq % R) * sizeof(GetReq), 本地req槽, sizeof(GetReq), ..., blocking=true)  // WR① 请求
  spin 等待 flag[flagOff] == seq（acquire 语义 + 超时）                            // 见下
  t1 = now()          // 观察到标志即完成

服务器 get 回写线程 (每连接一个或共享, G 个, 轮询各自槽位):
  if ring[slot].seq > seen[slot]:
    mgr.Write(clientSegVa + req.clientOff, serverSegVa + dataOff, req.size,
              srcChip, dstChip, blocking=true, extraSge={clientSegVa + flagBase + req.flagOff, 8B})  // WR② 回写
    seen[slot] = req.seq
```

- **WR② 回写 = 单条 WRITE、2 个 src sge**（数据 sge + 标志 sge，对齐参考 `UrmaGatherWrite` 的多 sge 能力，jfs `max_sge=13`）：数据与完成标志同一条 WR 落客户端内存，客户端观察到 `flag==seq` 即数据可见（同一 WR 内 sge 顺序落盘）。
- 若目标平台对单 WR 多 sge 顺序无保证，提供 `--get-fence` 回退：数据 WR + fence 标志 WR 两条（`flag.bs.fence=1`），正确性优先、多一条 WR。
- 客户端各线程串行发 get（阻塞模型），`flag[flagOff]` 每线程独立，无写冲突；`--qps` 节流只作用于客户端请求发出速率，服务器回写线程全速排空（打流线程数默认 = 客户端线程数，可 `--server-workers` 调）。
- **时延口径（get）= t0(填请求) → t1(观察到 flag)**，即完整的 KV Get 往返（含请求 WR + 服务器排空 + 回写 WR）。

### 6.2 单线程打流循环

```
client thread:
for (elapsed < duration) {
  if (targetQps > 0) PaceToNextSlot();                  // §6.3
  t0 = now();
  op = PickOp();                                        // write/get/mixed(按比例)
  if (op==write) { mgr.Write(dst, off, size, srcChip, dstChip, true); }
  else           { PostGetRequest(...); WaitFlag(...); } // §6.1 WR① + 等待
  t1 = now();
  hist.Record(t1 - t0);                                 // 每线程独立直方图，无锁
  bytes += size; ops++; off = (off + size) % segLen;
}
```

- 全速模式（`--qps 0`）= 带宽测试；节流模式（`--qps N`）= 时延/QPS 测试，同时报吞吐。
- 服务器侧 get 回写线程循环：轮询请求环（busy-spin + 退避），有请求即 `mgr.Write` 回写，统计回写字节/次数（服务器侧报表可选）。

### 6.3 QPS 节流

- 每线程 slot 周期 `T = 1e9 / (qps / threads)` ns；维护 `nextSlotNs`，`PaceToNextSlot`：剩余 ≤50us 忙等，否则 `clock_nanosleep`；睡醒后按实际时间修正 `nextSlotNs`（漂移补偿），避免累计误差。

### 6.4 统计与报表（§7）

---

## 7. 统计模型

### 7.1 时延直方图（`histogram.h`）

HdrHistogram-lite：
- 单位 ns；`lowest=100ns, highest=60s, sigfigs=3` → 桶数 ≈ 1024×log2(6e11) ≈ 37k，内存 ~300KB/线程。
- 桶索引 = 编码函数（子桶 1024 幂次 + 指数段），`Record()` 原子（每线程私有，无竞争）。
- 汇总时跨线程 `Merge`，再算：`avg / min / p50 / p90 / p99 / p999 / p9999 / max`，输出单位自动（ns/us/ms）。

### 7.2 吞吐

- 每线程 `std::atomic<uint64_t> ops, bytes`；统计采样线程每 `--report-interval` 秒读一次快照，差分得到瞬时 IOPS 与 MB/s。
- **带宽口径按数据方向**：write = 客户端→服务器（客户端发出 payload 字节）；get = 服务器→客户端（服务器回写 payload 字节，客户端按其请求字节计，请求 WR① 不计入带宽）。

### 7.3 输出格式

```
[2026-xx-xx 12:00:01] [summary] duration=30.0s threads=16 op=write size=4096B affinity=affinity
  ops=18,000,000  iops=600,000/s  bandwidth=2343.75 MB/s  bytes=73.7GB  errors=0
  latency(us): avg=26.67  min=8.12  p50=25.00  p90=30.50  p99=40.12  p999=88.35  p9999=152.40  pmax=1042.00
```

---

## 8. bonding 亲和模型

### 8.1 chip id 解析

- cpu → NUMA node → chip id：读取 `/sys/devices/system/cpu/cpuN/nodeX`（避免依赖 libnuma）；chip id = 该 NUMA node 号（与参考 `NumaIdToChipId` 语义一致）。
- 亲和模式下 WR 设置 `bondp_wr.src_chip_id / dst_chip_id` 且 `flag.bs.has_drv_ext = 1`；非亲和模式下置 `INVALID_CHIP_ID(0xFF)`（同参考，禁用 drv ext）。

### 8.2 三种模式

**"源 / 目的"按数据方向定义**（谁发 WR 谁是源，数据落在谁的内存谁是目的），与具体 op 的进程角色解耦：

- write（Put）：源 = 客户端打流线程，目的 = 服务器内存；
- get（Get）：源 = **服务器回写线程**（发 WR②），目的 = **客户端缓冲**（被回写），角色与 write 互换。

| 模式 | 源侧（发 WR 的线程） | 目的侧（数据落内存方） | src_chip_id | dst_chip_id |
| --- | --- | --- | --- | --- |
| `affinity` 亲和 | 固定绑定（write: 客户端 `--src-cpus`；get: 服务器 `--dst-cpus`） | 固定绑定 + 缓冲按目的 NUMA 分配 | 固定 = chip(源 cpu) | 固定 = chip(目的 cpu) |
| `anti-affinity` 反亲和 | 不固定（每 op 从源 cpu 池随机取 chip） | 固定绑定 `--dst-cpus` | 每 op 随机 | 固定 |
| `non-affinity` 非亲和 | 不固定，chip 随机 | 不固定，chip 随机 | 每 op 随机 | 每 op 随机 |

- 随机性：每 op 从对应 cpu 列表取随机 cpu → 其 chip id（每线程私有 `std::mt19937`，避免锁竞争；`--seed` 可复现）。
- CPU 绑定：`pthread_setaffinity_np`（Linux）。服务器 `--dst-cpus` 绑定线程数 = 目的 CPU 数（write 目的 / get 源，占位稳定拓扑；数据面硬件直达，无需拷贝 CPU）。
- NUMA 缓冲：目的方缓冲按目的 chip 的 NUMA node `mbind(MPOL_BIND)` 分配（write=服务器缓冲；get=客户端缓冲），保证数据真正落在目的 chip；`--no-mbind` 可关。
- 未指定 cpus 时默认：亲和 = 源 `0..threads-1`、目的按 `--dst-cpus` 或物理核数；反亲和/非亲和 = 随机池取全部可用 CPU。

---

## 9. CLI 设计

```
kv-bench server --port 8000 [--seg-size 1073741824] [--dst-cpus 0,4,8,12]
                [--server-workers 16] [--urma-dev bonding_dev_0] [--eid 0]
                [--event-mode] [--jetty-pool 64]

kv-bench client --server 10.0.0.1:8000
                --threads 16 --op write|get|mixed --size 4096
                --duration 30 --qps 0
                --affinity affinity|anti-affinity|non-affinity
                [--src-cpus 0-15] [--dst-cpus 0,4,8,12]
                [--mixed-ratio 50]            # mixed 模式下 write 占比
                [--get-fence]                 # get 回写拆成 数据WR+fence标志WR
                [--report-interval 1] [--seg-size 268435456]
                [--urma-dev bonding_dev_0] [--eid 0] [--event-mode]
                [--jetty-pool 64] [--timeout-ms 5000] [--seed 42]
```

- cpulist 语法：`0,2,4-7` 区间+列表。
- 环境变量（对齐参考）：`DS_URMA_DEV_NAME`（默认 `bonding_dev_0`）可覆盖 `--urma-dev`。

---

## 10. 构建

- CMake ≥ 3.16，Linux x86_64/arm64，`-pthread`。
- 查找 liburma：参考 `FindURMA.cmake` 思路——优先 `URMA_INCLUDE_LOCATION` / `URMA_LIB_LOCATION`（下载包模式），否则系统路径 `/usr/include`、`/usr/lib64/urma`。
- 无 SDK 环境编译检查：`-DKV_BENCH_USE_ABI_COMPAT=ON` 使用自带 ABI 镜像头（仅编译用，运行仍需真实 liburma）。
- 可选 `-DKV_BENCH_WITH_MOCK=ON`：移植参考 `urma_mock` 的极简内存回环 backend，支持无硬件自测（P1 后加入，见 §12）。

---

## 11. 错误处理与清理（简化）

- 打流期错误：单 op 失败 → 计数 errors++、打日志，继续；连续失败阈值（默认 100）→ 中止打流并报错退出（服务器侧回写失败同样计入服务器 errors，DONE 消息带回客户端汇总）。
- 超时：客户端 `WaitFlag` / `WaitToFinish(timeoutMs)` 超时 → 计数 error、jetty lane 释放（不回退重试）。
- 清理顺序（对齐参考 Stop/Clear）：停轮询线程 → 释放并 `delete` jetty/jfr/jfc/jfce/context → `unimport_seg`/`unimport_jetty` → `unregister_seg` → `urma_uninit` → 关闭 TCP。

---

## 12. 实施阶段

| 阶段 | 内容 | 产出 |
| --- | --- | --- |
| P0 | 骨架：CMake、options/log、histogram、stats、affinity | 可编译框架，单元自测直方图/亲和解析 |
| P1 | urma_provider + urma_resource（init/segment/jetty 池） | 资源层初始化日志正确 |
| P2 | urma_manager（写/轮询/事件/握手 import）+ TCP handshake | 双端握手成功，import 日志正确 |
| P3 | 打流引擎：write 全速/节流 + 统计报表 | write 模式跑通，指标输出 |
| P4 | get 回写：请求环 + FlagArea + 服务器回写线程（含 `--get-fence` 回退） | get 模式跑通 |
| P5 | bonding 亲和三模式（绑定 + chip id + mbind，含 get 角色互换） | 三种模式 × 两种 op 可运行 |
| P6 | 打磨：多客户端、mixed、错误处理、README、真实环境验证 | 发布 |

---

## 13. 风险与待确认

| 项 | 说明 |
| --- | --- |
| 真实环境验证 | 本仓库无 URMA 硬件/SDK，代码在 Linux + liburma 环境编译验证；建议先在目标机器跑通 `urma_init`/`create_context` |
| get 完成标志顺序 | 默认单 WR 2 sge（数据+标志）；若平台不保证单 WR 内 sge 顺序，启用 `--get-fence` 回退（数据 WR + fence 标志 WR） |
| 握手通道 | TCP + POD（已确认，无 brpc/protobuf） |
| 随机粒度 | 每 op 随机 chip（已确认） |
| rjetty 委托 | 优先走 `get_rjetty` 委托路径，SDK 不支持时退回 legacy 构造路径（代码双路径保留） |

已确认决策：get 采用**服务器 WRITE 回写**模型（§6.1）；不做 mock；亲和"源/目的"按数据方向定义（§8.2）。
