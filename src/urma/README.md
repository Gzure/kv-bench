# URMA layer

对齐 yuanrong-datasystem `src/datasystem/common/rdma/*` 精简后的 URMA 栈，
业务层（`src/kv_bench.cpp`）只与本层交互，不直接调用 `urma_api.h`。

## 分层

| 文件 | 角色 | 对应 yuanrong |
| --- | --- | --- |
| `urma_send_lane.h` | `SendJettyPool`：send Jetty 池管理对象（Add/PopIdle/Release/Remove/GetStats/At），**线程安全内建于对象**（内部 mutex），PopIdle 按 FIFO 空闲队列轮转保证每轮取到"新的"空闲 jetty | `urma_send_lane.h::SendJettyPool` |
| `urma_resource.{h,cpp}` | 资源层：RAII 句柄（context/jfce/jfc/jfr/jetty/target jetty/segment）+ `UrmaResource`（设备查询、队列创建、`SendJettyPool` 池、register/import、token、设备能力） | `urma_resource.{h,cpp}` |
| `urma_manager.{h,cpp}` | 管理层：`UrmaManager`（urma_init/设备发现/`RegisterBuffer`/`Exchange` 握手与 import/`AcquireSendLane`/`PostWrite` 系列/轮询线程+每线程事件槽/`Stop`）、`UrmaConnection`、`PostJettyRw` WR 构造 | `urma_manager.{h,cpp}` |

## 说明

- `PostJettyRw`（WRITE）/`PostJettyRd`（READ）直接内联在
  `urma_manager.cpp`：保留 SGE 方向、completion、target Jetty、driver extension、
  bonding chip 字段；READ 为 src/dst sge 对调（对齐参考 `UrmaRead`）。
- 已移除 yuanrong 的故障生命周期（PostGate 状态机/退休/补池/异步事件）、protobuf、
  lane lease、metrics、worker RPC 与 bthread 依赖；bench 场景不做故障恢复。
- 每轮（一次业务请求）经 `UrmaManager::AcquireSendLane` 从资源层 Jetty 池取一条
  新的 send jetty，用后 `ReleaseSendLane` 归还（池按 FIFO 空闲队列轮转 + in-use 标记分配）。
- 完成模型：`UrmaManager` 起独立轮询线程 `urma_poll_jfc`；每个 worker 从固定空闲
  事件槽池领取带 generation 的 token，按 `user_ctx = workerId<<40 | token` 完成。
  槽只在 `ProbeEvent` / `WaitEvent` / `AbortEvent` 消费后归还，迟到 CQE 不会覆盖新事件。`WaitEvent` 超时只取消事件 token，不取消硬件 WR；调用方会隔离对应 send lane，直到连接销毁。
- 控制面握手为 TCP + POD（`WireInfo`），数据面只走 URMA。
