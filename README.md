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

## 分层（对齐 yuanrong-datasystem）

```text
业务层   src/kv_bench.cpp            选项/亲和(chip)/打流引擎/get 请求环与回写/统计
管理层   src/urma/urma_manager.*     UrmaManager: init/设备发现/握手(交换+import)/AcquireSendLane/
                                     PostWrite 系列(内联 PostJettyRw)/轮询线程+事件槽/清理
资源层   src/urma/urma_resource.*    UrmaResource + RAII 句柄(context/jfce/jfc/jfr/jetty/segment)
池对象   src/urma/urma_send_lane.h   SendJettyPool（对齐 yuanrong：Add/PopIdle/Release/Remove/GetStats，
                                     线程安全内建；游标轮转保证每轮取"新的"空闲 jetty）
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

## 打流模型（每轮双源并发发送）

**每轮（round）= 从 bonding 设备的两个源 CPU 并发发出 2 条 WRITE**（两条同时 in-flight，
`src_chip_id = {chip1, chip2}`，bondp driver extension 路由到两个源端口）：

- `--dual-mode mirror`（默认）：两条 WR 各发满 `value-size`，轮数据量 = `2×size`（压满 bonding 双端口）；
- `--dual-mode split`：两条 WR 各发 `size/2`，轮数据量 = `size`（要求 size 为偶数）。

## 操作类型

- `--op write`（Put，默认）：客户端双源并发 WRITE 直写服务器内存，双 CQE 完成记一轮时延。
- `--op get`（Get）：对齐 datasystem worker `UbWriteHelper -> UrmaWritePayload` 模型——
  客户端把 32B 请求 WRITE 进服务器请求环，服务器回写线程并发 2 条 WRITE
  （单 WQE 2-sge：数据 + 完成标志；`--get-fence` 可拆成 data WR + fence flag WR）
  回写客户端缓冲，客户端观察到双 flag 完成记一轮时延。
- `--op mixed`：按 `--mixed-ratio`（write 占比%）混合。

## 亲和（bonding）

- `affinity`：源固定（客户端线程绑定 `--source-cpus`，每条 WR 固定 chip1/chip2）、
  目的固定（`--destination-cpus` + 数据区 `mbind` 到目的 NUMA node）。
- `anti`/`anti-affinity`：源随机（每轮随机 chip），目的固定。
- `none`：两端都不绑定，源/目的 chip 每轮随机。

CPU 亲和（线程绑定 + mbind）恒生效；**bonding chip 路由（WR 的 `has_drv_ext` +
`src/dst_chip_id`）默认关闭**（对齐参考默认路径，部分平台 post 时会报
`URMA_CR_LOC_ACCESS_ERR`），显式 `--drv-ext` 开启。
`--seed` 控制随机；`--no-mbind` 关闭 NUMA 绑定；`--fixed-offset` 恒压同一地址（热缓存测试）。

## 统计输出

- 每线程 HdrHistogram-lite（ns 精度，3 位有效数字），汇总输出
  `avg/min/p50/p90/p99/p999/p9999/pmax`（us）。
- `--report-interval <s>` 周期打印瞬时 IOPS/带宽。
- 汇总行含 requests/IOPS/WR 速率（= 2×IOPS）/带宽（MB/s）/errors。

```text
==== summary role=client op=write threads=16 size=4194304 dual=mirror affinity=affinity jetty_count=16 duration=30.0s ====
requests=9000000 iops=300000.00 wr_rate=600000.00 bandwidth=25165.82 MB/s (201326.59 Mb/s) bytes=75497472000 errors=0
round latency(us): avg=53.33 min=16.24 p50=50.00 p90=61.00 p99=80.25 p999=176.70 p9999=304.80 pmax=2084.00
```

## Jetty 线性度扫描

**每轮（round）从 send Jetty 池取一条新的 jetty**（对齐 yuanrong `AcquireSendLane` 模型）：
池按游标轮转 + in-use 标记分配，用后归还。`jetty_count` 越大，每轮拿到的 jetty 越分散，
用于观察 Jetty 数量对带宽/时延的线性度：

```bash
for n in 1 2 4 8 16 32 64 128 200; do
  ./build/kv-bench --dev-name bonding0 --server-ip 10.0.0.20 \
    --server-port 13857 --value-size 4194304 --jetty-count "$n" \
    --qps 0 --duration 30 --affinity-mode affinity \
    --source-cpus 4,5 | tee result-$n.txt
done
```

池大小自动取 `max(jetty_count, threads)`（保证每个打流线程每轮都有 jetty 可取）。
服务器 get 回写线程数 = 客户端线程数（`--server-workers` 可覆盖），且总回写线程数不超过服务器 jetty 数。

## 其它参数

| 参数 | 说明 |
| --- | --- |
| `--trans-mode` | 0=RM(默认) 1=RC 2=UM 3=RS |
| `--tp-type` / `--multi-path` | bonding 设备建议保持默认（RM+multi_path 或 RC） |
| `--event-mode` | 使用 wait_jfc/ack/rearm 事件模式而非忙轮询 |
| `--cacheable` | 注册/导入 cacheable 段（默认 non-cacheable） |
| `--timeout-ms` | 完成等待超时（默认 5000） |
| `-c, --cs-coexist` | 单进程双角色（仅 loopback 调试） |
