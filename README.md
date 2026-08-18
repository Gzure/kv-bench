# kv-bench

独立的跨节点 URMA WRITE 测试工具。它不启动、连接或依赖 YuanRong worker；两端都由同一个 `kv-bench` 二进制启动，TCP 只用于交换 URMA 段和 Jetty descriptor，数据面直接调用 URMA。

```text
kv-bench --server                 kv-bench --server-ip <server> --qps ...
    |                                      |
    +-- descriptor control TCP -----------+
    +=========== URMA WRITE data plane ===+
```

## 构建

需要先构建 UMDK 并得到 `liburma`：

```bash
cmake -S . -B build -DUMDK_ROOT=/path/to/umdk
cmake --build build -j
```

`UMDK_ROOT` 只提供 URMA 的头文件和库；CMake 不会加入 `yuanrong-datasystem`。

## 启动两端

服务端只负责创建并导出目标段/Jetty，不能用 YuanRong worker 代替：

```bash
./build/kv-bench --dev-name bonding0 --server-port 13857 \
  --value-size 4M --jetty-count 1 --affinity-mode affinity \
  --destination-cpus 8,9
```

客户端连接服务端并打流：

```bash
./build/kv-bench --dev-name bonding0 --server-ip 10.0.0.20 \
  --server-port 13857 --value-size 4M --qps 0 --duration 30 \
  --jetty-count 1 --affinity-mode affinity --source-cpus 4,5
```

`--server-ip` 存在时进程是 client；没有该参数时是 server。两端的 `--dev-name` 应使用同一类 URMA/bonding 设备，服务端端口是控制面 TCP 端口，不是 URMA 数据面端口。

## Bonding 亲和模式

- `affinity`：client 使用 `--source-cpus`，server 使用 `--destination-cpus`。
- `anti`/`anti-affinity`：client 不绑定，server 使用 `--destination-cpus`。
- `none`：两端都不绑定，交给系统调度。

CPU 列表由 bench 进程自身通过 `sched_setaffinity` 设置；它不会修改系统或 YuanRong 配置。

## 4MB/8MB 和 Jetty 线性度

每次完成一个 WRITE WQE 后复用对应 local Jetty；`--jetty-count N` 创建 N 个独立 send Jetty，并按轮询方式发送，范围 `1..200`。因此可以固定 4MB write，逐步扫描 Jetty 数量：

```bash
for n in 1 2 4 8 16 32 64 128 200; do
  ./build/kv-bench --dev-name bonding0 --server-ip 10.0.0.20 \
    --server-port 13857 --value-size 4194304 --jetty-count "$n" \
    --qps 0 --duration 30 --affinity-mode affinity \
    --source-cpus 4,5 | tee result-$n.txt
done
```

每个结果包含 QPS、带宽和 WRITE completion 平均时延。将 `bandwidth(N)/bandwidth(1)` 与 `latency(N)/latency(1)` 作图即可观察线性度。`--value-size 8388608` 用于 8MB 场景。

当前工具测的是已注册 URMA buffer 的 warm data-plane latency，不是 YuanRong KV cache 命中率；它用于隔离观察缓存/内存热态对 URMA WRITE completion 的影响。

默认注册为 non-cacheable；增加 `--cacheable` 可切换为 cacheable 段。建议保持其余参数不变分别跑两组，对比 `first_latency_us` 和 `avg_latency_us`。
