# kv-bench manager/worker

`manager` 是独立的 Python 控制组件，基于 **FastAPI + uvicorn**；前端为
**Vue 3 + Vite + Element Plus** 单页应用（由 manager 直接托管构建产物）。
worker 是 `kv-bench --worker` 启动的常驻 HTTP 服务，不访问 manager；manager
通过 worker API 完成任务生命周期控制。

```mermaid
flowchart LR
    C[浏览器 / REST 调用方] -->|REST /v1| M[FastAPI Manager]
    M -->|SSH: rpm -qa / cmake / scp| A[Worker A]
    M -->|SSH: rpm -qa / cmake / scp| B[Worker B]
    M -->|HTTP start/stop| WA[Worker A API]
    M -->|HTTP start/stop| WB[Worker B API]
    WA --> KA[kv-bench]
    WB --> KB[kv-bench]
    KA <-->|URMA data plane| KB
```

## 安装依赖

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r manager/requirements.txt
```

## 启动

```bash
python3 -m manager.app --port 18080          # manager（默认 0.0.0.0:18080）
/opt/kv-bench/build/kv-bench --worker --worker-port=18082   # 每个 worker
```

启动后访问 `http://manager:18080/` 打开管理界面（节点管理 / 部署 / 任务）。
API 文档（Swagger UI）在 `http://manager:18080/docs`。

## 前端（Vue 3 + Element Plus）

- 源码位于 `manager/web/`，构建产物 `manager/web/dist/` 由 FastAPI 自动托管
  （`dist/assets` 存在即生效；不存在时首页显示构建提示）。
- 开发模式（后端 18080 常驻，Vite 代理 `/v1`）：

  ```bash
  cd manager/web
  npm install
  npm run dev        # http://127.0.0.1:5173
  ```

- 生产构建（提交/部署前执行一次）：

  ```bash
  cd manager/web
  npm run build      # 产出 dist/，之后无需 Node 环境
  ```

## API

### 节点管理

- `GET /v1/nodes`（别名 `GET /v1/workers`）：节点列表（不含密码）。
- `POST /v1/nodes`：保存节点。节点可携带多个 `tags`（字符串列表），用于在
  部署与创建任务时按标签筛选节点。节点信息保存在 manager 当前目录的 `nodes.json`；
  密码写入该文件但不会出现在 HTTP 响应中，生产环境应限制文件权限。
- `DELETE /v1/nodes/{name}`：删除节点。
- `PATCH /v1/nodes/{name}`：更新节点标签（`{"tags": [...]}`），保留密码等其余字段。

示例（带标签）：

```json
{
  "name": "node-a",
  "ip": "10.0.0.1",
  "tags": ["gpu", "idle"]
}
```

### 部署

`POST /v1/deploy`：manager 对每个节点执行 `rpm -qa` 并筛选 `urma`；
版本集合不一致时，仅对不一致节点执行独立的 CMake 编译，然后复制 artifact。
**部署不做 worker 常驻启动** —— worker 在任务启动时按任务拉起（见下）。

```json
{
  "nodes": [{"name":"a","ip":"10.0.0.1","api_port":18082}],
  "artifact": "build/kv-bench",
  "destination": "/opt/kv-bench/build/kv-bench",
  "source_dir": "/opt/kv-bench",
  "umdk_root": "/opt/umdk"
}
```

`umdk_root` 可选：留空（或省略）时，不一致节点的编译命令不加 `-DUMDK_ROOT`，
由 cmake 走系统/环境默认查找 URMA。

部署前 manager 会自动在远端 `mkdir -p` 目标目录（scp 不自动建目录）；
SSH/SCP 失败时返回 `{"error": "..."}` 并附 stderr 详情（如认证失败、
远端目录不可写、本地 artifact 不存在）。

响应 `{versions, consistent, reference}`：各节点 URMA 包版本、是否一致、参考版本。
部署状态（`worker_state = deployed`）写入 `deploy_status.json`，节点管理页可查看。

### 任务

`POST /v1/tasks` 的 `bench_items` 按拓扑指定：

```json
{
  "task_id": "bench-1",
  "workers": [
    {"name":"a","ip":"10.0.0.1"},
    {"name":"b","ip":"10.0.0.2"}
  ],
  "bench_items": [{"src":"10.0.0.1","dst":"10.0.0.2","type":"bidirectional"}],
  "options": {"op":"write","threads":4,"duration":30}
}
```

**任务生命周期**：创建（`queued`）→ 启动（`running`）→ 停止（`stopped`，
**可再次启动重跑**）→ 删除。`PUT /v1/tasks/{id}` 修改任务（`queued`/`stopped`
时可用，改后回到 `queued` 待执行，task_id 不可改）；`DELETE /v1/tasks/{id}`
删除任务（回收 worker/端口并删除 `runs/{task_id}/` 产物）。

**任务启动时按任务拉起 worker，端口错开（默认 18082 起顺延、空闲复用）**：

- `POST /v1/tasks/{id}/start`：manager 为任务中每个节点分配独立端口，ssh
  `nohup kv-bench --worker --worker-port=<端口> >/var/log/kv-bench-worker-{task_id}.log 2>&1 </dev/null &`
  拉起该任务的 worker，轮询 `/v1/health` 就绪后下发 bench。**顺序保证**：
  先下发被动端（server），轮询确认其数据面端口（`--server-port`，默认 13857）
  已监听，再下发主动端（client）—— client 启动时 server 必然已就绪；
  超时未监听则回滚并返回 `{"error": ...}`。多任务端口互不冲突，
  同一节点可并发多个任务；manager 与 worker 同机也适用。响应含 `worker_ports`。
- `POST /v1/tasks/{id}/stop`：经该任务端口停止 bench，ssh `pkill -f worker-port=<端口>`
  杀掉 worker 并释放端口。
- 任务自然结束后 worker 保活（结果/日志仍可查），端口占用到该任务被停止。
- `GET /v1/tasks/{id}/result`：聚合各 worker 结果（`ops/bytes/errors` → `iops/带宽`），
  并落盘到 `runs/{task_id}/result.json`。
- `GET /v1/tasks/{id}/logs`：SSH `tail` 各 worker 的 `/var/log/kv-bench-worker-{task_id}.log`
  并保存到 `runs/{task_id}/logs/{worker}.log`（拉取失败写 `.error`），返回日志内容。
  kv-bench 的 `main()` 已对 stdout 设置行缓冲（`setvbuf(_IOLBF)`），
  printf 的区间统计/summary 实时落盘，任务期间即可 tail 到。

`options` 覆盖 kv-bench 全部打流参数（`_` 转 `-` 后透传为 CLI 参数）：
标量 `{"threads": 4}` → `--threads=4`；开关 `{"event_mode": true}` → `--event-mode`；
默认开启的开关可关闭：`{"mbind": false}` → `--no-mbind`、`{"drv_ext": false}` → `--no-drv-ext`。

## 持久化与运行产物

manager 运行目录（默认当前目录）下的文件：

| 文件 | 内容 |
| --- | --- |
| `nodes.json` | 节点配置（含 SSH 密码，注意文件权限） |
| `tasks.json` | 任务与状态（创建/启动/停止/结果 + worker 端口映射实时持久化，manager 重启不丢） |
| `deploy_status.json` | 各节点部署状态（部署时间/artifact/URMA 版本一致性/worker 状态） |
| `runs/{task_id}/` | 每任务产物：`result.json`、`task.json`（任务快照）、`logs/{worker}.log`（SSH 拉取的 worker 日志）、`manager.log`（生命周期事件） |

## 测试

域逻辑测试仅依赖 Python 标准库（`fastapi`/`httpx` 缺失时自动跳过路由测试）：

```bash
python3 -m unittest discover -s manager/tests -v
```

## 备注

- 错误响应统一为 `{"error": "..."}`：pydantic 校验失败 422、逻辑错误 400、
  SSH/SCP/worker 调用等运行期失败 500（均带具体原因）。
- worker API 调用**绕过环境代理**（`httpx trust_env=False`，内网控制面直连）；
  manager 机器上配置的 HTTP_PROXY/HTTPS_PROXY 仅用于外网，不会劫持 worker 流量。
- worker API 不接受 manager 地址，也没有注册/回连逻辑。
