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
版本集合不一致时，仅对不一致节点执行独立的 CMake 编译，然后复制 artifact
并以 nohup 启动 worker。

```json
{
  "nodes": [{"name":"a","ip":"10.0.0.1","api_port":18082}],
  "artifact": "build/kv-bench",
  "destination": "/opt/kv-bench/build/kv-bench",
  "source_dir": "/opt/kv-bench",
  "umdk_root": "/opt/umdk"
}
```

响应 `{versions, consistent, reference}`：各节点 URMA 包版本、是否一致、参考版本。

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

manager 调用 worker API：

- `POST /v1/tasks/{id}/start`：manager 为拓扑生成每个 worker 的 kv-bench 参数，
  并调用 worker 的 `POST /v1/tasks/start`；worker 在同一 kv-bench 进程内启动测试线程。
- `POST /v1/tasks/{id}/stop`：调用相关 worker 的 `POST /v1/tasks/{id}/stop`。
- `GET /v1/tasks/{id}/result`：聚合各 worker 结果（`ops/bytes/errors` → `iops/带宽`）。
- worker `GET /v1/health`：查看 worker 本地运行中的 kv-bench 任务。

## 测试

域逻辑测试仅依赖 Python 标准库（`fastapi`/`httpx` 缺失时自动跳过路由测试）：

```bash
python3 -m unittest discover -s manager/tests -v
```

## 备注

- 错误响应统一为 `{"error": "..."}`；pydantic 校验失败返回 422。
- worker API 不接受 manager 地址，也没有注册/回连逻辑。
