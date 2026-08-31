# kv-bench Manager 重构设计规格（FastAPI + Vue 3/Element Plus）

> 日期：2026-08-31
> 分支：`multi_bench`
> 状态：已获用户批准（2026-08-31）

## 1. 目标

将 `manager` 从 Python stdlib `http.server`（`ThreadingHTTPServer` + `BaseHTTPRequestHandler`）
与内联 HTML/JS console（`manager/console.py` 单字符串页面）重构为成熟开源栈：

- **后端**：FastAPI + uvicorn（自动 OpenAPI `/docs`、pydantic 入参校验、标准错误模型）。
- **前端**：Vue 3 + Vite + Element Plus 单页应用（节点管理 / 部署 / 任务 三视图），界面简洁美观。
- **交付**：单进程 —— FastAPI 托管 `manager/web/dist` 构建产物，`python3 -m manager.app --port 18080`
  入口与命令行参数不变；开发时 Vite dev server 将 `/v1` 代理到后端。

约束：**域逻辑语义与现有 REST 接口完全兼容**；现有 `unittest`（`manager/tests/test_manager.py`）
在不安装任何第三方包的情况下继续通过；worker API（C++ 侧 `src/kv_bench.cpp`）零改动。

## 2. 现状（基线）

- `manager/app.py`（380 行）：`Node`/`BenchItem`/`TaskSpec`/`VersionCheck` dataclass、
  `NodeStore`（JSON 持久化）、`SshExecutor`（sshpass+ssh/scp）、`HttpWorkerClient`（urllib 调 worker API）、
  `DeploymentManager`（版本一致性校验/按需编译/部署/拓扑任务计划/结果聚合）、`ApiHandler`（HTTP 层）、`serve/main`。
- `manager/console.py`：`CONSOLE_HTML` 单页内联前端（表格+textarea JSON 输入）。
- `manager/__init__.py`：`from .app import BenchItem, DeploymentManager, Node, NodeStore, TaskSpec`。
- `manager/tests/test_manager.py`：4 个 unittest，全部走 Fake 执行器/客户端，不触碰网络。
- worker HTTP API（C++）：`GET /v1/health`、`POST /v1/tasks/start {task_id, command}`、
  `POST /v1/tasks/{id}/stop`、`GET /v1/tasks/{id}/result`。

**现状隐患（本次修复）**：`GET /v1/tasks` 对 `TaskSpec` 做 `asdict()` 时会把 `workers` 中每个
`Node.password` 带进 HTTP 响应（`repr=False` 只影响 repr，不影响 asdict）。新实现所有任务/节点响应
统一剥除 `password`。

## 3. 目标结构

```
manager/
├── app.py               # 域逻辑（保持现有类与语义）+ 入口：uvicorn 启动
├── api.py               # 新增：FastAPI 应用（create_app）+ pydantic 请求模型 + 静态托管
├── console.py           # 删除（内联前端废弃）
├── requirements.txt     # fastapi / uvicorn[standard] / httpx
├── __init__.py          # 原导出不变（仅依赖 .app，不依赖 fastapi）
├── tests/
│   ├── test_manager.py  # 原 unittest 不变
│   └── test_api.py      # 新增：FastAPI 路由测试（skipUnless fastapi 可用）
└── web/                 # 新增：Vue 3 + Vite + Element Plus SPA
    ├── package.json / vite.config.ts / index.html / tsconfig.json / src/env.d.ts
    └── src/
        ├── main.ts / App.vue / router.ts / api.ts
        └── views/ Nodes.vue · Deploy.vue · Tasks.vue
```

## 4. 后端设计（FastAPI）

### 4.1 模块边界（避免循环/强依赖）

- `app.py` **不**在模块顶层 import fastapi/httpx：
  - `main()`/`serve()` 内部惰性 `from .api import create_app`；
  - `HttpWorkerClient` 改为 `try: import httpx except ImportError: httpx = None`，
    方法内 `httpx is None` 时抛 `RuntimeError("httpx 未安装：pip install -r manager/requirements.txt")`。
  - 保证 `from manager import ...` 与域测试在无第三方包环境可用。
- `api.py` 顶层 `from fastapi import ...`、`from .app import ...`（仅被惰性 import 或测试环境引用）。
- `manager/__init__.py` 保持原样。

### 4.2 REST 路由（与现状逐一对齐）

| 方法/路径 | 现状行为 | 新实现 |
| --- | --- | --- |
| `GET /` | 返回 `CONSOLE_HTML` | `web/dist` 存在 → `index.html`；否则返回简洁提示页（说明 `npm run build`） |
| `GET /v1/nodes`、`GET /v1/workers` | 节点列表（剥 password） | 同 |
| `POST /v1/nodes` | 保存节点 → 201 `{name, state}` | 同（pydantic `NodeIn` 校验） |
| `DELETE /v1/nodes/{name}` | 删除 → `{state: deleted}`，缺失 404 | 同 |
| `POST /v1/deploy` | 版本一致性→按需编译→拷贝→nohup 启动 worker，返回 `VersionCheck` dict | 同 |
| `GET /v1/tasks` | 任务列表（**泄漏 password，修复**） | 同但剥 password |
| `POST /v1/tasks` | 创建 → 201 `{task_id, state: queued}`；重名 400 | 同（pydantic `TaskIn`/`BenchItemIn` 校验，`src==dst` 仍 ValueError→400） |
| `POST /v1/tasks/{id}/start` | → `{task_id, state: running, commands}`；缺失 404 / 非 queued 400 | 同 |
| `POST /v1/tasks/{id}/stop` | → `{task_id, state: stopped}` | 同 |
| `GET /v1/tasks/{id}/result` | `collect_result` → `{workers, aggregate}` | 同 |

- 请求模型：`NodeIn`、`BenchItemIn`（`type: Literal[...]`）、`TaskIn`、`DeployIn`；
  转换回域对象（`Node(**x.model_dump())` 等），域逻辑零改动。
- 错误模型：全局异常处理器 `KeyError → 404 {"error": ...}`、`ValueError → 400 {"error": ...}`；
  pydantic 校验失败沿用 FastAPI 标准 422。
- `DeploymentManager.deploy` 等同步阻塞调用直接作为 sync 路由（FastAPI 自动跑线程池）。

### 4.3 静态托管

- `DIST = manager/web/dist`。存在 → `app.mount("/assets", StaticFiles(...))` + `GET /` 返回 `index.html`
  （前端用 hash 路由 `createWebHashHistory`，无需 SPA fallback 中间件）。
- 不存在 → `GET /` 返回内联提示页（styled，含安装/构建命令）。

### 4.4 依赖

`manager/requirements.txt`：

```
fastapi>=0.115
uvicorn[standard]>=0.30
httpx>=0.27
```

## 5. 前端设计（Vue 3 + Vite + Element Plus）

### 5.1 工程

- Vite 6 + `@vitejs/plugin-vue` 5 + TypeScript（`vue-tsc --noEmit` 类型检查）+ vue-router 4（hash 模式）。
- Element Plus 全量引入（内部工具，不必摇树）+ `zh-cn` locale；`@element-plus/icons-vue` 图标按需本地导入。
- `vite.config.ts`：`server.port 5173`，`proxy: { '/v1': 'http://127.0.0.1:18080' }`，`build.outDir: 'dist'`。
- `package.json` scripts：`dev`（vite）、`build`（`vue-tsc --noEmit && vite build`）。

### 5.2 布局（App.vue）

- `el-container`：左侧深色侧边栏（品牌名 + `el-menu` 三入口：节点管理/部署/任务，`router` 模式），
  右侧内容区 `router-view`。
- 全局样式：浅灰背景 `#f5f7fa`、卡片圆角、统一间距；中文界面。

### 5.3 视图

- **Nodes.vue**：节点表格（名称/地址/用户/SSH 端口/API 端口/工作目录/操作）+「新增节点」对话框表单
  （含 password 输入）+ 删除二次确认（`ElMessageBox`）；增删后刷新。
- **Deploy.vue**：部署表单（artifact/destination/source_dir/umdk_root）+ 从已存节点勾选目标节点；
  结果面板：各节点 URMA 包版本列表 + 一致性 tag（一致 / 不一致-已按需编译）；deploy 本身触发
  artifact 拷贝与 worker nohup 启动（无逐节点状态回传，接口语义与现状一致）。
- **Tasks.vue**：
  - 「创建任务」对话框：task_id、workers 多选（下拉自节点）、bench items 动态行编辑器
    （src/dst 下拉 + type 下拉：forward/reverse/bidirectional，可增删）、options 表单
    （op/threads/duration/qps/concurrency/value_size/jetty_count…）。
  - 任务表格：task_id、状态 `el-tag`（queued/running/stopped 配色）、bench items 摘要、操作（启动/停止/结果）。
  - 「结果」抽屉：聚合指标卡（ops / IOPS / 带宽 MB/s / errors，自绘统计卡）+ 每 worker 明细表 + JSON 检视。
- 数据流：仅调 manager API（`src/api.ts` axios 客户端），不直连 worker；Tasks 视图 3s 轮询刷新。

## 6. 测试与验证

- `manager/tests/test_manager.py`：不变，必须通过（纯 stdlib）。
- `manager/tests/test_api.py`（新增）：`@unittest.skipUnless(HAVE_FASTAPI)`，用 `TestClient(create_app(manager))`
  覆盖：nodes CRUD、deploy 版本检查、tasks 创建/启动/停止/结果、**password 不出现在任何响应**。
- 本沙箱（无外网）可执行：域 unittest、`python -m py_compile` 全部 Python 文件、Vue 源码静态检查；
  `pip install` / `npm install` / `vite build` 在用户侧执行（文档给出命令）。

## 7. 文档更新

- 重写 `manager/README.md`：安装依赖、启动（不变）、前端 dev/build、API 速查。
- `README.md`：「多节点 manager/worker 模式」小节提及 console 处改为 SPA，其余不动。
- `DESIGN.md`：§1.1 manager 描述追加一句「manager 基于 FastAPI，前端为 Vue 3 SPA」；接口语义不变。

## 8. 非目标（YAGNI）

- 不做 ECharts 图表（worker result 无时延分位数流，指标卡+表格已足够）。
- 不做任务历史持久化（现有内存态保持不变）。
- 不做登录鉴权（内网工具，现状无鉴权）。
- 不引入 vuex/pinia（三视图间无共享可变状态）。
