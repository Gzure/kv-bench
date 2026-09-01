import axios from 'axios'

export type BenchType = 'forward' | 'reverse' | 'bidirectional'
export type TaskState = 'queued' | 'running' | 'stopped'

export interface Node {
  name: string
  ip: string
  user: string
  ssh_port: number
  workdir: string
  binary: string
  api_port: number
  tags: string[]
  deploy?: NodeDeployStatus | null
}

export interface NodeDeployStatus {
  deployed_at: string
  artifact: string
  destination: string
  versions: string[]
  consistent: boolean
  worker_state: string
}

export interface NodeInput extends Node {
  password: string
}

export interface BenchItem {
  src: string
  dst: string
  type: BenchType
}

export interface Task {
  task_id: string
  workers: Node[]
  bench_items: BenchItem[]
  options: Record<string, unknown>
  state: TaskState
  result: Record<string, unknown>
}

export interface VersionCheck {
  versions: Record<string, string[]>
  consistent: boolean
  reference: string[]
}

export interface WorkerResult {
  state?: string
  ops?: number
  bytes?: number
  errors?: number
  error?: string
}

export interface TaskResult {
  workers: Record<string, WorkerResult>
  aggregate: {
    ops: number
    bytes: number
    errors: number
    iops: number
    bandwidth_mb_s: number
  }
}

export interface TaskLogs {
  task_id: string
  directory: string
  workers: Record<string, { content: string; error: boolean }>
  manager_log: string
}

export interface DeployPayload {
  nodes: NodeInput[]
  artifact: string
  destination: string
  source_dir: string
  umdk_root: string
}

export interface TaskPayload {
  task_id?: string
  workers: NodeInput[]
  bench_items: BenchItem[]
  options: Record<string, unknown>
}

const http = axios.create({ timeout: 120_000 })

export const api = {
  // 节点
  listNodes: async (): Promise<Node[]> => (await http.get('/v1/nodes')).data,
  saveNode: async (node: NodeInput) => (await http.post('/v1/nodes', node)).data,
  deleteNode: async (name: string) => (await http.delete(`/v1/nodes/${encodeURIComponent(name)}`)).data,
  patchNodeTags: async (name: string, tags: string[]): Promise<Node> =>
    (await http.patch(`/v1/nodes/${encodeURIComponent(name)}`, { tags })).data,

  // 部署
  deploy: async (payload: DeployPayload): Promise<VersionCheck> =>
    (await http.post('/v1/deploy', payload)).data,

  // 任务
  listTasks: async (): Promise<Task[]> => (await http.get('/v1/tasks')).data,
  createTask: async (payload: TaskPayload) => (await http.post('/v1/tasks', payload)).data,
  startTask: async (id: string) => (await http.post(`/v1/tasks/${encodeURIComponent(id)}/start`)).data,
  stopTask: async (id: string) => (await http.post(`/v1/tasks/${encodeURIComponent(id)}/stop`)).data,
  taskResult: async (id: string): Promise<TaskResult> =>
    (await http.get(`/v1/tasks/${encodeURIComponent(id)}/result`)).data,
  taskLogs: async (id: string): Promise<TaskLogs> =>
    (await http.get(`/v1/tasks/${encodeURIComponent(id)}/logs`)).data,
}

/** 归一化 axios / 后端错误消息（后端统一返回 {"error": ...}）。 */
export function errMsg(error: unknown): string {
  if (axios.isAxiosError(error)) {
    const data = error.response?.data as { error?: string } | undefined
    return data?.error ?? error.message
  }
  return String(error)
}

/** 收集全部节点的去重标签（排序后）。 */
export function collectTags(nodes: Node[]): string[] {
  const tags = new Set<string>()
  for (const node of nodes) {
    for (const tag of node.tags ?? []) tags.add(tag)
  }
  return [...tags].sort((a, b) => a.localeCompare(b, 'zh-CN'))
}

/** 节点是否匹配标签筛选（任一匹配 ANY）。 */
export function matchesTags(node: Node, selected: string[]): boolean {
  if (!selected.length) return true
  return selected.some((tag) => (node.tags ?? []).includes(tag))
}

/** kv-bench CLI 打流参数 -> 创建任务面板（_ 转 -；bool 开关 True->--flag，
 *  mbind/drv-ext 默认开、False 转 --no-flag；空值不传走默认）。 */
export interface BenchOptionField {
  key: string
  label: string
  group: string
  type: 'number' | 'string' | 'select' | 'bool'
  options?: Array<{ value: string; label: string }>
  default?: boolean | number | string
  hint?: string
}

export const BENCH_OPTION_FIELDS: BenchOptionField[] = [
  // ---- 基础 ----
  { key: 'op', label: '操作类型', group: '基础', type: 'select', options: [
    { value: 'write', label: 'write（Put）' },
    { value: 'get', label: 'get（直接 READ）' },
    { value: 'mixed', label: 'mixed' },
  ] },
  { key: 'threads', label: '线程数', group: '基础', type: 'number', default: 1 },
  { key: 'duration', label: '时长 (s)', group: '基础', type: 'number', default: 10 },
  { key: 'qps', label: '目标 QPS（0=全速）', group: '基础', type: 'number' },
  { key: 'concurrency', label: '并发度', group: '基础', type: 'number', hint: 'write 在飞窗口' },
  { key: 'concurrency_unit', label: '并发单位', group: '基础', type: 'select', options: [
    { value: 'req_group', label: 'req_group（在飞批次）' },
    { value: 'req', label: 'req（在飞请求）' },
  ] },

  // ---- 数据模型 ----
  { key: 'value_size', label: 'Value 大小（字节）', group: '数据模型', type: 'number', hint: '默认 4M = 4194304' },
  { key: 'jetty_count', label: 'Jetty 池大小', group: '数据模型', type: 'number', hint: '1..200' },
  { key: 'trans_mode', label: '传输模式', group: '数据模型', type: 'select', options: [
    { value: '0', label: '0（RM）' },
    { value: '1', label: '1（RC）' },
    { value: '2', label: '2（UM）' },
    { value: '3', label: '3（RS）' },
  ] },
  { key: 'single_chip', label: '单 chip', group: '数据模型', type: 'select', options: [
    { value: '0', label: '0（关闭）' },
    { value: '1', label: '1（chip1）' },
    { value: '2', label: '2（chip2）' },
  ] },
  { key: 'fixed_offset', label: '恒压 offset 0', group: '数据模型', type: 'bool', hint: '热缓存测试' },

  // ---- 亲和与 CPU ----
  { key: 'affinity_mode', label: '亲和模式', group: '亲和与 CPU', type: 'select', options: [
    { value: 'affinity', label: 'affinity（源==目的同 chip）' },
    { value: 'anti', label: 'anti（源随机，目的固定）' },
    { value: 'none', label: 'none（全随机）' },
  ] },
  { key: 'source_cpus', label: '源 CPU 列表', group: '亲和与 CPU', type: 'string', hint: '如 4,5 或 4,6-8' },
  { key: 'destination_cpus', label: '目的 CPU 列表', group: '亲和与 CPU', type: 'string', hint: '如 8,9' },
  { key: 'poll_cpu', label: '轮询线程 CPU', group: '亲和与 CPU', type: 'number', hint: '默认自动选非 worker 核' },
  { key: 'mbind', label: 'NUMA mbind', group: '亲和与 CPU', type: 'bool', default: true, hint: '默认开，关闭传 --no-mbind' },

  // ---- 协议与模式 ----
  { key: 'dev_name', label: '设备名', group: '协议与模式', type: 'string', hint: '如 bonding0' },
  { key: 'event_mode', label: '事件模式', group: '协议与模式', type: 'bool', hint: 'wait_jfc/ack/rearm' },
  { key: 'cacheable', label: 'Cacheable 段', group: '协议与模式', type: 'bool', hint: '默认 non-cacheable' },
  { key: 'drv_ext', label: 'chip 路由 (drv-ext)', group: '协议与模式', type: 'bool', default: true, hint: '默认开，关闭传 --no-drv-ext' },
  { key: 'import_rtp', label: 'RTP import 绕行', group: '协议与模式', type: 'bool', hint: '头库版本不匹配时的绕行' },

  // ---- 统计与稳定性 ----
  { key: 'mixed_ratio', label: 'mixed 写占比 (%)', group: '统计与稳定性', type: 'number' },
  { key: 'report_interval', label: '上报间隔 (s)', group: '统计与稳定性', type: 'number' },
  { key: 'seed', label: '随机种子', group: '统计与稳定性', type: 'number', hint: '默认 42' },
  { key: 'timeout_ms', label: '完成等待超时 (ms)', group: '统计与稳定性', type: 'number', hint: '默认 5000' },
]

export const BENCH_OPTION_GROUPS: string[] = (() => {
  const groups: string[] = []
  for (const field of BENCH_OPTION_FIELDS) {
    if (!groups.includes(field.group)) groups.push(field.group)
  }
  return groups
})()
