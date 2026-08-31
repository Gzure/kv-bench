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

/** 常见打流参数 -> kv-bench CLI 选项（_ 转 -，空值跳过）。 */
export interface BenchOptionField {
  key: string
  label: string
  type: 'number' | 'select'
  options?: string[]
}

export const BENCH_OPTION_FIELDS: BenchOptionField[] = [
  { key: 'op', label: '操作类型', type: 'select', options: ['write', 'get', 'mixed'] },
  { key: 'threads', label: '线程数', type: 'number' },
  { key: 'duration', label: '时长(s)', type: 'number' },
  { key: 'qps', label: '目标 QPS(0=全速)', type: 'number' },
  { key: 'concurrency', label: '并发度', type: 'number' },
  { key: 'value_size', label: 'Value 大小(字节)', type: 'number' },
  { key: 'jetty_count', label: 'Jetty 池大小', type: 'number' },
  { key: 'mixed_ratio', label: 'mixed 写占比(%)', type: 'number' },
  { key: 'report_interval', label: '上报间隔(s)', type: 'number' },
]
