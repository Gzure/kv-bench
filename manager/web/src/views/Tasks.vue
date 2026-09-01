<template>
  <el-card class="page-card" shadow="never">
    <template #header>
      <div class="card-header">
        <span>任务列表 <span class="muted">（每 3s 自动刷新）</span></span>
        <el-button type="primary" :icon="Plus" @click="openCreate">创建任务</el-button>
      </div>
    </template>

    <el-table :data="tasks" v-loading="loading" stripe>
      <el-table-column prop="task_id" label="任务 ID" min-width="140" />
      <el-table-column label="状态" width="100">
        <template #default="{ row }">
          <el-tag :type="stateType(row.state)" size="small">{{ stateText(row.state) }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="拓扑" min-width="220">
        <template #default="{ row }">
          <el-tag v-for="(item, i) in row.bench_items" :key="i" size="small" effect="plain" class="topo-tag">
            {{ item.src }} → {{ item.dst }} · {{ item.type }}
          </el-tag>
          <span v-if="!row.bench_items.length" class="muted">—</span>
        </template>
      </el-table-column>
      <el-table-column label="参数" min-width="200" show-overflow-tooltip>
        <template #default="{ row }">
          {{ optionSummary(row) }}
        </template>
      </el-table-column>
      <el-table-column label="操作" width="230" fixed="right">
        <template #default="{ row }">
          <el-button size="small" type="success" plain :icon="VideoPlay" :disabled="row.state !== 'queued'" @click="start(row)">启动</el-button>
          <el-button size="small" type="warning" plain :icon="VideoPause" :disabled="row.state !== 'running'" @click="stop(row)">停止</el-button>
          <el-button size="small" :icon="DataLine" @click="showResult(row)">结果</el-button>
        </template>
      </el-table-column>
      <template #empty>
        <el-empty description="暂无任务，点击右上角「创建任务」" :image-size="72" />
      </template>
    </el-table>
  </el-card>

  <!-- 创建任务 -->
  <el-dialog v-model="createVisible" title="创建任务" width="680px" destroy-on-close>
    <el-form :model="createForm" label-width="100px">
      <el-form-item label="任务 ID">
        <el-input v-model="createForm.task_id" placeholder="留空自动生成" />
      </el-form-item>
      <el-form-item label="标签筛选">
        <el-select v-model="workerTagFilter" multiple clearable placeholder="按标签筛选 Worker（任一匹配）" class="full">
          <el-option v-for="tag in allTags" :key="tag" :value="tag" :label="tag" />
        </el-select>
      </el-form-item>
      <el-form-item label="参与 Worker" required>
        <el-select v-model="createForm.workerNames" multiple placeholder="从已保存节点中选择" class="full">
          <el-option v-for="node in filteredNodes" :key="node.name" :value="node.name" :label="`${node.name}（${node.ip}）`" />
        </el-select>
      </el-form-item>
      <el-form-item label="拓扑 (bench)">
        <div class="bench-editor">
          <div v-for="(item, index) in createForm.benchItems" :key="index" class="bench-row">
            <el-select v-model="item.src" placeholder="源" class="bench-select">
              <el-option v-for="name in createForm.workerNames" :key="name" :value="name" :label="name" />
            </el-select>
            <span class="arrow">→</span>
            <el-select v-model="item.dst" placeholder="目的" class="bench-select">
              <el-option v-for="name in createForm.workerNames" :key="name" :value="name" :label="name" />
            </el-select>
            <el-select v-model="item.type" class="bench-type">
              <el-option value="forward" label="forward" />
              <el-option value="reverse" label="reverse" />
              <el-option value="bidirectional" label="bidirectional" />
            </el-select>
            <el-button size="small" type="danger" plain :icon="Delete" circle @click="createForm.benchItems.splice(index, 1)" />
          </div>
          <el-button size="small" :icon="Plus" @click="createForm.benchItems.push({ src: '', dst: '', type: 'forward' })">
            添加拓扑项
          </el-button>
        </div>
      </el-form-item>
      <el-form-item label="打流参数">
        <div class="option-grid">
          <div v-for="field in BENCH_OPTION_FIELDS" :key="field.key" class="option-item">
            <label>{{ field.label }}</label>
            <el-select v-if="field.type === 'select'" v-model="createForm.options[field.key]" clearable class="full">
              <el-option v-for="opt in field.options ?? []" :key="opt" :value="opt" :label="opt" />
            </el-select>
            <el-input-number v-else v-model="createForm.options[field.key]" :min="0" :controls="false" class="full" placeholder="默认" />
          </div>
        </div>
      </el-form-item>
    </el-form>
    <template #footer>
      <el-button @click="createVisible = false">取消</el-button>
      <el-button type="primary" :loading="creating" @click="create">创建</el-button>
    </template>
  </el-dialog>

  <!-- 结果抽屉 -->
  <el-drawer v-model="resultVisible" :title="`任务结果 · ${currentTask?.task_id ?? ''}`" size="680px">
    <div v-if="result" class="metric-grid">
      <div class="metric-card">
        <div class="metric-label">操作数 ops</div>
        <div class="metric-value">{{ fmtInt(result.aggregate.ops) }}</div>
      </div>
      <div class="metric-card">
        <div class="metric-label">IOPS</div>
        <div class="metric-value">{{ fmtFloat(result.aggregate.iops) }}</div>
      </div>
      <div class="metric-card">
        <div class="metric-label">带宽</div>
        <div class="metric-value">{{ fmtFloat(result.aggregate.bandwidth_mb_s) }}<span class="metric-unit">MB/s</span></div>
      </div>
      <div class="metric-card">
        <div class="metric-label">错误</div>
        <div class="metric-value" :class="{ 'metric-err': result.aggregate.errors > 0 }">{{ fmtInt(result.aggregate.errors) }}</div>
      </div>
    </div>

    <el-divider v-if="result" content-position="left">各 Worker 明细</el-divider>
    <el-table v-if="result" :data="workerRows" border size="small">
      <el-table-column prop="name" label="Worker" width="110" />
      <el-table-column label="状态" width="110">
        <template #default="{ row }">
          <el-tag :type="workerStateType(row.state)" size="small">{{ row.state }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="ops" label="ops" width="110" align="right" />
      <el-table-column prop="bytes" label="bytes" width="130" align="right" />
      <el-table-column prop="errors" label="errors" width="90" align="right" />
      <el-table-column label="备注" min-width="120" show-overflow-tooltip>
        <template #default="{ row }">{{ row.error ?? '' }}</template>
      </el-table-column>
    </el-table>

    <el-divider v-if="result" content-position="left">原始 JSON</el-divider>
    <pre v-if="result" class="json-view">{{ JSON.stringify(result, null, 2) }}</pre>
    <el-empty v-else description="暂无结果（任务可能尚未完成或 worker 不可达）" :image-size="72" />

    <el-divider content-position="left">日志（保存于 runs/{{ currentTask?.task_id }}）</el-divider>
    <div class="log-toolbar">
      <el-button size="small" :icon="Refresh" :loading="logsLoading" @click="loadLogs">刷新日志</el-button>
      <span v-if="logs?.directory" class="muted">{{ logs.directory }}</span>
    </div>
    <template v-if="logs && (logWorkerNames.length || logs.manager_log)">
      <el-tabs v-model="activeLogTab">
        <el-tab-pane v-for="name in logWorkerNames" :key="name" :label="name" :name="name">
          <el-alert
            v-if="logs.workers[name]?.error"
            title="日志拉取失败（SSH tail 出错）"
            type="error"
            :closable="false"
            class="log-alert"
          />
          <pre class="json-view log-view">{{ logs.workers[name]?.content || '（无内容）' }}</pre>
        </el-tab-pane>
        <el-tab-pane v-if="logs.manager_log" label="manager 事件" name="__manager__">
          <pre class="json-view log-view">{{ logs.manager_log }}</pre>
        </el-tab-pane>
      </el-tabs>
    </template>
    <el-empty v-else-if="logs" description="暂无日志（任务可能尚未启动）" :image-size="60" />
  </el-drawer>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { DataLine, Delete, Plus, Refresh, VideoPause, VideoPlay } from '@element-plus/icons-vue'

import {
  api,
  BENCH_OPTION_FIELDS,
  collectTags,
  errMsg,
  matchesTags,
  type BenchItem,
  type Node,
  type Task,
  type TaskLogs,
  type TaskResult,
} from '@/api'

const tasks = ref<Task[]>([])
const nodes = ref<Node[]>([])
const loading = ref(false)
let timer: number | undefined

// ---- 列表 ----
const stateType = (state: string): 'info' | 'success' | 'danger' =>
  state === 'running' ? 'success' : state === 'stopped' ? 'danger' : 'info'

const stateText = (state: string): string =>
  state === 'queued' ? '排队中' : state === 'running' ? '运行中' : '已停止'

const optionSummary = (task: Task): string =>
  Object.entries(task.options)
    .map(([key, value]) => `${key}=${value}`)
    .join(', ')

async function load() {
  loading.value = true
  try {
    tasks.value = await api.listTasks()
  } catch (error) {
    ElMessage.error(errMsg(error))
  } finally {
    loading.value = false
  }
}

async function start(task: Task) {
  try {
    await api.startTask(task.task_id)
    ElMessage.success(`任务 ${task.task_id} 已启动`)
    await load()
  } catch (error) {
    ElMessage.error(errMsg(error))
  }
}

async function stop(task: Task) {
  try {
    await ElMessageBox.confirm(`确认停止任务 ${task.task_id}？`, '停止任务', {
      type: 'warning',
      confirmButtonText: '停止',
      cancelButtonText: '取消',
    })
  } catch {
    return
  }
  try {
    await api.stopTask(task.task_id)
    ElMessage.success(`任务 ${task.task_id} 已停止`)
    await load()
  } catch (error) {
    ElMessage.error(errMsg(error))
  }
}

// ---- 创建 ----
const createVisible = ref(false)
const creating = ref(false)
const workerTagFilter = ref<string[]>([])

const allTags = computed(() => collectTags(nodes.value))
const filteredNodes = computed(() => nodes.value.filter((node) => matchesTags(node, workerTagFilter.value)))

// 标签筛选变化时，剔除已被过滤掉的已选 Worker。
watch(workerTagFilter, () => {
  const visible = new Set(filteredNodes.value.map((node) => node.name))
  createForm.workerNames = createForm.workerNames.filter((name) => visible.has(name))
})

const createForm = reactive<{
  task_id: string
  workerNames: string[]
  benchItems: BenchItem[]
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  options: Record<string, any>
}>({
  task_id: '',
  workerNames: [],
  benchItems: [{ src: '', dst: '', type: 'forward' }],
  options: { op: 'write' },
})

async function openCreate() {
  try {
    nodes.value = await api.listNodes()
  } catch (error) {
    ElMessage.error(errMsg(error))
  }
  Object.assign(createForm, {
    task_id: '',
    workerNames: [],
    benchItems: [{ src: '', dst: '', type: 'forward' }],
    options: { op: 'write' },
  })
  workerTagFilter.value = []
  createVisible.value = true
}

async function create() {
  if (createForm.workerNames.length < 2) {
    ElMessage.warning('至少选择 2 个 Worker')
    return
  }
  const workerMap = new Map(nodes.value.map((node) => [node.name, node]))
  const workers = createForm.workerNames.map((name) => {
    const node = workerMap.get(name)!
    return { ...node, password: '' }
  })
  const benchItems = createForm.benchItems.filter((item) => item.src && item.dst && item.src !== item.dst)
  if (!benchItems.length) {
    ElMessage.warning('请至少添加一条合法拓扑（src ≠ dst）')
    return
  }
  const options: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(createForm.options)) {
    if (value !== undefined && value !== null && value !== '') options[key] = value
  }
  creating.value = true
  try {
    await api.createTask({
      task_id: createForm.task_id.trim() || undefined,
      workers,
      bench_items: benchItems,
      options,
    })
    ElMessage.success('任务已创建')
    createVisible.value = false
    await load()
  } catch (error) {
    ElMessage.error(errMsg(error))
  } finally {
    creating.value = false
  }
}

// ---- 结果 ----
const resultVisible = ref(false)
const currentTask = ref<Task | null>(null)
const result = ref<TaskResult | null>(null)
const logs = ref<TaskLogs | null>(null)
const logsLoading = ref(false)
const activeLogTab = ref('')

const logWorkerNames = computed(() => (logs.value ? Object.keys(logs.value.workers) : []))

const workerRows = computed(() => {
  if (!result.value) return []
  return Object.entries(result.value.workers).map(([name, data]) => ({
    name,
    state: data.state ?? 'unknown',
    ops: data.ops ?? 0,
    bytes: data.bytes ?? 0,
    errors: data.errors ?? 0,
    error: data.error,
  }))
})

const workerStateType = (state: string): 'success' | 'warning' | 'danger' | 'info' =>
  state === 'ready' ? 'success' : state === 'running' ? 'warning' : state === 'unreachable' ? 'danger' : 'info'

async function showResult(task: Task) {
  currentTask.value = task
  result.value = null
  logs.value = null
  activeLogTab.value = ''
  resultVisible.value = true
  try {
    result.value = await api.taskResult(task.task_id)
  } catch (error) {
    ElMessage.error(errMsg(error))
  }
  await loadLogs()
}

async function loadLogs() {
  if (!currentTask.value) return
  logsLoading.value = true
  try {
    logs.value = await api.taskLogs(currentTask.value.task_id)
    if (!activeLogTab.value && logWorkerNames.value.length) {
      activeLogTab.value = logWorkerNames.value[0]
    }
  } catch (error) {
    ElMessage.error(errMsg(error))
  } finally {
    logsLoading.value = false
  }
}

// ---- 工具 ----
const fmtInt = (value: number): string => new Intl.NumberFormat('zh-CN').format(Math.round(value ?? 0))
const fmtFloat = (value: number): string => (value ?? 0).toFixed(2)

onMounted(() => {
  load()
  timer = window.setInterval(load, 3000)
})

onBeforeUnmount(() => {
  if (timer !== undefined) window.clearInterval(timer)
})
</script>

<style scoped>
.muted {
  color: #94a3b8;
  font-size: 12px;
  font-weight: 400;
}

.topo-tag {
  margin: 2px 4px 2px 0;
}

.full {
  width: 100%;
}

.bench-editor {
  width: 100%;
}

.bench-row {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 8px;
}

.bench-select {
  width: 160px;
}

.bench-type {
  width: 150px;
}

.arrow {
  color: #94a3b8;
}

.option-grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 10px 16px;
  width: 100%;
}

.option-item label {
  display: block;
  color: #64748b;
  font-size: 12px;
  margin-bottom: 4px;
}

.metric-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 12px;
}

.metric-err {
  color: #dc2626;
}

.log-toolbar {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 8px;
}

.log-alert {
  margin-bottom: 8px;
}

.log-view {
  max-height: 420px;
}
</style>
