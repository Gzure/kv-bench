<template>
  <el-card class="page-card" shadow="never">
    <template #header>
      <div class="card-header">
        <span>部署与启动 worker</span>
        <el-button :icon="Refresh" circle title="刷新节点" @click="loadNodes" />
      </div>
    </template>

    <el-form :model="form" label-width="150px" class="deploy-form">
      <el-form-item label="Artifact 路径" required>
        <el-input v-model="form.artifact" placeholder="build/kv-bench" />
      </el-form-item>
      <el-form-item label="目标路径" required>
        <el-input v-model="form.destination" placeholder="/opt/kv-bench/build/kv-bench" />
      </el-form-item>
      <el-form-item label="源码目录" required>
        <el-input v-model="form.source_dir" placeholder="/opt/kv-bench" />
      </el-form-item>
      <el-form-item label="UMDK 根目录" required>
        <el-input v-model="form.umdk_root" placeholder="/opt/umdk" />
      </el-form-item>
      <el-form-item label="按标签筛选">
        <el-select v-model="tagFilter" multiple clearable placeholder="选择标签后仅显示匹配节点（任一匹配）" class="full">
          <el-option v-for="tag in allTags" :key="tag" :value="tag" :label="tag" />
        </el-select>
      </el-form-item>
      <el-form-item label="目标节点">
        <div class="node-picker">
          <el-checkbox-group v-model="selected">
            <el-checkbox v-for="node in filteredNodes" :key="node.name" :value="node.name">
              {{ node.name }}（{{ node.ip }}）
              <el-tag v-for="tag in node.tags" :key="tag" size="small" effect="plain" class="chip">{{ tag }}</el-tag>
            </el-checkbox>
          </el-checkbox-group>
          <div v-if="filteredNodes.length" class="picker-actions">
            <el-button size="small" text type="primary" @click="selectAll">全选</el-button>
            <el-button size="small" text @click="clearSelected">清空</el-button>
            <span class="muted">已选 {{ selected.length }} / {{ filteredNodes.length }}</span>
          </div>
          <el-empty v-if="!filteredNodes.length" description="没有匹配的节点，请调整标签筛选或在「节点管理」中添加" :image-size="60" />
        </div>
      </el-form-item>
      <el-form-item>
        <el-button
          type="primary"
          :icon="Upload"
          :loading="deploying"
          :disabled="!selected.length"
          @click="deploy"
        >
          部署并启动 worker
        </el-button>
      </el-form-item>
    </el-form>

    <template v-if="result">
      <el-divider content-position="left">部署结果</el-divider>
      <el-alert
        :title="result.consistent
          ? '所有节点 URMA 版本一致，无需重新编译'
          : '存在版本不一致节点，已对其单独编译（cmake + build）'"
        :type="result.consistent ? 'success' : 'warning'"
        :closable="false"
        show-icon
        class="result-alert"
      />
      <el-table :data="versionRows" border size="small">
        <el-table-column prop="node" label="节点" width="150" />
        <el-table-column prop="packages" label="URMA 包">
          <template #default="{ row }">
            <el-tag v-for="pkg in row.packages" :key="pkg" size="small" class="pkg-tag" effect="plain">
              {{ pkg }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="状态" width="130">
          <template #default="{ row }">
            <el-tag :type="row.consistent ? 'success' : 'warning'" size="small">
              {{ row.consistent ? '一致' : '已按需编译' }}
            </el-tag>
          </template>
        </el-table-column>
      </el-table>
    </template>
  </el-card>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { Refresh, Upload } from '@element-plus/icons-vue'

import { api, collectTags, errMsg, matchesTags, type Node, type VersionCheck } from '@/api'

const nodes = ref<Node[]>([])
const selected = ref<string[]>([])
const tagFilter = ref<string[]>([])
const deploying = ref(false)
const result = ref<VersionCheck | null>(null)

const form = reactive({
  artifact: 'build/kv-bench',
  destination: '/opt/kv-bench/build/kv-bench',
  source_dir: '/opt/kv-bench',
  umdk_root: '/opt/umdk',
})

const allTags = computed(() => collectTags(nodes.value))
const filteredNodes = computed(() => nodes.value.filter((node) => matchesTags(node, tagFilter.value)))

// 标签筛选变化时，剔除已被过滤掉的选中节点。
watch(tagFilter, () => {
  const visible = new Set(filteredNodes.value.map((node) => node.name))
  selected.value = selected.value.filter((name) => visible.has(name))
})

const versionRows = computed(() => {
  if (!result.value) return []
  return Object.entries(result.value.versions).map(([node, packages]) => ({
    node,
    packages,
    consistent: result.value!.consistent || packages.join(',') === result.value!.reference.join(','),
  }))
})

async function loadNodes() {
  try {
    nodes.value = await api.listNodes()
    selected.value = selected.value.filter((name) => nodes.value.some((node) => node.name === name))
  } catch (error) {
    ElMessage.error(errMsg(error))
  }
}

function selectAll() {
  selected.value = filteredNodes.value.map((node) => node.name)
}

function clearSelected() {
  selected.value = []
}

async function deploy() {
  if (!selected.value.length) {
    ElMessage.warning('请先选择目标节点')
    return
  }
  const payloadNodes = nodes.value
    .filter((node) => selected.value.includes(node.name))
    .map((node) => ({ ...node, password: '' }))
  deploying.value = true
  try {
    result.value = await api.deploy({
      nodes: payloadNodes,
      artifact: form.artifact,
      destination: form.destination,
      source_dir: form.source_dir,
      umdk_root: form.umdk_root,
    })
    ElMessage.success('部署完成，worker 已启动')
  } catch (error) {
    ElMessage.error(errMsg(error))
  } finally {
    deploying.value = false
  }
}

onMounted(loadNodes)
</script>

<style scoped>
.deploy-form {
  max-width: 760px;
}

.muted {
  color: #64748b;
  font-size: 13px;
  margin-left: 8px;
}

.result-alert {
  margin-bottom: 14px;
}

.pkg-tag {
  margin-right: 6px;
}

.node-picker {
  width: 100%;
}

.node-picker .el-checkbox {
  margin-right: 16px;
  margin-bottom: 6px;
}

.chip {
  margin-left: 6px;
}

.picker-actions {
  margin-top: 6px;
  display: flex;
  align-items: center;
}
</style>
