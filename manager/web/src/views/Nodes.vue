<template>
  <el-card class="page-card" shadow="never">
    <template #header>
      <div class="card-header">
        <span>节点列表</span>
        <el-button type="primary" :icon="Plus" @click="openDialog">新增节点</el-button>
      </div>
    </template>

    <el-table :data="nodes" v-loading="loading" stripe>
      <el-table-column prop="name" label="名称" min-width="110" />
      <el-table-column label="标签" min-width="200">
        <template #default="{ row }">
          <div class="tag-cell">
            <el-tag
              v-for="tag in row.tags"
              :key="tag"
              size="small"
              effect="plain"
              closable
              class="tag-chip"
              @close="removeTag(row, tag)"
            >
              <span class="tag-text" :title="`点击修改标签 ${tag}`" @click="renameTag(row, tag)">{{ tag }}</span>
            </el-tag>
            <el-tag size="small" effect="plain" class="tag-add" title="添加标签" @click="addTag(row)">+ 标签</el-tag>
          </div>
        </template>
      </el-table-column>
      <el-table-column label="部署" width="100">
        <template #default="{ row }">
          <el-tooltip v-if="row.deploy" :content="deployTip(row.deploy)" placement="top">
            <el-tag size="small" :type="row.deploy.consistent ? 'success' : 'warning'">已部署</el-tag>
          </el-tooltip>
          <el-tag v-else size="small" type="info" effect="plain">未部署</el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="ip" label="地址" min-width="130" />
      <el-table-column prop="user" label="用户" width="90" />
      <el-table-column prop="ssh_port" label="SSH 端口" width="100" />
      <el-table-column prop="api_port" label="API 端口" width="100" />
      <el-table-column prop="workdir" label="工作目录" min-width="170" show-overflow-tooltip />
      <el-table-column label="操作" width="96" fixed="right">
        <template #default="{ row }">
          <el-button size="small" type="danger" plain :icon="Delete" @click="remove(row)" />
        </template>
      </el-table-column>
      <template #empty>
        <el-empty description="暂无节点，点击右上角「新增节点」" :image-size="72" />
      </template>
    </el-table>
  </el-card>

  <el-dialog v-model="dialogVisible" title="新增节点" width="540px" destroy-on-close>
    <el-form :model="form" label-width="110px" @submit.prevent>
      <el-form-item label="名称" required>
        <el-input v-model="form.name" placeholder="如 node-a" />
      </el-form-item>
      <el-form-item label="标签">
        <el-select
          v-model="form.tags"
          multiple
          filterable
          allow-create
          default-first-option
          placeholder="输入标签后回车添加，可多个"
          class="full"
        >
          <el-option v-for="tag in existingTags" :key="tag" :value="tag" :label="tag" />
        </el-select>
      </el-form-item>
      <el-form-item label="IP 地址" required>
        <el-input v-model="form.ip" placeholder="10.0.0.1" />
      </el-form-item>
      <el-form-item label="SSH 用户">
        <el-input v-model="form.user" />
      </el-form-item>
      <el-form-item label="SSH 密码">
        <el-input v-model="form.password" type="password" show-password placeholder="留空则使用 SSH key" />
      </el-form-item>
      <el-form-item label="SSH 端口">
        <el-input-number v-model="form.ssh_port" :min="1" :max="65535" />
      </el-form-item>
      <el-form-item label="Worker API 端口">
        <el-input-number v-model="form.api_port" :min="1" :max="65535" />
      </el-form-item>
      <el-form-item label="工作目录">
        <el-input v-model="form.workdir" />
      </el-form-item>
      <el-form-item label="二进制路径">
        <el-input v-model="form.binary" />
      </el-form-item>
    </el-form>
    <template #footer>
      <el-button @click="dialogVisible = false">取消</el-button>
      <el-button type="primary" :loading="saving" @click="save">保存</el-button>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Delete, Plus } from '@element-plus/icons-vue'

import { api, collectTags, errMsg, type Node, type NodeDeployStatus, type NodeInput } from '@/api'

const nodes = ref<Node[]>([])
const loading = ref(false)
const saving = ref(false)
const dialogVisible = ref(false)

const existingTags = computed(() => collectTags(nodes.value))

const deployTip = (deploy: NodeDeployStatus): string =>
  `部署于 ${deploy.deployed_at}\n${deploy.destination}\nURMA: ${deploy.versions.join(', ') || '—'}\n一致性: ${deploy.consistent ? '一致' : '不一致'}\nworker: ${deploy.worker_state}`

const form = reactive<NodeInput>({
  name: '',
  ip: '',
  user: 'root',
  password: '',
  ssh_port: 22,
  api_port: 18082,
  workdir: '/opt/kv-bench',
  binary: '/opt/kv-bench/build/kv-bench',
  tags: [],
})

async function load() {
  loading.value = true
  try {
    nodes.value = await api.listNodes()
  } catch (error) {
    ElMessage.error(errMsg(error))
  } finally {
    loading.value = false
  }
}

function openDialog() {
  Object.assign(form, {
    name: '',
    ip: '',
    user: 'root',
    password: '',
    ssh_port: 22,
    api_port: 18082,
    workdir: '/opt/kv-bench',
    binary: '/opt/kv-bench/build/kv-bench',
    tags: [],
  })
  dialogVisible.value = true
}

async function save() {
  if (!form.name.trim() || !form.ip.trim()) {
    ElMessage.warning('名称与 IP 地址必填')
    return
  }
  saving.value = true
  try {
    await api.saveNode({ ...form })
    ElMessage.success(`节点 ${form.name} 已保存`)
    dialogVisible.value = false
    await load()
  } catch (error) {
    ElMessage.error(errMsg(error))
  } finally {
    saving.value = false
  }
}

async function remove(node: Node) {
  try {
    await ElMessageBox.confirm(`确认删除节点 ${node.name}（${node.ip}）？`, '删除节点', {
      type: 'warning',
      confirmButtonText: '删除',
      cancelButtonText: '取消',
    })
  } catch {
    return
  }
  try {
    await api.deleteNode(node.name)
    ElMessage.success('已删除')
    await load()
  } catch (error) {
    ElMessage.error(errMsg(error))
  }
}

// ---- 标签行内编辑（添加 / 修改 / 删除，均走 PATCH，保留密码等其余字段）----

async function addTag(node: Node) {
  const { value } = await ElMessageBox.prompt(`为节点 ${node.name} 添加标签`, '添加标签', {
    confirmButtonText: '添加',
    cancelButtonText: '取消',
    inputPlaceholder: '输入标签名',
  }).catch(() => ({ value: '' }))
  const tag = (value ?? '').trim()
  if (!tag) return
  if (node.tags.includes(tag)) {
    ElMessage.warning(`标签「${tag}」已存在`)
    return
  }
  await updateTags(node, [...node.tags, tag])
}

async function renameTag(node: Node, oldTag: string) {
  const { value } = await ElMessageBox.prompt(`将节点 ${node.name} 的标签「${oldTag}」改为：`, '修改标签', {
    confirmButtonText: '保存',
    cancelButtonText: '取消',
    inputValue: oldTag,
  }).catch(() => ({ value: '' }))
  const tag = (value ?? '').trim()
  if (!tag || tag === oldTag) return
  await updateTags(
    node,
    node.tags.map((item) => (item === oldTag ? tag : item)),
  )
}

async function removeTag(node: Node, tag: string) {
  await updateTags(
    node,
    node.tags.filter((item) => item !== tag),
  )
}

async function updateTags(node: Node, tags: string[]) {
  try {
    await api.patchNodeTags(node.name, tags)
    ElMessage.success(`已更新 ${node.name} 的标签`)
    await load()
  } catch (error) {
    ElMessage.error(errMsg(error))
  }
}

onMounted(load)
</script>

<style scoped>
.tag-cell {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 4px;
}

.tag-chip {
  margin: 1px 0;
}

.tag-text {
  cursor: pointer;
  display: inline-block;
  line-height: 1.6;
}

.tag-add {
  cursor: pointer;
  border-style: dashed;
  margin: 1px 0;
  color: #64748b;
}

.tag-add:hover {
  color: var(--el-color-primary);
  border-color: var(--el-color-primary);
}

.full {
  width: 100%;
}
</style>
