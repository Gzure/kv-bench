import { createRouter, createWebHashHistory } from 'vue-router'

import Deploy from '@/views/Deploy.vue'
import Nodes from '@/views/Nodes.vue'
import Tasks from '@/views/Tasks.vue'

// hash 路由：静态托管下无需服务端 SPA fallback 配置。
const router = createRouter({
  history: createWebHashHistory(),
  routes: [
    { path: '/', redirect: '/nodes' },
    { path: '/nodes', name: 'nodes', component: Nodes, meta: { title: '节点管理' } },
    { path: '/deploy', name: 'deploy', component: Deploy, meta: { title: '部署' } },
    { path: '/tasks', name: 'tasks', component: Tasks, meta: { title: '任务' } },
  ],
})

export default router
