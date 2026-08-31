<template>
  <el-container class="layout">
    <el-aside width="224px" class="sidebar">
      <div class="brand">
        <div class="brand-logo">K</div>
        <div class="brand-text">
          <div class="brand-title">kv-bench</div>
          <div class="brand-sub">URMA Manager</div>
        </div>
      </div>
      <el-menu
        :default-active="activeMenu"
        router
        class="menu"
        background-color="transparent"
        text-color="#b6c2d9"
        active-text-color="#ffffff"
      >
        <el-menu-item index="/nodes">
          <el-icon><Monitor /></el-icon>
          <span>节点管理</span>
        </el-menu-item>
        <el-menu-item index="/deploy">
          <el-icon><Upload /></el-icon>
          <span>部署</span>
        </el-menu-item>
        <el-menu-item index="/tasks">
          <el-icon><List /></el-icon>
          <span>任务</span>
        </el-menu-item>
      </el-menu>
      <div class="sidebar-foot">v1.0 · FastAPI + Vue</div>
    </el-aside>

    <el-container>
      <el-header class="header">
        <div class="header-title">{{ routeTitle }}</div>
        <el-tag size="small" type="info" effect="plain">数据面 URMA · 控制面 REST</el-tag>
      </el-header>
      <el-main class="main">
        <router-view />
      </el-main>
    </el-container>
  </el-container>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useRoute } from 'vue-router'
import { List, Monitor, Upload } from '@element-plus/icons-vue'

const route = useRoute()
const activeMenu = computed(() => route.path)
const routeTitle = computed(() => (route.meta.title as string) ?? 'kv-bench Manager')
</script>

<style>
:root {
  --el-color-primary: #2563eb;
}

html,
body,
#app {
  height: 100%;
  margin: 0;
}

body {
  background: #f5f7fa;
  font-family: system-ui, -apple-system, 'Segoe UI', 'PingFang SC', 'Microsoft YaHei', sans-serif;
}

.layout {
  height: 100%;
}

/* ---- 侧边栏 ---- */
.sidebar {
  background: #14233a;
  display: flex;
  flex-direction: column;
}

.brand {
  display: flex;
  align-items: center;
  gap: 11px;
  padding: 20px 20px 16px;
}

.brand-logo {
  width: 38px;
  height: 38px;
  border-radius: 10px;
  background: linear-gradient(135deg, #2563eb, #3b82f6);
  color: #fff;
  font-weight: 700;
  font-size: 20px;
  display: flex;
  align-items: center;
  justify-content: center;
  box-shadow: 0 4px 12px rgba(37, 99, 235, 0.35);
}

.brand-title {
  color: #fff;
  font-size: 16px;
  font-weight: 600;
  line-height: 1.25;
}

.brand-sub {
  color: #7c8db5;
  font-size: 12px;
}

.menu {
  border-right: none;
  flex: 1;
  padding-top: 6px;
}

.menu .el-menu-item {
  height: 44px;
  line-height: 44px;
  margin: 3px 10px;
  border-radius: 9px;
}

.menu .el-menu-item.is-active {
  background: #2563eb;
}

.menu .el-menu-item:not(.is-active):hover {
  background: rgba(255, 255, 255, 0.07);
}

.sidebar-foot {
  padding: 14px 20px;
  color: #52627f;
  font-size: 12px;
}

/* ---- 顶栏与内容 ---- */
.header {
  background: #fff;
  border-bottom: 1px solid #e5e7eb;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 24px;
  height: 56px;
}

.header-title {
  font-size: 16px;
  font-weight: 600;
  color: #1f2937;
}

.main {
  padding: 20px 24px;
  overflow: auto;
}

/* ---- 通用卡片/指标 ---- */
.page-card {
  border-radius: 10px;
  border: 1px solid #e5e7eb;
  box-shadow: 0 1px 3px rgba(16, 24, 40, 0.05);
}

.card-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  font-weight: 600;
}

.metric-card {
  background: #fff;
  border: 1px solid #e5e7eb;
  border-radius: 10px;
  padding: 14px 18px;
}

.metric-label {
  color: #64748b;
  font-size: 13px;
}

.metric-value {
  font-size: 24px;
  font-weight: 650;
  color: #0f172a;
  margin-top: 6px;
  font-variant-numeric: tabular-nums;
}

.metric-unit {
  font-size: 13px;
  color: #64748b;
  font-weight: 400;
  margin-left: 4px;
}

.json-view {
  background: #0f172a;
  color: #e2e8f0;
  border-radius: 8px;
  padding: 12px 14px;
  font-family: 'Cascadia Code', Consolas, Menlo, monospace;
  font-size: 12px;
  overflow: auto;
  max-height: 320px;
  white-space: pre;
}
</style>
