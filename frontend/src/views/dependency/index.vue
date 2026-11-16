<template>
  <section class="page" data-module="dependency">
    <header class="page-head">
      <div>
        <h2>依赖解析管理</h2>
        <p class="page-desc">管理ETL作业间的依赖关系，定义时间偏移、触发条件和等待策略。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记依赖关系</button>
        <button class="btn" type="button" @click="exportRows">导出依赖解析清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>发布状态</th>
          <th>来源编排图</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td>
            <span>{{ row['发布状态'] ?? '—' }}</span>
            <span v-if="row['来源发布版本']"> / v{{ row['来源发布版本'] }}</span>
            <span v-if="row['来源编排图ID'] && !row['拓扑可用']" class="error-text"> · 拓扑不可用</span>
          </td>
          <td>{{ row['来源图编号'] ?? '手工登记' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 3" class="empty-state">暂无依赖解析数据，可先登记依赖关系</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条依赖解析记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/dependency'
const columns = ["关系编号", "上游作业", "下游作业", "依赖类型", "偏移量分钟", "触发条件", "等待超时", "跳过标志"]
const actions = ["提交关系评审", "生效关系", "解除关系"]
const statuses = ["草稿", "关系评审中", "已生效", "已解除"]
const statLabels = ["依赖关系总数", "已生效关系", "评审中关系"]

const rows = ref<Row[]>([])
const total = ref(0)
const stats = computed(() => {
  const values = [
    total.value,
    rows.value.filter((row) => row.status === '已生效').length,
    rows.value.filter((row) => row.status === '关系评审中').length,
  ]
  return statLabels.map((label, index) => ({ label, value: values[index] }))
})
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '依赖关系登记入口尚未接入审批流'
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    const payload = await response.json().catch(() => ({}))
    if (!response.ok || !payload.ok) {
      throw new Error(payload.message || '依赖解析动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '依赖解析操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('依赖关系列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '依赖解析列表读取失败'
  }
}

onMounted(reload)
</script>
