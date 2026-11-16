<template>
  <section class="page" data-module="dag">
    <header class="page-head">
      <div>
        <h2>任务编排管理</h2>
        <p class="page-desc">编排ETL任务的有向无环图，定义节点间依赖关系和并发执行策略。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记编排图</button>
        <button class="btn" type="button" @click="exportRows">导出任务编排清单</button>
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
          <th>生命周期状态</th>
          <th>发布状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row.status ?? '—' }}</td>
          <td>
            <span>{{ row['发布状态'] ?? '未发布' }}</span>
            <span v-if="row['发布版本']"> / v{{ row['发布版本'] }}</span>
          </td>
          <td class="row-actions">
            <button
              v-for="action in availableActions(row)"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button class="link" type="button" @click="openDetail(row)">发布详情</button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 3" class="empty-state">暂无任务编排数据，可先登记编排图</td>
        </tr>
      </tbody>
    </table>

    <section v-if="detail" class="detail-panel">
      <header class="detail-head">
        <h3>发布详情 · {{ detail.entry['图编号'] }}</h3>
        <button class="btn ghost" type="button" @click="detail = null">收起</button>
      </header>
      <dl class="detail-grid">
        <div><dt>生命周期</dt><dd>{{ detail.entry.status }}</dd></div>
        <div><dt>发布状态</dt><dd>{{ detail.publish_status }}</dd></div>
        <div><dt>发布版本</dt><dd>{{ detail.topology?.['发布版本'] ?? '—' }}</dd></div>
        <div><dt>来源</dt><dd>{{ detail.topology?.['来源'] ?? '—' }}</dd></div>
        <div><dt>来源评审记录</dt><dd>{{ detail.topology?.['来源评审记录'] ?? '—' }}</dd></div>
        <div><dt>失败传播策略</dt><dd>{{ detail.topology?.['失败传播策略'] ?? entry(detail)['失败传播策略'] ?? '—' }}</dd></div>
        <div><dt>调度优先级</dt><dd>{{ detail.topology?.['调度优先级'] ?? entry(detail)['调度优先级'] ?? '—' }}</dd></div>
        <div><dt>拓扑指纹</dt><dd class="mono">{{ shortHash(detail.topology?.['拓扑指纹']) }}</dd></div>
      </dl>
      <div class="detail-cols">
        <div>
          <h4>已发布拓扑</h4>
          <p v-if="!detail.topology" class="empty-state">尚无已发布拓扑，依赖解析清单不会消费该编排图。</p>
          <template v-else>
            <p class="mono">{{ detail.topology['拓扑结构'] }}</p>
            <p>节点：{{ nodeAliases(detail.topology).join('、') || '—' }}</p>
            <p>依赖边：{{ edgeTexts(detail.topology).join('；') || '—' }}</p>
          </template>
        </div>
        <div>
          <h4>评审记录</h4>
          <table class="data-table">
            <thead><tr><th>记录</th><th>版本</th><th>状态</th><th>失败传播</th><th>优先级</th></tr></thead>
            <tbody>
              <tr v-for="review in detail.reviews" :key="String(review['评审记录编号'])">
                <td>{{ review['评审记录编号'] }}</td>
                <td>v{{ review['评审版本'] }}</td>
                <td>{{ review['评审状态'] }}</td>
                <td>{{ review['失败传播策略'] ?? '—' }}</td>
                <td>{{ review['调度优先级'] ?? '—' }}</td>
              </tr>
              <tr v-if="!detail.reviews.length"><td colspan="5" class="empty-state">暂无评审记录</td></tr>
            </tbody>
          </table>
        </div>
      </div>
      <div>
        <h4>依赖解析投影</h4>
        <table class="data-table">
          <thead><tr><th>关系编号</th><th>上游</th><th>下游</th><th>状态</th><th>发布状态</th><th>拓扑可用</th></tr></thead>
          <tbody>
            <tr v-for="item in detail.projection" :key="String(item.id)">
              <td>{{ item['关系编号'] }}</td>
              <td>{{ item['上游作业'] }}</td>
              <td>{{ item['下游作业'] }}</td>
              <td>{{ item.status }}</td>
              <td>{{ item['发布状态'] }}</td>
              <td>{{ item['拓扑可用'] ? '是' : '否' }}</td>
            </tr>
            <tr v-if="!detail.projection.length"><td colspan="6" class="empty-state">依赖解析清单暂无该编排图的投影</td></tr>
          </tbody>
        </table>
      </div>
    </section>

    <footer class="page-foot">
      <span>共 {{ total }} 条任务编排记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, any>

interface PublishDetail {
  entry: Row
  publish_status: string
  topology: Row | null
  reviews: Row[]
  projection: Row[]
}

const ENDPOINT = '/api/dag'
const columns = ["图编号", "图名称", "拓扑结构", "起始节点数", "最大并行度", "失败传播策略", "调度优先级", "生效时间范围"]
const statuses = ["草稿", "编排评审中", "已发布", "已停用"]
const statLabels = ["编排图总数", "已发布编排", "评审中编排"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const detail = ref<PublishDetail | null>(null)
const filterFields = columns.slice(0, 3)

const stats = computed(() => {
  const values = [
    total.value,
    rows.value.filter((row) => row.status === '已发布').length,
    rows.value.filter((row) => row.status === '编排评审中').length,
  ]
  return statLabels.map((label, index) => ({ label, value: values[index] }))
})

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '编排图登记入口尚未接入审批流'
}

function entry(value: PublishDetail): Row {
  return value.entry
}

function shortHash(value?: string | null) {
  return value ? `${value.slice(0, 12)}…` : '—'
}

function nodeAliases(topology: Row): string[] {
  return (topology['节点'] ?? []).map((node: Row) => String(node['别名'] ?? ''))
}

function edgeTexts(topology: Row): string[] {
  return (topology['依赖边'] ?? []).map((edge: Row) => `${edge['上游']} → ${edge['下游']}`)
}

function availableActions(row: Row): string[] {
  switch (row.status) {
    case '草稿':
      return ['提交编排评审']
    case '编排评审中':
      return ['提交编排评审', '发布编排']
    case '已发布':
      return ['停用编排']
    case '已停用':
      return ['恢复编排']
    default:
      return []
  }
}

async function openDetail(row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/publish`)
    if (!response.ok) {
      throw new Error('发布详情读取失败')
    }
    detail.value = (await response.json()) as PublishDetail
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '发布详情读取失败'
  }
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
      throw new Error(payload.message || '任务编排动作未生效，请稍后重试')
    }
    await reload()
    if (detail.value?.entry.id === row.id) {
      await openDetail(row)
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '任务编排操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('编排图列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '任务编排列表读取失败'
  }
}

onMounted(reload)
</script>
