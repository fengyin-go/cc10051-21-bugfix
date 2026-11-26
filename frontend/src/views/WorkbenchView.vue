<template>
  <section class="page" :data-module="meta.key">
    <header class="page-head">
      <div>
        <h2>{{ meta.name }}</h2>
        <p class="page-desc">{{ meta.desc }}</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">
          登记{{ meta.entity }}
        </button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">
          导出{{ meta.name }}清单
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p class="status-legend">
      <span v-for="item in statusSummary" :key="item.status" class="legend-item">
        {{ item.status }}：{{ item.count }}
      </span>
    </p>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in meta.fields" :key="column">
            <span>{{ row[column] ?? '—' }}</span>
            <span
              v-if="column === meta.fields[meta.fields.length - 1] && row['回执类型']"
              class="trace-tag"
              :data-testid="`receipt-type-${row.id}`"
            >{{ row['回执类型'] }}</span>
          </td>
          <td>
            <div>{{ row.status }}</div>
            <div
              v-if="row['阻断原因']"
              class="trace-block block-reason"
              :data-testid="`block-reason-${row.id}`"
            >阻断原因：{{ row['阻断原因'] }}</div>
            <div
              v-if="row['领取凭证']"
              class="trace-block claim-voucher"
              :data-testid="`claim-voucher-${row.id}`"
            >领取凭证：{{ row['领取凭证'] }}<template v-if="row['凭证状态']">（{{ row['凭证状态'] }}）</template></div>
            <div
              v-if="row['送达依据']"
              class="trace-block delivery-proof"
              :data-testid="`delivery-proof-${row.id}`"
            >送达依据：{{ row['送达依据'] }}</div>
            <div
              v-if="row['关联回执编号']"
              class="trace-block related-receipt"
              :data-testid="`related-receipt-${row.id}`"
            >关联回执：{{ row['关联回执编号'] }}</div>
          </td>
          <td class="row-actions">
            <button
              v-for="action in meta.actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button
              v-for="action in extraActions"
              :key="action"
              class="link domain-action"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button
              v-if="meta.key === 'dist_receipt'"
              class="link domain-action"
              type="button"
              :data-testid="`open-receipt-${row.id}`"
              @click="openReceipt(row)"
            >
              详情
            </button>
            <button
              v-if="meta.key === 'golden_publish'"
              class="link domain-action"
              type="button"
              :data-testid="`detail-${row.id}`"
              @click="openDetail(row)"
            >
              详情
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="meta.fields.length + 2" class="empty-state">
            暂无{{ meta.name }}数据，可先登记{{ meta.entity }}
          </td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记{{ meta.entity }}</h3>
        <label v-for="field in meta.fields" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <div v-if="detailRow" class="modal-backdrop" data-testid="detail-modal" @click.self="closeDetail">
      <div class="modal detail-modal" data-testid="detail-panel">
        <h3 data-testid="detail-title">{{ detailTitle }}</h3>
        <p v-if="detailMessage" class="detail-message" data-testid="detail-message">{{ detailMessage }}</p>
        <dl class="detail-grid" data-testid="detail-list">
          <div v-for="item in detailEntries" :key="item.key" class="detail-item" :data-testid="`detail-field-${item.key}`">
            <dt>{{ item.key }}</dt>
            <dd :data-testid="`detail-value-${item.key}`">{{ item.value }}</dd>
          </div>
        </dl>
        <div class="page-actions">
          <button class="btn primary" data-testid="detail-close" type="button" @click="closeDetail">关闭</button>
        </div>
      </div>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条{{ meta.name }}记录</span>
      <span v-if="actionMessage" class="ok-text" data-testid="action-message">{{ actionMessage }}</span>
      <span v-if="errorMessage" class="error-text" data-testid="action-error">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'

import {
  createEntry,
  downloadEntries,
  listEntries,
  moduleMeta,
  runAction as applyAction,
} from '@/api/backend-service'
import type { EntryRow } from '@/data/types'

const props = defineProps<{ moduleKey: string }>()
const meta = computed(() => moduleMeta(props.moduleKey))
const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const actionMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = computed(() => meta.value.fields.slice(0, 3))
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const detailRow = ref<EntryRow | null>(null)
const detailMessage = ref('')

// 规范动作之外的领域动作：服务端按同一 /actions 契约分派。
const EXTRA_ACTIONS: Record<string, string[]> = {
  // 「重新办理」是重试分发的业务说法，服务端按同一资格核对处理
  dist_task: ['重新办理', '确认送达'],
  dist_receipt: ['核对回执', '打开回执'],
}
const extraActions = computed(() => EXTRA_ACTIONS[meta.value.key] ?? [])

const DETAIL_KEYS = [
  'id', 'status', '回执编号', '回执类型', '分发批次', '分发任务ID', '订阅方',
  '版本标识', '领取凭证', '凭证状态', '送达依据', '关联回执编号', '关联回执ID',
  '回滚任务ID', '回执状态', '差异说明', '阻断原因', '资格状态',
  '发布批次', '实体编码', '金记录数', '发布时间',
  '目标版本', '回滚原因', '执行状态', '影响订阅方',
]

const detailTitle = computed(() => {
  if (!detailRow.value) return '详情'
  if (meta.value.key === 'dist_receipt') return `回执详情 #${detailRow.value.id}`
  if (meta.value.key === 'golden_publish') return `金记录发布详情 #${detailRow.value.id}`
  return `详情 #${detailRow.value.id}`
})

const detailEntries = computed(() => {
  if (!detailRow.value) return []
  return DETAIL_KEYS
    .filter((key) => detailRow.value?.[key] !== undefined && detailRow.value?.[key] !== null && detailRow.value?.[key] !== '')
    .map((key) => ({ key, value: String(detailRow.value?.[key]) }))
})

const stats = computed(() => [
  { label: meta.value.metrics[0], value: total.value },
  { label: meta.value.metrics[1], value: rows.value.filter((row) => row.status === meta.value.statuses[1]).length },
  {
    label: meta.value.metrics[2],
    value: rows.value.filter((row) => row.status === meta.value.statuses[meta.value.statuses.length - 1]).length,
  },
])
const statusSummary = computed(() =>
  meta.value.statuses.map((status) => ({
    status,
    count: rows.value.filter((row) => String(row.status) === status).length,
  })),
)

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  actionMessage.value = ''
  try {
    await downloadEntries(meta.value.key, filters.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(meta.value.fields.map((field) => [field, '']))
  errorMessage.value = ''
  actionMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const result = await createEntry(meta.value.key, createValues.value)
    if (!result.ok) throw new Error(result.message)
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: EntryRow) {
  // 打开/查看回执走带资格核对的详情弹窗
  if (meta.value.key === 'dist_receipt' && action.includes('打开')) {
    await openReceipt(row)
    return
  }
  errorMessage.value = ''
  actionMessage.value = ''
  let result: Awaited<ReturnType<typeof applyAction>> | null = null
  try {
    result = await applyAction(meta.value.key, Number(row.id), action)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
    return
  }
  // 无论通过还是阻断都重新拉取，确保服务端持久化的阻断原因/凭证/状态如实呈现，
  // 回到原分发行重试时看到的是服务端最新资格结论。
  await reload()
  if (!result.ok) {
    errorMessage.value = result.message || '操作未通过资格核对'
  } else {
    actionMessage.value = result.message || '操作已完成'
  }
}

async function openReceipt(row: EntryRow) {
  errorMessage.value = ''
  actionMessage.value = ''
  try {
    const result = await applyAction('dist_receipt', Number(row.id), '打开回执')
    if (result.ok) {
      // 只有服务端核对为真实送达的回执才打开送达依据详情
      detailRow.value = (result.receipt as EntryRow | undefined)
        ?? (result.entry as EntryRow | undefined)
        ?? row
      detailMessage.value = result.message || '真实送达记录'
      await reload()
    } else {
      // 非送达记录或未经真实确认：不打开详情，只给出可追查的阻断说明
      detailRow.value = null
      detailMessage.value = ''
      errorMessage.value = result.message || '该回执不是可打开的真实送达记录'
    }
  } catch (error) {
    detailRow.value = null
    errorMessage.value = error instanceof Error ? error.message : '回执打开失败'
  }
}

function openDetail(row: EntryRow) {
  detailRow.value = row
  detailMessage.value = ''
}

function closeDetail() {
  detailRow.value = null
  detailMessage.value = ''
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries(meta.value.key, filters.value)
    rows.value = payload.items
    total.value = payload.total
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

onMounted(reload)

// SPA 左侧导航只改变 :moduleKey，组件实例会被复用，必须显式重新加载，
// 否则从分发任务跳到金记录发布再回来时页面仍停留在旧模块。
watch(
  () => props.moduleKey,
  () => {
    filters.value = {}
    showCreate.value = false
    detailRow.value = null
    detailMessage.value = ''
    errorMessage.value = ''
    actionMessage.value = ''
    void reload()
  },
)
</script>
