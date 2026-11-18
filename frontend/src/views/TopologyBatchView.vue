<template>
  <section class="page" data-testid="topology-batch-page">
    <header class="page-head">
      <div>
        <h2>批量部署结果</h2>
        <p class="page-desc">
          批量选择多条拓扑运行数据一次提交，整组结果保留在台账；并发重试只接收一组版本，事务异常回滚整组。
        </p>
      </div>
    </header>

    <!-- 批量选择 -->
    <section class="ops-panel">
      <h3 class="ops-title">批量选择拓扑运行数据</h3>
      <form class="ops-form" @submit.prevent="submitBatch">
        <label>
          <span>幂等键（留空按拓扑集合自动生成）</span>
          <input v-model="idemKey" data-testid="batch-key" placeholder="例如 release-2026-11-18" />
        </label>
        <label>
          <span>降级方式</span>
          <select v-model="batchMode" data-testid="batch-mode">
            <option value="unified">统一降级</option>
            <option value="partial">局部降级</option>
          </select>
        </label>
        <button
          class="btn primary"
          data-testid="batch-submit"
          type="submit"
          :disabled="submitting || !selected.size"
        >
          {{ submitting ? '提交中…' : `提交批量部署（已选 ${selected.size} 条）` }}
        </button>
      </form>

      <table class="data-table">
        <thead>
          <tr>
            <th>选择</th>
            <th>拓扑名称</th>
            <th>算子链</th>
            <th>当前版本</th>
            <th>拓扑状态</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="topo in topologies" :key="topo.name" :data-testid="`pick-${topo.name}`">
            <td>
              <input
                type="checkbox"
                :checked="selected.has(topo.name)"
                :data-testid="`pick-check-${topo.name}`"
                @change="toggle(topo.name)"
              />
            </td>
            <td>{{ topo.name }}</td>
            <td>{{ topo.chain }}</td>
            <td>
              <span v-if="topo.version" class="tag ok">{{ topo.version }}</span>
              <span v-else class="tag muted">未发布</span>
            </td>
            <td>{{ topo.status }}</td>
          </tr>
        </tbody>
      </table>
      <p v-if="submitMessage" class="ops-message" :class="{ error: !submitOk }" data-testid="batch-message">
        {{ submitMessage }}
      </p>
    </section>

    <!-- 整组台账 -->
    <section class="ops-panel">
      <h3 class="ops-title">整组处理台账</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>批量组</th>
            <th>组级版本</th>
            <th>幂等键</th>
            <th>成功/失败</th>
            <th>状态</th>
            <th>提交时间</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="group in batches" :key="group.group_id" :data-testid="`batch-${group.group_id}`">
            <td>{{ group.group_id }}</td>
            <td><span class="tag ok">{{ group.group_version }}</span></td>
            <td>{{ group.idempotency_key }}</td>
            <td>{{ group.success_count }} / {{ group.fail_count }}</td>
            <td>
              <span class="tag" :class="statusClass(group.status)">{{ group.status }}</span>
            </td>
            <td>{{ formatTime(group.created_at) }}</td>
            <td>
              <button
                class="link"
                type="button"
                :data-testid="`batch-detail-${group.group_id}`"
                @click="openDetail(group.group_id)"
              >
                逐条详情
              </button>
            </td>
          </tr>
          <tr v-if="!batches.length">
            <td colspan="7" class="empty-state">暂无批量处理记录</td>
          </tr>
        </tbody>
      </table>
    </section>

    <!-- 批次详情 -->
    <section v-if="detail" class="ops-panel" data-testid="batch-detail">
      <h3 class="ops-title">
        批次详情：{{ detail.group_id }}（{{ detail.group_version }}）
        <span class="tag" :class="statusClass(detail.status)">{{ detail.status }}</span>
      </h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>逐条版本</th>
            <th>拓扑</th>
            <th>结果</th>
            <th>拓扑版本</th>
            <th>运行数据</th>
            <th>失败原因</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in detail.items" :key="item.item_version">
            <td>{{ item.item_version }}</td>
            <td>{{ item.topology }}</td>
            <td>
              <span class="tag" :class="item.ok ? 'ok' : 'bad'">{{ item.ok ? '成功' : '失败' }}</span>
            </td>
            <td>{{ item.version ?? '—' }}</td>
            <td>{{ item.runtime_id ?? '—' }}</td>
            <td>{{ item.error ?? '—' }}</td>
          </tr>
        </tbody>
      </table>
      <p v-if="detail.error" class="ops-message error">事务异常：{{ detail.error }}（已回滚整组）</p>
    </section>

    <!-- 按拓扑版本核对 -->
    <section class="ops-panel">
      <h3 class="ops-title">按拓扑版本核对</h3>
      <form class="ops-form" @submit.prevent="verify">
        <label>
          <span>拓扑</span>
          <select v-model="verifyTopology" data-testid="verify-topology">
            <option value="" disabled>选择拓扑</option>
            <option v-for="name in versionedTopologies" :key="name" :value="name">{{ name }}</option>
          </select>
        </label>
        <label>
          <span>拓扑版本</span>
          <select v-model="verifyVersion" data-testid="verify-version">
            <option value="" disabled>选择版本</option>
            <option v-for="v in versionsOf(verifyTopology)" :key="v" :value="v">{{ v }}</option>
          </select>
        </label>
        <button class="btn" data-testid="verify-submit" type="submit">核对</button>
      </form>

      <div v-if="verifyResult" class="verify-result" data-testid="verify-result">
        <template v-if="verifyResult.matched">
          <p class="ops-line">
            运行数据：{{ verifyResult.runtime?.runtime_id }} ·
            {{ verifyResult.runtime?.chain.join(' → ') }} ·
            状态 {{ verifyResult.runtime?.status }} ·
            来源 {{ verifyResult.runtime?.group_id ?? '手工部署' }}
          </p>
          <table class="data-table">
            <thead>
              <tr><th>降级ID</th><th>节点</th><th>限流值</th><th>触发QPS</th><th>方式</th><th>状态</th></tr>
            </thead>
            <tbody>
              <tr v-for="entry in verifyResult.degradations" :key="entry.degrade_id">
                <td>{{ entry.degrade_id }}</td>
                <td>{{ entry.node }}</td>
                <td>{{ entry.throttle_limit }}</td>
                <td>{{ entry.original_qps }}</td>
                <td>{{ entry.mode === 'partial' ? '局部降级' : '统一降级' }}</td>
                <td>{{ entry.status }}</td>
              </tr>
              <tr v-if="!verifyResult.degradations.length">
                <td colspan="6" class="empty-state">该版本无降级记录</td>
              </tr>
            </tbody>
          </table>
        </template>
        <p v-else class="ops-message error">未找到该拓扑版本的运行数据</p>
      </div>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { listEntries } from '@/api/backend-service'
import {
  getTopologyBatch,
  listTopologyBatches,
  listTopologyRuntime,
  submitTopologyBatch,
  verifyTopologyVersion,
  type BatchGroup,
  type RuntimeSnapshot,
  type VersionVerify,
} from '@/api/topology-ops'

type TopologyOption = { name: string; chain: string; version: string | null; status: string }

const topologies = ref<TopologyOption[]>([])
const runtimeSnapshots = ref<RuntimeSnapshot[]>([])
const selected = ref<Set<string>>(new Set())
const idemKey = ref('')
const batchMode = ref('unified')
const submitting = ref(false)
const submitMessage = ref('')
const submitOk = ref(true)
const batches = ref<BatchGroup[]>([])
const detail = ref<BatchGroup | null>(null)
const verifyTopology = ref('')
const verifyVersion = ref('')
const verifyResult = ref<VersionVerify | null>(null)

const versionedTopologies = computed(() => [
  ...new Set(runtimeSnapshots.value.map((s) => s.topology_name)),
])

function versionsOf(name: string): string[] {
  return runtimeSnapshots.value
    .filter((s) => s.topology_name === name)
    .map((s) => s.version)
}

function toggle(name: string) {
  const next = new Set(selected.value)
  if (next.has(name)) next.delete(name)
  else next.add(name)
  selected.value = next
}

function statusClass(status: string): string {
  if (status === '已完成') return 'ok'
  if (status === '已回滚' || status === '全部失败') return 'bad'
  if (status === '部分失败') return 'warn'
  return 'muted'
}

function formatTime(ts: number): string {
  return new Date(ts * 1000).toLocaleString('zh-CN', { hour12: false })
}

async function reload() {
  const [topoPage, runtime, ledger] = await Promise.all([
    listEntries('task_topology', {}),
    listTopologyRuntime(),
    listTopologyBatches(),
  ])
  runtimeSnapshots.value = runtime.items
  const currentByName = new Map(
    runtime.items.filter((s) => s.is_current).map((s) => [s.topology_name, s.version]),
  )
  topologies.value = topoPage.items.map((row) => ({
    name: String(row['拓扑名称'] ?? ''),
    chain: String(row['算子链'] ?? ''),
    version: currentByName.get(String(row['拓扑名称'] ?? '')) ?? null,
    status: String(row.status ?? ''),
  }))
  batches.value = ledger.items
}

async function submitBatch() {
  submitting.value = true
  submitMessage.value = ''
  try {
    const result = await submitTopologyBatch(
      [...selected.value],
      idemKey.value.trim() || null,
      batchMode.value,
    )
    submitOk.value = result.ok
    submitMessage.value = String(result.message ?? result.error ?? '已提交')
    if (result.group) detail.value = result.group
    await reload()
  } catch (error) {
    submitOk.value = false
    submitMessage.value = error instanceof Error ? error.message : '批量提交失败'
  } finally {
    submitting.value = false
  }
}

async function openDetail(groupId: string) {
  detail.value = await getTopologyBatch(groupId)
}

async function verify() {
  if (!verifyTopology.value || !verifyVersion.value) return
  verifyResult.value = await verifyTopologyVersion(verifyTopology.value, verifyVersion.value)
}

onMounted(reload)
</script>

<style scoped>
.ops-panel { margin-bottom: 16px; background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 14px; }
.ops-title { margin: 0 0 10px; font-size: 15px; }
.ops-form { display: flex; flex-wrap: wrap; gap: 10px; align-items: flex-end; margin-bottom: 12px; }
.ops-form label { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: var(--muted); }
.ops-form input, .ops-form select { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; font-size: 13px; min-width: 200px; }
.ops-message { margin: 10px 0 0; font-size: 13px; color: #027a48; }
.ops-message.error { color: #b42318; }
.ops-line { font-size: 13px; margin: 0 0 10px; }
.verify-result { margin-top: 10px; }
.tag { display: inline-block; padding: 1px 8px; border-radius: 10px; font-size: 12px; background: #eef2f6; color: #475467; }
.tag.ok { background: #dcfae6; color: #027a48; }
.tag.warn { background: #fef0c7; color: #b54708; }
.tag.bad { background: #fee4e2; color: #b42318; }
.tag.muted { background: #f2f4f7; color: #98a2b3; }
</style>
