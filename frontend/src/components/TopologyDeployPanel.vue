<template>
  <section class="ops-panel" data-testid="topology-deploy-panel">
    <h3 class="ops-title">算子链部署与拓扑运行数据</h3>
    <p class="ops-desc">
      读取拓扑配置部署算子链：节点全部就绪后发布运行版本，并联动写入状态存储；同一拓扑重复部署只保留一个当前运行快照。
    </p>

    <table class="data-table">
      <thead>
        <tr>
          <th>拓扑名称</th>
          <th>算子链</th>
          <th>当前版本</th>
          <th>降级节点</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`deploy-row-${row.id}`">
          <td>{{ row['拓扑名称'] }}</td>
          <td>{{ row['算子链'] }}</td>
          <td>
            <span v-if="runtimeOf(row).current" class="tag ok">
              {{ runtimeOf(row).current?.version }}
            </span>
            <span v-else class="tag muted">未发布</span>
          </td>
          <td>
            <template v-if="runtimeOf(row).degraded.length">
              <span v-for="d in runtimeOf(row).degraded" :key="d.degrade_id" class="tag warn">
                {{ d.node }}·{{ d.throttle_limit }}
              </span>
            </template>
            <span v-else class="tag muted">无</span>
          </td>
          <td class="row-actions">
            <button
              class="link"
              type="button"
              :data-testid="`deploy-${row.id}`"
              :disabled="deployingId === Number(row.id)"
              @click="deploy(row)"
            >
              {{ deployingId === Number(row.id) ? '部署中…' : '部署算子链' }}
            </button>
            <button class="link" type="button" :data-testid="`runtime-${row.id}`" @click="inspect(row)">
              运行数据
            </button>
          </td>
        </tr>
      </tbody>
    </table>

    <p v-if="message" class="ops-message" :class="{ error: !lastOk }" data-testid="deploy-message">
      {{ message }}
    </p>

    <div v-if="detail" class="ops-detail" data-testid="runtime-detail">
      <h4>拓扑「{{ detail.topology }}」运行数据</h4>
      <div v-if="detail.current" class="ops-grid">
        <div>
          <span class="ops-label">当前版本</span>
          <strong>{{ detail.current.version }}（{{ detail.current.runtime_id }}）</strong>
        </div>
        <div>
          <span class="ops-label">状态存储联动</span>
          <strong>state_store #{{ detail.current.state_entry_id }}</strong>
        </div>
        <div>
          <span class="ops-label">来源批量组</span>
          <strong>{{ detail.current.group_id ?? '手工部署' }}</strong>
        </div>
      </div>
      <p v-else class="ops-message">尚未发布运行版本。</p>

      <table v-if="detail.current" class="data-table">
        <thead>
          <tr><th>节点</th><th>归属模块</th><th>引用状态</th><th>就绪</th><th>降级</th></tr>
        </thead>
        <tbody>
          <tr v-for="ns in detail.current.node_states" :key="ns.node">
            <td>{{ ns.node }}</td>
            <td>{{ ns.module ?? '—' }}</td>
            <td>{{ ns.ref_status ?? '未找到' }}</td>
            <td><span class="tag" :class="ns.ready ? 'ok' : 'bad'">{{ ns.ready ? '就绪' : '未就绪' }}</span></td>
            <td>
              <span v-if="degradedNodeNames.has(ns.node)" class="tag warn">已降级</span>
              <span v-else class="tag muted">正常</span>
            </td>
          </tr>
        </tbody>
      </table>

      <p v-if="detail.history.length" class="ops-history">
        历史版本：
        <span
          v-for="snap in detail.history"
          :key="snap.runtime_id"
          class="tag"
          :class="snap.is_current ? 'ok' : 'muted'"
        >
          {{ snap.version }}·{{ snap.status }}
        </span>
      </p>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  deployTopology,
  getTopologyRuntime,
  type TopologyRuntimeData,
} from '@/api/topology-ops'
import type { EntryRow } from '@/data/types'

const props = defineProps<{ rows: EntryRow[] }>()
const emit = defineEmits<{ (e: 'changed'): void }>()

const deployingId = ref<number | null>(null)
const message = ref('')
const lastOk = ref(true)
const detail = ref<TopologyRuntimeData | null>(null)
const runtimeMap = ref<Map<number, TopologyRuntimeData>>(new Map())

const degradedNodeNames = computed(
  () => new Set((detail.value?.degraded_nodes ?? []).map((d) => d.node)),
)

function runtimeOf(row: EntryRow): { current: TopologyRuntimeData['current']; degraded: TopologyRuntimeData['degraded_nodes'] } {
  const data = runtimeMap.value.get(Number(row.id))
  return { current: data?.current ?? null, degraded: data?.degraded_nodes ?? [] }
}

async function refreshRuntime(row: EntryRow) {
  try {
    const data = await getTopologyRuntime(Number(row.id))
    runtimeMap.value.set(Number(row.id), data)
    runtimeMap.value = new Map(runtimeMap.value)
    return data
  } catch {
    return null
  }
}

async function refreshAll() {
  await Promise.all(props.rows.map((row) => refreshRuntime(row)))
}

async function deploy(row: EntryRow) {
  deployingId.value = Number(row.id)
  message.value = ''
  try {
    const result = await deployTopology(Number(row.id))
    lastOk.value = result.ok
    message.value = result.ok
      ? String(result.message ?? '部署成功')
      : `部署未发布：${result.error ?? '未知原因'}`
    const data = await refreshRuntime(row)
    if (data) detail.value = data
    emit('changed')
  } catch (error) {
    lastOk.value = false
    message.value = error instanceof Error ? error.message : '部署失败'
  } finally {
    deployingId.value = null
  }
}

async function inspect(row: EntryRow) {
  const data = await refreshRuntime(row)
  if (data) {
    detail.value = data
    message.value = ''
  }
}

onMounted(refreshAll)
</script>

<style scoped>
.ops-panel { margin-top: 16px; background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 14px; }
.ops-title { margin: 0 0 4px; font-size: 15px; }
.ops-desc { margin: 0 0 12px; color: var(--muted); font-size: 12px; }
.ops-message { margin: 10px 0 0; font-size: 13px; color: #027a48; }
.ops-message.error { color: #b42318; }
.ops-detail { margin-top: 14px; border-top: 1px dashed var(--border); padding-top: 12px; }
.ops-detail h4 { margin: 0 0 10px; font-size: 14px; }
.ops-grid { display: flex; gap: 24px; margin-bottom: 10px; flex-wrap: wrap; }
.ops-label { display: block; font-size: 12px; color: var(--muted); }
.ops-history { margin-top: 10px; font-size: 12px; color: var(--muted); }
.tag { display: inline-block; padding: 1px 8px; border-radius: 10px; font-size: 12px; margin: 1px 2px; background: #eef2f6; color: #475467; }
.tag.ok { background: #dcfae6; color: #027a48; }
.tag.warn { background: #fef0c7; color: #b54708; }
.tag.bad { background: #fee4e2; color: #b42318; }
.tag.muted { background: #f2f4f7; color: #98a2b3; }
.link:disabled { color: var(--muted); cursor: default; }
</style>
