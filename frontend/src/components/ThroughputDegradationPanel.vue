<template>
  <section class="ops-panel" data-testid="throughput-degradation-panel">
    <h3 class="ops-title">背压触发与降级状态清单</h3>
    <p class="ops-desc">
      读取部署后的拓扑运行数据，背压触发时写入降级状态清单并保留拓扑版本；可核对受影响节点与当前限流状态。
    </p>

    <form class="ops-form" @submit.prevent="trigger">
      <label>
        <span>已部署拓扑</span>
        <select v-model="formTopology" data-testid="bp-topology">
          <option value="" disabled>选择拓扑</option>
          <option v-for="name in deployedTopologies" :key="name" :value="name">{{ name }}</option>
        </select>
      </label>
      <label>
        <span>当前QPS（留空按90%负载模拟）</span>
        <input v-model="formQps" data-testid="bp-qps" placeholder="例如 950" />
      </label>
      <label>
        <span>降级方式</span>
        <select v-model="formMode" data-testid="bp-mode">
          <option value="partial">局部降级（按已部署节点）</option>
          <option value="unified">统一降级（等整条拓扑就绪）</option>
        </select>
      </label>
      <button class="btn primary" data-testid="bp-submit" type="submit" :disabled="triggering">
        {{ triggering ? '触发中…' : '触发背压' }}
      </button>
      <button class="btn ghost" type="button" @click="reload">刷新清单</button>
    </form>

    <p v-if="message" class="ops-message" :class="{ error: !lastOk }" data-testid="bp-message">
      {{ message }}
    </p>

    <table class="data-table">
      <thead>
        <tr>
          <th>降级ID</th>
          <th>拓扑</th>
          <th>拓扑版本</th>
          <th>节点</th>
          <th>限流值</th>
          <th>触发QPS</th>
          <th>方式</th>
          <th>状态</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="entry in entries" :key="entry.degrade_id" :data-testid="`deg-${entry.degrade_id}`">
          <td>{{ entry.degrade_id }}</td>
          <td>{{ entry.topology_name }}</td>
          <td><span class="tag ok">{{ entry.topology_version }}</span></td>
          <td>{{ entry.node }}</td>
          <td>{{ entry.throttle_limit }}</td>
          <td>{{ entry.original_qps }}</td>
          <td>{{ entry.mode === 'partial' ? '局部降级' : '统一降级' }}</td>
          <td>
            <span class="tag" :class="entry.status === '生效中' ? 'warn' : 'muted'">
              {{ entry.status }}
            </span>
          </td>
        </tr>
        <tr v-if="!entries.length">
          <td colspan="8" class="empty-state">暂无降级记录，触发背压后在此核对受影响节点</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  listDegradations,
  listTopologyRuntime,
  triggerBackpressure,
  type DegradationEntry,
} from '@/api/topology-ops'

const emit = defineEmits<{ (e: 'changed'): void }>()

const entries = ref<DegradationEntry[]>([])
const deployedTopologies = ref<string[]>([])
const formTopology = ref('')
const formQps = ref('')
const formMode = ref('partial')
const triggering = ref(false)
const message = ref('')
const lastOk = ref(true)

async function reload() {
  const [deg, runtime] = await Promise.all([listDegradations(), listTopologyRuntime()])
  entries.value = [...deg.items].sort((a, b) => b.created_at - a.created_at)
  deployedTopologies.value = [
    ...new Set(runtime.items.filter((s) => s.is_current).map((s) => s.topology_name)),
  ]
}

async function trigger() {
  if (!formTopology.value) {
    lastOk.value = false
    message.value = '请先选择已部署的拓扑'
    return
  }
  triggering.value = true
  message.value = ''
  try {
    const qps = formQps.value.trim() ? Number(formQps.value) : null
    const result = await triggerBackpressure(formTopology.value, qps, formMode.value)
    lastOk.value = result.ok
    message.value = result.ok
      ? String(result.message ?? '处理完成')
      : `未触发：${result.error ?? '未知原因'}`
    await reload()
    emit('changed')
  } catch (error) {
    lastOk.value = false
    message.value = error instanceof Error ? error.message : '背压触发失败'
  } finally {
    triggering.value = false
  }
}

onMounted(reload)
</script>

<style scoped>
.ops-panel { margin-top: 16px; background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 14px; }
.ops-title { margin: 0 0 4px; font-size: 15px; }
.ops-desc { margin: 0 0 12px; color: var(--muted); font-size: 12px; }
.ops-form { display: flex; flex-wrap: wrap; gap: 10px; align-items: flex-end; margin-bottom: 12px; }
.ops-form label { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: var(--muted); }
.ops-form input, .ops-form select { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; font-size: 13px; min-width: 180px; }
.ops-message { margin: 0 0 10px; font-size: 13px; color: #027a48; }
.ops-message.error { color: #b42318; }
.tag { display: inline-block; padding: 1px 8px; border-radius: 10px; font-size: 12px; background: #eef2f6; color: #475467; }
.tag.ok { background: #dcfae6; color: #027a48; }
.tag.warn { background: #fef0c7; color: #b54708; }
.tag.muted { background: #f2f4f7; color: #98a2b3; }
</style>
