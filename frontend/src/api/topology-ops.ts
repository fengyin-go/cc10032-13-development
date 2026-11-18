/** 拓扑部署 ↔ 吞吐降级联动接口封装（含序号13批量处理）。 */
import { request } from './client'

export type NodeState = {
  node: string
  module: string | null
  ref_status: string | null
  ready: boolean
}

export type RuntimeSnapshot = {
  runtime_id: string
  topology_name: string
  topology_id: number
  version: string
  chain: string[]
  node_states: NodeState[]
  status: string
  is_current: boolean
  group_id: string | null
  item_version: string | null
  state_entry_id: number | null
  created_at: number
}

export type DeployResult = {
  ok: boolean
  published?: boolean
  error?: string
  topology?: string
  version?: string
  runtime_id?: string
  state_entry_id?: number
  nodes?: NodeState[]
  message?: string
}

export type DegradedNode = {
  node: string
  throttle_limit: number
  original_qps: number
  mode: string
  degrade_id: string
  version: string
}

export type TopologyRuntimeData = {
  topology: string
  topology_id: number
  topology_status: string
  current: RuntimeSnapshot | null
  degraded_nodes: DegradedNode[]
  history: RuntimeSnapshot[]
}

export type DegradationEntry = {
  degrade_id: string
  topology_name: string
  topology_version: string
  runtime_id: string
  node: string
  source: string
  mode: string
  original_qps: number
  throttle_limit: number
  reason: string
  status: string
  group_id: string | null
  created_at: number
}

export type BackpressureResult = {
  ok: boolean
  triggered: boolean
  error?: string
  topology?: string
  version?: string
  mode?: string
  throttle_limit?: number
  affected_nodes?: string[]
  entries?: DegradationEntry[]
  message?: string
}

export type BatchItem = {
  topology: string
  item_version: string
  ok: boolean
  version: string | null
  runtime_id: string | null
  state_entry_id: number | null
  error: string | null
}

export type BatchGroup = {
  group_id: string
  idempotency_key: string
  group_version: string
  topology_names: string[]
  mode: string
  status: string
  items: BatchItem[]
  rolled_back: boolean
  error: string | null
  initiator: string
  created_at: number
  finished_at: number | null
  success_count: number
  fail_count: number
}

export type BatchSubmitResult = {
  ok: boolean
  deduplicated?: boolean
  rolled_back?: boolean
  error?: string
  group?: BatchGroup
  message?: string
}

export type VersionVerify = {
  topology: string
  version: string
  runtime: RuntimeSnapshot | null
  degradations: DegradationEntry[]
  matched: boolean
}

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const response = await request(path, {
    method: 'POST',
    body: JSON.stringify(body),
  })
  return (await response.json()) as T
}

async function getJson<T>(path: string): Promise<T> {
  const response = await request(path)
  if (!response.ok) throw new Error(`接口返回 ${response.status}`)
  return (await response.json()) as T
}

export function deployTopology(id: number): Promise<DeployResult> {
  return postJson(`/api/task_topology/${id}/deploy`, {})
}

export function getTopologyRuntime(id: number): Promise<TopologyRuntimeData> {
  return getJson(`/api/task_topology/${id}/runtime`)
}

export function listTopologyRuntime(): Promise<{ items: RuntimeSnapshot[]; total: number }> {
  return getJson('/api/topology-runtime')
}

export function triggerBackpressure(
  topology: string,
  currentQps: number | null,
  mode: string,
): Promise<BackpressureResult> {
  return postJson('/api/throughput_mgr/backpressure', {
    topology,
    current_qps: currentQps,
    mode,
  })
}

export function listDegradations(
  topology?: string,
  version?: string,
): Promise<{ items: DegradationEntry[]; total: number }> {
  const params = new URLSearchParams()
  if (topology) params.set('topology', topology)
  if (version) params.set('version', version)
  const query = params.toString()
  return getJson(`/api/throughput_mgr/degradations${query ? `?${query}` : ''}`)
}

export function submitTopologyBatch(
  topologies: string[],
  idempotencyKey: string | null,
  mode: string,
): Promise<BatchSubmitResult> {
  return postJson('/api/topology-batch', {
    topologies,
    idempotency_key: idempotencyKey,
    mode,
  })
}

export function listTopologyBatches(): Promise<{ items: BatchGroup[]; total: number }> {
  return getJson('/api/topology-batch')
}

export function getTopologyBatch(groupId: string): Promise<BatchGroup> {
  return getJson(`/api/topology-batch/${groupId}`)
}

export function verifyTopologyVersion(topology: string, version: string): Promise<VersionVerify> {
  return getJson(
    `/api/topology-batch-verify/${encodeURIComponent(topology)}/${encodeURIComponent(version)}`,
  )
}
