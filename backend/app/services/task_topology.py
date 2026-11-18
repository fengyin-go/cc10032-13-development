"""任务拓扑领域服务：管理流式计算任务的DAG拓扑结构。

增强能力: 算子链校验、节点分发策略、故障转移评估、资源配额检查、关联stream_job
增强能力: 读取拓扑配置部署算子链，发布拓扑运行数据并联动状态存储；
          节点未全部就绪不发布运行版本，同一拓扑只保留一个当前运行快照。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 算子链节点可归属的模块及其名称字段
_NODE_MODULES: list[tuple[str, str]] = [
    ("stream_source", "源端名称"),
    ("calc_operator", "算子名称"),
    ("sliding_index", "指标名称"),
    ("window_config", "策略名称"),
    ("join_stream", "配置名称"),
    ("cep_rule", "序列名称"),
    ("sink_output", "配置名称"),
    ("aggregate_sink", "任务名称"),
    ("state_store", "配置名称"),
]

# 节点处于这些状态时视为未就绪
_NODE_BAD_STATUSES = {"已停止", "已停用", "已下线", "异常断连", "已过期", "已关闭", "写入异常"}


class TaskTopologyService(BaseModuleService):
    MODULE = "task_topology"
    ENTITY = "拓扑配置"
    LIST_FIELDS = ["拓扑名称", "算子链", "并行度", "数据分发", "故障转移", "资源配额", "运行状态"]
    REQUIRED_FIELDS = ["拓扑名称", "算子链", "并行度"]
    STATUS_ORDER = ["草稿", "已部署", "运行中", "已停止"]
    ACTION_RULES = {"部署拓扑": "草稿", "停止拓扑": "运行中"}
    NEGATIVE_ACTIONS = []

    # ---- 算子链部署（拓扑运行数据 ↔ 状态存储联动） ----

    @staticmethod
    def parse_chain(raw_chain: str) -> list[str]:
        """解析算子链文本为节点列表，支持 → / -> / , 分隔。"""
        normalized = str(raw_chain or "").replace("->", "→").replace(",", "→").replace("，", "→")
        return [seg.strip() for seg in normalized.split("→") if seg.strip()]

    @staticmethod
    def check_chain_nodes(chain: list[str]) -> list[dict[str, Any]]:
        """逐节点检查就绪状态：节点需存在于某个模块且不处于停用类状态。"""
        node_states: list[dict[str, Any]] = []
        for node in chain:
            matched: dict[str, Any] | None = None
            for module, name_field in _NODE_MODULES:
                for row in store.rows(module):
                    if str(row.get(name_field, "")) == node:
                        matched = {
                            "node": node,
                            "module": module,
                            "ref_status": row.get("status", ""),
                            "ready": row.get("status", "") not in _NODE_BAD_STATUSES,
                        }
                        break
                if matched is not None:
                    break
            if matched is None:
                matched = {"node": node, "module": None, "ref_status": None, "ready": False}
            node_states.append(matched)
        return node_states

    def deploy_operator_chain(
        self,
        entry_id: int,
        *,
        group_id: str | None = None,
        item_version: str | None = None,
    ) -> dict[str, Any]:
        """读取拓扑配置并部署算子链，发布拓扑运行数据。

        事务边界：算子链部署成功与状态存储记录同时可见；
        节点未全部就绪时不发布运行版本；同一拓扑重复部署只保留一个当前运行快照。
        """
        topo = store.find(self.MODULE, entry_id)
        if topo is None:
            return {"ok": False, "error": f"{self.ENTITY} {entry_id} 不存在或已归档"}
        topology_name = str(topo.get("拓扑名称", ""))
        chain = self.parse_chain(str(topo.get("算子链", "")))
        if not chain:
            return {"ok": False, "error": f"拓扑「{topology_name}」算子链为空，无法部署"}

        node_states = self.check_chain_nodes(chain)
        ready_count = sum(1 for ns in node_states if ns["ready"])
        if ready_count != len(chain):
            # 节点未全部就绪：不发布运行版本，仅返回逐节点结果
            return {
                "ok": False,
                "published": False,
                "error": f"{ready_count}/{len(chain)} 节点就绪，未全部就绪不发布运行版本",
                "topology": topology_name,
                "nodes": node_states,
            }

        lock = store.deploy_lock(topology_name)
        with lock:
            state_entry: dict[str, Any] | None = None
            snapshot = None
            try:
                # 1. 联动写入状态存储（拓扑运行数据 ↔ 状态存储）
                state_entry = {
                    "id": max(
                        (int(r.get("id", 0)) for r in store.rows("state_store")),
                        default=0,
                    ) + 1,
                    "配置名称": f"topology_runtime_{topology_name}",
                    "后端类型": "memory",
                    "状态键模式": f"topology://{topology_name}/runtime",
                    "TTL时长": "0",
                    "检查点间隔": "30",
                    "增量模式": "overwrite",
                    "清理策略": "manual",
                    "status": "已生效",
                    "pending": False,
                    "abnormal": False,
                    "_link_type": "topology_runtime",
                    "_topology": topology_name,
                    "_node_states": node_states,
                    "_group_id": group_id,
                    "_timestamp": time.time(),
                }
                # 2. 发布运行版本（旧当前快照被取代，只保留一个当前运行快照）
                snapshot = store.publish_runtime_snapshot(
                    topology_name,
                    int(topo.get("id", 0)),
                    chain,
                    node_states,
                    group_id=group_id,
                    item_version=item_version,
                    state_entry_id=state_entry["id"],
                )
                state_entry["_topology_version"] = snapshot.version
                state_entry["_runtime_id"] = snapshot.runtime_id
                store.rows("state_store").append(state_entry)
                # 3. 更新拓扑运行状态：部署成功与状态记录同时可见
                topo["status"] = "运行中"
                topo["运行状态"] = f"{snapshot.version}·{len(chain)}节点就绪"
                topo["pending"] = False
                topo["abnormal"] = False
            except Exception:
                # 事务回滚：状态记录与运行快照都不可见
                if state_entry is not None and state_entry in store.rows("state_store"):
                    store.rows("state_store").remove(state_entry)
                if snapshot is not None:
                    store.remove_runtime_snapshot(snapshot.runtime_id)
                raise

        return {
            "ok": True,
            "published": True,
            "topology": topology_name,
            "version": snapshot.version,
            "runtime_id": snapshot.runtime_id,
            "item_version": item_version,
            "group_id": group_id,
            "state_entry_id": state_entry["id"],
            "nodes": node_states,
            "message": f"拓扑「{topology_name}」算子链部署成功，运行版本 {snapshot.version} 已发布",
        }

    def deploy_by_name(
        self,
        topology_name: str,
        *,
        group_id: str | None = None,
        item_version: str | None = None,
    ) -> dict[str, Any]:
        """按拓扑名称部署（序号13批量入口按名称逐条调用）。"""
        for row in store.rows(self.MODULE):
            if str(row.get("拓扑名称", "")) == topology_name:
                return self.deploy_operator_chain(
                    int(row.get("id", 0)),
                    group_id=group_id,
                    item_version=item_version,
                )
        return {"ok": False, "error": f"拓扑「{topology_name}」不存在", "topology": topology_name}

    def runtime_data(self, entry_id: int) -> dict[str, Any] | None:
        """拓扑运行数据：当前版本快照 + 降级节点 + 历史版本。"""
        topo = store.find(self.MODULE, entry_id)
        if topo is None:
            return None
        topology_name = str(topo.get("拓扑名称", ""))
        current = store.current_runtime(topology_name)
        history = store.runtime_history(topology_name)
        return {
            "topology": topology_name,
            "topology_id": entry_id,
            "topology_status": topo.get("status", ""),
            "current": current.as_dict() if current else None,
            "degraded_nodes": store.degraded_nodes(topology_name),
            "history": [snap.as_dict() for snap in history],
        }

    def list_runtime_data(self) -> list[dict[str, Any]]:
        """全部拓扑运行数据（供批量选择与批量结果页核对）。"""
        return [snap.as_dict() for snap in store.runtime_history()]

    def validate_topology_dag(self, topology_name: str) -> dict[str, Any]:
        """校验拓扑DAG的合法性：算子链节点数、并行度范围、资源配额。"""
        topo = None
        for r in store.rows(self.MODULE):
            if r.get("拓扑名称") == topology_name:
                topo = r
                break
        if topo is None:
            return {"ok": False, "error": f"拓扑「{topology_name}」不存在"}

        checks = []
        chain = [
            c.strip()
            for c in topo.get("算子链", "").split("→")
            if c.strip()
        ]
        checks.append({
            "check": "chain_nodes",
            "ok": 2 <= len(chain) <= 10,
            "detail": f"算子链节点数: {len(chain)}",
        })

        try:
            parallelism = int(topo.get("并行度", "4"))
            checks.append({
                "check": "parallelism",
                "ok": 1 <= parallelism <= 64,
                "detail": f"并行度: {parallelism}",
            })
        except (ValueError, TypeError):
            checks.append({"check": "parallelism", "ok": False, "detail": "并行度格式错误"})

        # 资源配额校验
        quota = topo.get("资源配额", "")
        if quota:
            checks.append({
                "check": "resource_quota",
                "ok": any(unit in quota for unit in ("cpu", "mem", "gpu", "core")),
                "detail": f"资源配额: {quota}",
            })

        return {
            "ok": all(c["ok"] for c in checks),
            "topology": topology_name,
            "chain": chain,
            "checks": checks,
        }

    def deploy_with_topology_check(
        self, topology_name: str,
    ) -> dict[str, Any]:
        """部署拓扑前检查算子链中每个节点对应的模块是否可用。"""
        topo = None
        for r in store.rows(self.MODULE):
            if r.get("拓扑名称") == topology_name:
                topo = r
                break
        if topo is None:
            return {"ok": False, "error": f"拓扑「{topology_name}」不存在"}

        chain = [
            c.strip()
            for c in topo.get("算子链", "").split("→")
            if c.strip()
        ]
        node_status = []
        for node in chain:
            found = False
            for mod in ["stream_source", "calc_operator", "sliding_index", "window_config"]:
                for r in store.rows(mod):
                    if node in str(r.get(list(r.keys())[:3])):
                        found = True
                        node_status.append({
                            "node": node,
                            "module": mod,
                            "found": True,
                            "status": r.get("status", ""),
                        })
                        break
                if found:
                    break
            if not found:
                node_status.append({"node": node, "found": False})

        all_found = all(ns["found"] for ns in node_status)
        if all_found:
            topo["status"] = "运行中"
            topo["pending"] = True

        return {
            "ok": all_found,
            "topology": topology_name,
            "nodes": node_status,
        }

    def evaluate_failover(self, topology_name: str) -> dict[str, Any]:
        """评估故障转移能力：检查拓扑中是否有备用调度路径。"""
        topo = None
        for r in store.rows(self.MODULE):
            if r.get("拓扑名称") == topology_name:
                topo = r
                break
        if topo is None:
            return {"ok": False, "error": f"拓扑「{topology_name}」不存在"}

        failover_strategy = topo.get("故障转移", "")
        restore_plans = [
            rp for rp in store.rows("restore_plan")
            if topology_name in str(rp.get("任务范围", ""))
        ]
        checkpoints = [
            cp for cp in store.rows("checkpoint_mgr")
            if cp.get("checkpoint_id") and cp.get("status") == "已启用"
        ]

        can_failover = bool(
            failover_strategy or len(restore_plans) > 0 or len(checkpoints) > 0
        )
        return {
            "ok": True,
            "topology": topology_name,
            "can_failover": can_failover,
            "strategy": failover_strategy,
            "restore_plans": len(restore_plans),
            "available_checkpoints": len(checkpoints),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["deployed"] = sum(1 for r in rows if r.get("status") == "已部署")
        base["running"] = sum(1 for r in rows if r.get("status") == "运行中")
        base["runtime_versions"] = len(store.runtime_history())
        base["degraded_topologies"] = len({
            e.topology_name for e in store.degradations() if e.status == "生效中"
        })
        return base