"""吞吐管理领域服务：管理流式计算任务的吞吐容量。

增强能力: 背压自动检测、QPS限流触发、扩容规则评估、降级策略验证、关联topology状态
增强能力: 读取部署后的拓扑运行数据，背压触发时写入降级状态清单并保留拓扑版本；
          支持局部降级（按已部署节点）与统一降级（等整条拓扑就绪）两种取舍。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ThroughputMgrService(BaseModuleService):
    MODULE = "throughput_mgr"
    ENTITY = "吞吐配置"
    LIST_FIELDS = ["配置名称", "数据源", "最大QPS", "并发分区", "背压阈值", "限流策略", "扩容规则"]
    REQUIRED_FIELDS = ["配置名称", "数据源", "最大QPS"]
    STATUS_ORDER = ["草稿", "已生效", "已触发", "已关闭"]
    ACTION_RULES = {"启用配置": "草稿", "关闭配置": "已生效"}
    NEGATIVE_ACTIONS = []

    # ---- 背压触发 → 降级状态清单（联动拓扑运行数据） ----

    def _find_config_by_source(self, source_name: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if row.get("数据源") == source_name:
                return row
        return None

    @staticmethod
    def _throttle_limit(config: dict[str, Any] | None, max_qps: float) -> tuple[str, float]:
        """按限流策略计算降级后的限流值。"""
        strategy = (config or {}).get("限流策略", "token_bucket")
        if strategy == "token_bucket":
            return strategy, max_qps * 0.7
        if strategy == "leaky_bucket":
            return strategy, max_qps * 0.5
        return strategy, max_qps * 0.8

    def trigger_backpressure_for_topology(
        self,
        topology_name: str,
        current_qps: float | None = None,
        *,
        mode: str = "partial",
        group_id: str | None = None,
    ) -> dict[str, Any]:
        """读取部署后的拓扑运行数据，背压触发时写入降级状态清单并保留拓扑版本。

        mode=partial：按已部署节点生成局部降级；
        mode=unified：等整条拓扑就绪后统一降级（未全就绪则拒绝）。
        清单条目始终挂在当前可执行拓扑版本上。
        """
        snapshot = store.current_runtime(topology_name)
        if snapshot is None:
            return {
                "ok": False,
                "triggered": False,
                "error": f"拓扑「{topology_name}」尚未部署运行版本，无法生成降级状态清单",
            }
        source = snapshot.chain[0] if snapshot.chain else ""
        config = self._find_config_by_source(source)
        try:
            max_qps = float((config or {}).get("最大QPS", "1000"))
        except (ValueError, TypeError):
            max_qps = 1000.0
        try:
            threshold = float((config or {}).get("背压阈值", "0.75"))
        except (ValueError, TypeError):
            threshold = 0.75
        if current_qps is None:
            # 未指定负载时按 90% 负载模拟，确保背压场景可演示
            current_qps = max_qps * 0.9
        load_ratio = current_qps / max(max_qps, 1)
        overloaded = load_ratio > threshold
        if not overloaded:
            return {
                "ok": True,
                "triggered": False,
                "topology": topology_name,
                "version": snapshot.version,
                "current_qps": current_qps,
                "max_qps": max_qps,
                "load_ratio": round(load_ratio, 4),
                "threshold": threshold,
                "message": "负载未超背压阈值，未触发降级",
            }

        ready_nodes = [ns["node"] for ns in snapshot.node_states if ns.get("ready")]
        if mode == "unified":
            if len(ready_nodes) != len(snapshot.chain):
                return {
                    "ok": False,
                    "triggered": False,
                    "error": "统一降级要求整条拓扑就绪，当前存在未就绪节点",
                    "topology": topology_name,
                    "version": snapshot.version,
                    "ready_nodes": ready_nodes,
                }
            affected = list(snapshot.chain)
        else:
            # 局部降级：只覆盖已部署（就绪）节点
            affected = ready_nodes
        if not affected:
            return {
                "ok": False,
                "triggered": False,
                "error": "当前版本没有可降级的已部署节点",
                "topology": topology_name,
                "version": snapshot.version,
            }

        strategy, throttle_limit = self._throttle_limit(config, max_qps)
        entries = store.record_degradations(
            topology_name,
            snapshot.version,
            snapshot.runtime_id,
            affected,
            source=source,
            mode=mode,
            original_qps=current_qps,
            throttle_limit=throttle_limit,
            group_id=group_id,
        )
        if config is not None and config.get("status") != "已触发":
            config["status"] = "已触发"
            config["pending"] = True
            config["abnormal"] = True

        return {
            "ok": True,
            "triggered": True,
            "topology": topology_name,
            "version": snapshot.version,
            "runtime_id": snapshot.runtime_id,
            "mode": mode,
            "source": source,
            "current_qps": current_qps,
            "max_qps": max_qps,
            "load_ratio": round(load_ratio, 4),
            "strategy": strategy,
            "throttle_limit": throttle_limit,
            "affected_nodes": affected,
            "entries": [e.as_dict() for e in entries],
            "message": f"背压触发，拓扑「{topology_name}」{snapshot.version} 已写入 {len(entries)} 条降级记录",
        }

    def degradation_list(
        self,
        topology_name: str | None = None,
        version: str | None = None,
    ) -> list[dict[str, Any]]:
        """降级状态清单查询：按拓扑/版本核对受影响节点与限流状态。"""
        return [e.as_dict() for e in store.degradations(topology_name, version)]

    def topology_throttle_status(self, topology_name: str) -> dict[str, Any]:
        """拓扑维度核对：当前版本、受影响节点与限流状态。"""
        snapshot = store.current_runtime(topology_name)
        degraded = store.degraded_nodes(topology_name)
        source = snapshot.chain[0] if snapshot and snapshot.chain else ""
        config = self._find_config_by_source(source) if source else None
        return {
            "topology": topology_name,
            "version": snapshot.version if snapshot else None,
            "source": source,
            "config_status": (config or {}).get("status"),
            "throttle_limits": {
                d["node"]: d["throttle_limit"] for d in degraded
            },
            "degraded_nodes": degraded,
        }

    def monitor_backpressure(
        self, source_name: str, current_qps: float,
    ) -> dict[str, Any]:
        """监控背压状态：计算负载比、触发限流判断。"""
        config = None
        for r in store.rows(self.MODULE):
            if r.get("数据源") == source_name:
                config = r
                break
        try:
            max_qps = float((config or {}).get("最大QPS", "1000"))
            threshold = float((config or {}).get("背压阈值", "0.75"))
        except (ValueError, TypeError):
            max_qps, threshold = 1000.0, 0.75

        load_ratio = current_qps / max(max_qps, 1)
        is_overloaded = load_ratio > threshold

        if config and is_overloaded and config.get("status") != "已触发":
            config["status"] = "已触发"
            config["pending"] = True
            config["abnormal"] = True

        return {
            "ok": True,
            "source": source_name,
            "current_qps": current_qps,
            "max_qps": max_qps,
            "load_ratio": round(load_ratio, 4),
            "threshold": threshold,
            "overloaded": is_overloaded,
            "strategy": (config or {}).get("限流策略", "token_bucket"),
        }

    def apply_throttle_action(
        self, source_name: str,
    ) -> dict[str, Any]:
        """应用限流策略：计算降级后的QPS并写入状态存储。"""
        config = None
        for r in store.rows(self.MODULE):
            if r.get("数据源") == source_name:
                config = r
                break
        if config is None:
            return {"ok": False, "error": f"吞吐配置「{source_name}」不存在"}

        try:
            max_qps = float(config.get("最大QPS", "1000"))
        except (ValueError, TypeError):
            max_qps = 1000.0

        strategy = config.get("限流策略", "token_bucket")
        if strategy == "token_bucket":
            new_limit = max_qps * 0.7
        elif strategy == "leaky_bucket":
            new_limit = max_qps * 0.5
        else:
            new_limit = max_qps * 0.8

        # 写入状态存储记录
        state_entry = {
            "id": max(
                (int(r.get("id", 0)) for r in store.rows("state_store")), default=0
            ) + 1,
            "配置名称": f"throttle_{source_name}_{int(time.time())}",
            "后端类型": "memory",
            "状态键模式": f"throttle://{source_name}",
            "TTL时长": "3600",
            "检查点间隔": "30",
            "增量模式": "overwrite",
            "清理策略": "ttl_expire",
            "status": "已生效",
            "pending": True,
            "abnormal": True,
            "_throttle_source": source_name,
            "_original_qps": max_qps,
            "_new_limit": new_limit,
            "_strategy": strategy,
            "_timestamp": time.time(),
        }
        store.rows("state_store").append(state_entry)

        return {
            "ok": True,
            "source": source_name,
            "strategy": strategy,
            "original_qps": max_qps,
            "new_limit": new_limit,
            "reduction_pct": round((1 - new_limit / max_qps) * 100, 1),
        }

    def evaluate_auto_scale(self, source_name: str) -> dict[str, Any]:
        """评估自动扩容规则：判断是否需要扩容及扩容数量。"""
        config = None
        for r in store.rows(self.MODULE):
            if r.get("数据源") == source_name:
                config = r
                break
        if config is None:
            return {"ok": False, "error": f"吞吐配置「{source_name}」不存在"}

        scale_rule = config.get("扩容规则", "").lower()
        try:
            current_partitions = int(config.get("并发分区", "4"))
        except (ValueError, TypeError):
            current_partitions = 4

        if "double" in scale_rule or "翻倍" in scale_rule:
            new_partitions = min(current_partitions * 2, 64)
        elif "incremental" in scale_rule or "增量" in scale_rule:
            new_partitions = min(current_partitions + 4, 64)
        else:
            new_partitions = current_partitions

        should_scale = config.get("status") == "已触发"

        return {
            "ok": True,
            "source": source_name,
            "should_scale": should_scale,
            "current_partitions": current_partitions,
            "new_partitions": new_partitions if should_scale else current_partitions,
            "scale_rule": scale_rule,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["triggered"] = sum(1 for r in rows if r.get("status") == "已触发")
        base["active"] = sum(1 for r in rows if r.get("status") == "已生效")
        base["active_degradations"] = sum(
            1 for e in store.degradations() if e.status == "生效中"
        )
        return base