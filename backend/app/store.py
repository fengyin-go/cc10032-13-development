"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。

增强: DataChannel 跨模块数据通道 + FailureRecovery 失败恢复机制
增强: 拓扑运行数据快照 + 降级状态清单 + 批量处理台账（序号13），
      支撑 task_topology → throughput_mgr → state_store 的联动事务。
"""
from __future__ import annotations

import copy
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from app.seed import SEED_ROWS


@dataclass
class DataChannelEvent:
    """跨模块数据通道事件：模块间数据传递的统一载体。"""
    channel_id: str
    source_module: str
    target_module: str
    data_type: str
    payload: dict[str, Any]
    timestamp: float
    status: str = "pending"  # pending, delivered, consumed, failed
    retry_count: int = 0
    max_retries: int = 3
    _history: list[dict[str, Any]] = field(default_factory=list)

    def mark_delivered(self) -> None:
        self.status = "delivered"
        self._history.append({"status": "delivered", "ts": time.time()})

    def mark_consumed(self, consumer_response: dict[str, Any] | None = None) -> None:
        self.status = "consumed"
        self._history.append({
            "status": "consumed",
            "ts": time.time(),
            "response": consumer_response,
        })

    def mark_failed(self, error: str) -> None:
        self.status = "failed"
        self._history.append({"status": "failed", "ts": time.time(), "error": error})

    def can_retry(self) -> bool:
        return self.retry_count < self.max_retries


@dataclass
class FailureRecovery:
    """失败恢复记录：跟踪模块间数据流中的失败并执行恢复。"""
    recovery_id: str
    failed_module: str
    failed_operation: str
    error_detail: str
    recovery_strategy: str  # retry, skip, checkpoint, escalate
    context: dict[str, Any]
    status: str = "open"  # open, recovering, recovered, escalated
    created_at: float = field(default_factory=time.time)
    resolved_at: float | None = None

    def attempt_recovery(self) -> dict[str, Any]:
        self.status = "recovering"
        if self.recovery_strategy == "retry":
            return {"action": "retry", "context": self.context, "recovery_id": self.recovery_id}
        elif self.recovery_strategy == "skip":
            self.status = "recovered"
            self.resolved_at = time.time()
            return {"action": "skip", "recovery_id": self.recovery_id}
        elif self.recovery_strategy == "checkpoint":
            return {"action": "restore_from_checkpoint", "recovery_id": self.recovery_id}
        else:  # escalate
            self.status = "escalated"
            return {"action": "escalate", "recovery_id": self.recovery_id}

    def mark_resolved(self) -> None:
        self.status = "recovered"
        self.resolved_at = time.time()


@dataclass
class TopologyRuntimeSnapshot:
    """拓扑运行数据：一次算子链部署发布的运行版本快照。

    同一拓扑只允许一个 is_current=True 的快照；重复部署时旧快照被取代，
    历史版本保留用于按版本核对。
    """
    runtime_id: str
    topology_name: str
    topology_id: int
    version: str                     # 拓扑级递增版本：v1、v2…
    chain: list[str]                 # 算子链节点
    node_states: list[dict[str, Any]]  # 逐节点就绪结果
    status: str = "就绪"             # 就绪 / 已取代 / 已回滚
    is_current: bool = True
    group_id: str | None = None      # 来源批量组（序号13），手工部署为 None
    item_version: str | None = None  # 组内逐条版本，如 G0001.2
    state_entry_id: int | None = None  # 联动写入 state_store 的记录 id
    created_at: float = field(default_factory=time.time)

    def as_dict(self) -> dict[str, Any]:
        return {
            "runtime_id": self.runtime_id,
            "topology_name": self.topology_name,
            "topology_id": self.topology_id,
            "version": self.version,
            "chain": list(self.chain),
            "node_states": copy.deepcopy(self.node_states),
            "status": self.status,
            "is_current": self.is_current,
            "group_id": self.group_id,
            "item_version": self.item_version,
            "state_entry_id": self.state_entry_id,
            "created_at": self.created_at,
        }


@dataclass
class DegradedStateEntry:
    """降级状态清单条目：背压触发后某个拓扑节点的降级记录。

    保留拓扑版本，供拓扑页面与批量结果页按版本核对受影响节点。
    """
    degrade_id: str
    topology_name: str
    topology_version: str            # 保留拓扑版本
    runtime_id: str
    node: str
    source: str
    mode: str                        # partial 局部降级 / unified 统一降级
    original_qps: float
    throttle_limit: float            # 限流值
    reason: str = "背压触发"
    status: str = "生效中"           # 生效中 / 已解除 / 已回滚
    group_id: str | None = None
    created_at: float = field(default_factory=time.time)

    def as_dict(self) -> dict[str, Any]:
        return {
            "degrade_id": self.degrade_id,
            "topology_name": self.topology_name,
            "topology_version": self.topology_version,
            "runtime_id": self.runtime_id,
            "node": self.node,
            "source": self.source,
            "mode": self.mode,
            "original_qps": self.original_qps,
            "throttle_limit": self.throttle_limit,
            "reason": self.reason,
            "status": self.status,
            "group_id": self.group_id,
            "created_at": self.created_at,
        }


@dataclass
class TopologyBatchGroup:
    """序号13批量处理台账：一组拓扑运行数据的整组处理结果。

    一次提交保留整组结果；并发重试按幂等键只接收一组版本；
    事务异常后整组回滚。
    """
    group_id: str
    idempotency_key: str
    group_version: str               # 组级版本，如 G0001
    topology_names: list[str]
    mode: str = "unified"
    status: str = "处理中"           # 处理中 / 已完成 / 部分失败 / 已回滚
    items: list[dict[str, Any]] = field(default_factory=list)  # 逐条结果
    rolled_back: bool = False
    error: str | None = None
    initiator: str = "manual"
    created_at: float = field(default_factory=time.time)
    finished_at: float | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "group_id": self.group_id,
            "idempotency_key": self.idempotency_key,
            "group_version": self.group_version,
            "topology_names": list(self.topology_names),
            "mode": self.mode,
            "status": self.status,
            "items": copy.deepcopy(self.items),
            "rolled_back": self.rolled_back,
            "error": self.error,
            "initiator": self.initiator,
            "created_at": self.created_at,
            "finished_at": self.finished_at,
            "success_count": sum(1 for it in self.items if it.get("ok")),
            "fail_count": sum(1 for it in self.items if not it.get("ok")),
        }


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 跨模块数据通道：channel_id → DataChannelEvent
        self._channels: dict[str, DataChannelEvent] = {}
        # 失败恢复记录：recovery_id → FailureRecovery
        self._failures: dict[str, FailureRecovery] = {}
        # 拓扑运行数据：runtime_id → TopologyRuntimeSnapshot
        self._runtime_snapshots: dict[str, TopologyRuntimeSnapshot] = {}
        # 拓扑名 → 运行版本历史（按发布时间升序的 runtime_id 列表）
        self._runtime_history: dict[str, list[str]] = {}
        # 降级状态清单：背压触发后逐节点写入
        self._degraded_states: list[DegradedStateEntry] = []
        # 序号13批量处理台账：group_id → TopologyBatchGroup
        self._batch_groups: dict[str, TopologyBatchGroup] = {}
        # 幂等键 → group_id：并发重试只接收一组版本
        self._batch_idem_index: dict[str, str] = {}
        # 并发控制：拓扑级部署锁 + 批量提交锁 + 计数器锁
        self._deploy_locks: dict[str, threading.Lock] = {}
        self._deploy_locks_guard = threading.Lock()
        self._batch_lock = threading.Lock()
        self._seq_lock = threading.Lock()
        self._runtime_seq = 0
        self._degrade_seq = 0
        self._batch_seq = 0

    # ---- 序号生成 ----

    def _next_seq(self, kind: str) -> int:
        with self._seq_lock:
            if kind == "runtime":
                self._runtime_seq += 1
                return self._runtime_seq
            if kind == "degrade":
                self._degrade_seq += 1
                return self._degrade_seq
            self._batch_seq += 1
            return self._batch_seq

    # ---- 拓扑运行数据 ----

    def deploy_lock(self, topology_name: str) -> threading.Lock:
        """按拓扑名取部署锁：并发部署同一拓扑时串行化，保证只保留一个当前快照。"""
        with self._deploy_locks_guard:
            lock = self._deploy_locks.get(topology_name)
            if lock is None:
                lock = threading.Lock()
                self._deploy_locks[topology_name] = lock
            return lock

    def publish_runtime_snapshot(
        self,
        topology_name: str,
        topology_id: int,
        chain: list[str],
        node_states: list[dict[str, Any]],
        *,
        group_id: str | None = None,
        item_version: str | None = None,
        state_entry_id: int | None = None,
    ) -> TopologyRuntimeSnapshot:
        """发布拓扑运行版本：旧当前快照被取代，其降级清单条目同步解除。

        调用方需先持有 deploy_lock(topology_name)，保证并发下只有一个当前快照。
        """
        history = self._runtime_history.setdefault(topology_name, [])
        previous_id = history[-1] if history else None
        previous = self._runtime_snapshots.get(previous_id) if previous_id else None
        if previous is not None:
            previous.is_current = False
            previous.status = "已取代"
            # 旧版本联动的状态记录同步停用，只保留一个当前运行快照
            if previous.state_entry_id is not None:
                for row in self.rows("state_store"):
                    if row.get("id") == previous.state_entry_id:
                        row["status"] = "已停用"
                        break
        # 旧版本的降级条目随版本更替解除，清单最终只呈现一个可执行拓扑版本
        for entry in self._degraded_states:
            if entry.topology_name == topology_name and entry.status == "生效中":
                entry.status = "已解除"
        version = f"v{len(history) + 1}"
        seq = self._next_seq("runtime")
        snapshot = TopologyRuntimeSnapshot(
            runtime_id=f"RT-{seq:05d}",
            topology_name=topology_name,
            topology_id=topology_id,
            version=version,
            chain=list(chain),
            node_states=copy.deepcopy(node_states),
            status="就绪",
            is_current=True,
            group_id=group_id,
            item_version=item_version,
            state_entry_id=state_entry_id,
        )
        self._runtime_snapshots[snapshot.runtime_id] = snapshot
        history.append(snapshot.runtime_id)
        return snapshot

    def current_runtime(self, topology_name: str) -> TopologyRuntimeSnapshot | None:
        """读取拓扑当前运行快照（部署后的拓扑运行数据）。"""
        history = self._runtime_history.get(topology_name, [])
        for runtime_id in reversed(history):
            snap = self._runtime_snapshots.get(runtime_id)
            if snap is not None and snap.is_current:
                return snap
        return None

    def runtime_history(self, topology_name: str | None = None) -> list[TopologyRuntimeSnapshot]:
        """运行版本历史：指定拓扑或全部拓扑，按发布时间升序。"""
        if topology_name is not None:
            ids = self._runtime_history.get(topology_name, [])
            return [
                self._runtime_snapshots[rid] for rid in ids
                if rid in self._runtime_snapshots
            ]
        snapshots: list[TopologyRuntimeSnapshot] = []
        for ids in self._runtime_history.values():
            snapshots.extend(
                self._runtime_snapshots[rid] for rid in ids
                if rid in self._runtime_snapshots
            )
        return sorted(snapshots, key=lambda s: s.created_at)

    def remove_runtime_snapshot(self, runtime_id: str) -> None:
        """移除运行快照（批量回滚用），并尝试恢复上一版本为当前版本。"""
        snap = self._runtime_snapshots.pop(runtime_id, None)
        if snap is None:
            return
        history = self._runtime_history.get(snap.topology_name, [])
        if runtime_id in history:
            history.remove(runtime_id)
        if snap.is_current:
            for rid in reversed(history):
                prev = self._runtime_snapshots.get(rid)
                if prev is not None and prev.status != "已回滚":
                    prev.is_current = True
                    prev.status = "就绪"
                    # 恢复其联动的状态记录为生效
                    if prev.state_entry_id is not None:
                        for row in self.rows("state_store"):
                            if row.get("id") == prev.state_entry_id:
                                row["status"] = "已生效"
                                break
                    break

    # ---- 降级状态清单 ----

    def record_degradations(
        self,
        topology_name: str,
        topology_version: str,
        runtime_id: str,
        nodes: list[str],
        *,
        source: str,
        mode: str,
        original_qps: float,
        throttle_limit: float,
        group_id: str | None = None,
    ) -> list[DegradedStateEntry]:
        """背压触发时写入降级状态清单，逐节点一条，保留拓扑版本。"""
        entries: list[DegradedStateEntry] = []
        for node in nodes:
            seq = self._next_seq("degrade")
            entry = DegradedStateEntry(
                degrade_id=f"DEG-{seq:05d}",
                topology_name=topology_name,
                topology_version=topology_version,
                runtime_id=runtime_id,
                node=node,
                source=source,
                mode=mode,
                original_qps=original_qps,
                throttle_limit=throttle_limit,
                group_id=group_id,
            )
            self._degraded_states.append(entry)
            entries.append(entry)
        return entries

    def degradations(
        self,
        topology_name: str | None = None,
        version: str | None = None,
    ) -> list[DegradedStateEntry]:
        """按拓扑/版本查询降级状态清单，供批量结果页对照核对。"""
        entries = list(self._degraded_states)
        if topology_name is not None:
            entries = [e for e in entries if e.topology_name == topology_name]
        if version is not None:
            entries = [e for e in entries if e.topology_version == version]
        return entries

    def degraded_nodes(self, topology_name: str) -> list[dict[str, Any]]:
        """拓扑当前版本中仍处于降级状态的节点，供拓扑页面判断。"""
        current = self.current_runtime(topology_name)
        if current is None:
            return []
        return [
            {
                "node": e.node,
                "throttle_limit": e.throttle_limit,
                "original_qps": e.original_qps,
                "mode": e.mode,
                "degrade_id": e.degrade_id,
                "version": e.topology_version,
            }
            for e in self._degraded_states
            if e.topology_name == topology_name
            and e.topology_version == current.version
            and e.status == "生效中"
        ]

    # ---- 序号13批量处理台账 ----

    def create_batch_group(
        self,
        idempotency_key: str,
        topology_names: list[str],
        *,
        mode: str = "unified",
        initiator: str = "manual",
    ) -> tuple[TopologyBatchGroup, bool]:
        """创建批量组；幂等键已存在时返回既有组（并发重试只记一次）。

        返回 (group, created)：created=False 表示命中幂等，复用既有整组结果。
        """
        with self._batch_lock:
            existing_id = self._batch_idem_index.get(idempotency_key)
            if existing_id is not None:
                existing = self._batch_groups.get(existing_id)
                if existing is not None:
                    return existing, False
            seq = self._next_seq("batch")
            group = TopologyBatchGroup(
                group_id=f"BG-{seq:05d}",
                idempotency_key=idempotency_key,
                group_version=f"G{seq:04d}",
                topology_names=list(topology_names),
                mode=mode,
                initiator=initiator,
            )
            self._batch_groups[group.group_id] = group
            self._batch_idem_index[idempotency_key] = group.group_id
            return group, True

    def get_batch(self, group_id: str) -> TopologyBatchGroup | None:
        return self._batch_groups.get(group_id)

    def list_batches(self) -> list[TopologyBatchGroup]:
        """整组处理台账：按创建时间倒序。"""
        return sorted(
            self._batch_groups.values(), key=lambda g: g.created_at, reverse=True
        )

    def finalize_batch(self, group_id: str, status: str, error: str | None = None) -> None:
        group = self._batch_groups.get(group_id)
        if group is None:
            return
        group.status = status
        group.error = error
        group.finished_at = time.time()

    def rollback_batch(self, group_id: str) -> dict[str, Any]:
        """事务异常后回滚整组：撤掉本组发布的运行快照、状态记录与降级清单。"""
        group = self._batch_groups.get(group_id)
        if group is None:
            return {"ok": False, "error": f"批量组 {group_id} 不存在"}
        removed_snapshots: list[str] = []
        removed_state_entries: list[int] = []
        # 1. 撤回本组发布的运行快照及其联动的状态存储记录
        for snap in list(self._runtime_snapshots.values()):
            if snap.group_id != group_id:
                continue
            if snap.state_entry_id is not None:
                state_rows = self.rows("state_store")
                for row in list(state_rows):
                    if row.get("id") == snap.state_entry_id:
                        state_rows.remove(row)
                        removed_state_entries.append(snap.state_entry_id)
                        break
            snap.status = "已回滚"
            self.remove_runtime_snapshot(snap.runtime_id)
            removed_snapshots.append(snap.runtime_id)
        # 2. 解除本组写入的降级清单条目
        rolled_degradations = 0
        for entry in self._degraded_states:
            if entry.group_id == group_id and entry.status == "生效中":
                entry.status = "已回滚"
                rolled_degradations += 1
        # 3. 台账标记回滚
        group.rolled_back = True
        group.status = "已回滚"
        group.finished_at = time.time()
        return {
            "ok": True,
            "group_id": group_id,
            "removed_snapshots": removed_snapshots,
            "removed_state_entries": removed_state_entries,
            "rolled_back_degradations": rolled_degradations,
        }

    # ---- 跨模块数据通道 ----

    def send_to_channel(
        self,
        source: str,
        target: str,
        data_type: str,
        payload: dict[str, Any],
    ) -> DataChannelEvent:
        """通过数据通道将数据从一个模块发送到另一个模块。

        创建 DataChannelEvent 记录并返回，下游模块通过 consume_from_channel 消费。
        """
        import hashlib
        channel_id = hashlib.md5(
            f"{source}_{target}_{data_type}_{time.time()}".encode()
        ).hexdigest()[:16]
        event = DataChannelEvent(
            channel_id=channel_id,
            source_module=source,
            target_module=target,
            data_type=data_type,
            payload=payload,
            timestamp=time.time(),
        )
        self._channels[channel_id] = event
        # 同时写入下游模块的 store 表
        notification = {
            "id": max(
                (int(r.get("id", 0)) for r in self.rows(target)), default=0
            ) + 1,
            "channel_id": channel_id,
            "source": source,
            "data_type": data_type,
            "payload_snapshot": copy.deepcopy(payload),
            "timestamp": event.timestamp,
            "status": "pending",
            "pending": True,
            "abnormal": False,
        }
        self.rows(target).append(notification)
        return event

    def consume_from_channel(
        self, channel_id: str, consumer_module: str,
    ) -> DataChannelEvent | None:
        """消费数据通道中的事件。"""
        event = self._channels.get(channel_id)
        if event is None:
            return None
        if event.target_module != consumer_module:
            return None
        event.mark_consumed()
        # 更新下游模块表中的通知状态
        for row in self.rows(consumer_module):
            if row.get("channel_id") == channel_id:
                row["status"] = "consumed"
                row["pending"] = False
                break
        return event

    def get_pending_channels(self, target_module: str) -> list[DataChannelEvent]:
        """获取目标模块中待消费的数据通道事件。"""
        return [
            e for e in self._channels.values()
            if e.target_module == target_module and e.status == "pending"
        ]

    # ---- 失败恢复 ----

    def register_failure(
        self,
        module: str,
        operation: str,
        error: str,
        strategy: str = "retry",
        context: dict[str, Any] | None = None,
    ) -> FailureRecovery:
        """注册失败事件并创建恢复记录。"""
        import hashlib
        rid = hashlib.md5(
            f"{module}_{operation}_{time.time()}".encode()
        ).hexdigest()[:12]
        recovery = FailureRecovery(
            recovery_id=rid,
            failed_module=module,
            failed_operation=operation,
            error_detail=error,
            recovery_strategy=strategy,
            context=context or {},
        )
        self._failures[rid] = recovery
        return recovery

    def get_open_failures(self, module: str | None = None) -> list[FailureRecovery]:
        """获取未恢复的失败记录。"""
        failures = [
            f for f in self._failures.values()
            if f.status in ("open", "recovering")
        ]
        if module:
            failures = [f for f in failures if f.failed_module == module]
        return failures

    def resolve_failure(self, recovery_id: str) -> FailureRecovery | None:
        """标记失败为已恢复。"""
        recovery = self._failures.get(recovery_id)
        if recovery:
            recovery.mark_resolved()
        return recovery

    def get_failure_stats(self) -> dict[str, Any]:
        """获取失败恢复统计。"""
        all_failures = list(self._failures.values())
        open_count = sum(1 for f in all_failures if f.status in ("open", "recovering"))
        resolved_count = sum(1 for f in all_failures if f.status == "recovered")
        escalated_count = sum(1 for f in all_failures if f.status == "escalated")
        return {
            "total": len(all_failures),
            "open": open_count,
            "resolved": resolved_count,
            "escalated": escalated_count,
        }

    # ---- 基础 CRUD ----

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        # 附加数据通道和失败恢复统计
        channel_stats = {
            "active_channels": len(self._channels),
            "pending_channels": sum(
                1 for e in self._channels.values() if e.status == "pending"
            ),
        }
        failure_stats = self.get_failure_stats()
        # 拓扑运行数据与降级联动统计
        topology_stats = {
            "runtime_snapshots": len(self._runtime_snapshots),
            "current_versions": sum(
                1 for s in self._runtime_snapshots.values() if s.is_current
            ),
            "active_degradations": sum(
                1 for e in self._degraded_states if e.status == "生效中"
            ),
            "batch_groups": len(self._batch_groups),
            "rolled_back_groups": sum(
                1 for g in self._batch_groups.values() if g.rolled_back
            ),
        }
        return {
            "cards": cards,
            "modules": modules,
            "channel_stats": channel_stats,
            "failure_stats": failure_stats,
            "topology_stats": topology_stats,
        }


store = Store()