"""序号13批量处理：批量选择多条拓扑运行数据，整组部署并生成处理台账。

行为约定：
  - 一次提交保留整组结果（组级版本 + 逐条版本）；
  - 并发重试按幂等键只接收一组版本，重复提交复用既有整组结果；
  - 逐条业务失败（如节点未就绪）记入详情，整组继续，状态为部分失败；
  - 事务异常（非业务失败）回滚整组：撤回本组运行快照、状态记录与降级清单。
"""
from __future__ import annotations

import hashlib
from typing import Any

from app.services.task_topology import TaskTopologyService
from app.store import store


class TopologyBatchService:
    """序号13批量入口：整组处理拓扑运行数据。"""

    def __init__(self, topology_service: TaskTopologyService | None = None) -> None:
        self._topology = topology_service or TaskTopologyService()

    @staticmethod
    def _default_key(names: list[str]) -> str:
        """未显式传幂等键时，按拓扑集合生成确定性的幂等键。"""
        digest = hashlib.md5(f"batch:{'|'.join(sorted(names))}".encode()).hexdigest()
        return f"auto-{digest[:16]}"

    def submit_batch(
        self,
        topology_names: list[str],
        *,
        idempotency_key: str | None = None,
        mode: str = "unified",
        initiator: str = "manual",
    ) -> dict[str, Any]:
        """批量部署一组拓扑运行数据。

        并发/重试安全：同一幂等键只产生一组结果；事务异常回滚整组。
        """
        names = [str(n).strip() for n in topology_names if str(n).strip()]
        if not names:
            return {"ok": False, "error": "未选择任何拓扑运行数据"}
        key = (idempotency_key or "").strip() or self._default_key(names)

        group, created = store.create_batch_group(
            key, names, mode=mode, initiator=initiator
        )
        if not created:
            # 并发重试只记一次：直接复用既有整组结果，不重复发布版本
            return {
                "ok": group.status != "已回滚",
                "deduplicated": True,
                "rolled_back": group.rolled_back,
                "group": group.as_dict(),
                "message": f"命中幂等键 {key}，复用既有整组结果 {group.group_version}",
            }

        results: list[dict[str, Any]] = []
        try:
            for idx, name in enumerate(names, 1):
                item_version = f"{group.group_version}.{idx}"
                res = self._topology.deploy_by_name(
                    name,
                    group_id=group.group_id,
                    item_version=item_version,
                )
                results.append({
                    "topology": name,
                    "item_version": item_version,
                    "ok": bool(res.get("ok")),
                    "version": res.get("version"),
                    "runtime_id": res.get("runtime_id"),
                    "state_entry_id": res.get("state_entry_id"),
                    "error": res.get("error"),
                })
                # 逐条结果即时入台账，详情页可看到处理进度
                group.items = list(results)
        except Exception as exc:
            # 事务异常：回滚整组（运行快照 + 状态记录 + 降级清单）
            rollback = store.rollback_batch(group.group_id)
            store.finalize_batch(group.group_id, "已回滚", error=str(exc))
            return {
                "ok": False,
                "deduplicated": False,
                "rolled_back": True,
                "error": f"批量事务异常，已回滚整组：{exc}",
                "rollback": rollback,
                "group": group.as_dict(),
            }

        failed = [r for r in results if not r["ok"]]
        if not failed:
            status = "已完成"
        elif len(failed) < len(results):
            status = "部分失败"
        else:
            status = "全部失败"
        store.finalize_batch(group.group_id, status)
        return {
            "ok": not failed,
            "deduplicated": False,
            "rolled_back": False,
            "group": group.as_dict(),
            "message": (
                f"整组处理完成：{group.group_version}，"
                f"成功 {len(results) - len(failed)} 条，失败 {len(failed)} 条"
            ),
        }

    def get_batch(self, group_id: str) -> dict[str, Any] | None:
        """批次详情：逐条成功失败与逐条版本。"""
        group = store.get_batch(group_id)
        return group.as_dict() if group else None

    def list_batches(self) -> list[dict[str, Any]]:
        """整组处理台账（另一入口联动生成整组清单）。"""
        return [g.as_dict() for g in store.list_batches()]

    def verify_by_version(self, topology_name: str, version: str) -> dict[str, Any]:
        """按拓扑版本核对：运行数据 + 降级状态清单对照。"""
        snapshots = [
            snap.as_dict()
            for snap in store.runtime_history(topology_name)
            if snap.version == version
        ]
        degradations = [
            e.as_dict() for e in store.degradations(topology_name, version)
        ]
        return {
            "topology": topology_name,
            "version": version,
            "runtime": snapshots[0] if snapshots else None,
            "degradations": degradations,
            "matched": bool(snapshots),
        }


# 路由层共享的批量服务单例
batch_service = TopologyBatchService()
