"""拓扑部署与背压降级联动接口。

覆盖：
  - 任务拓扑：部署算子链（发布拓扑运行数据）、查询拓扑运行数据与降级节点
  - 吞吐管理：背压触发写入降级状态清单、按拓扑/版本核对降级清单
  - 序号13：批量提交拓扑运行数据、整组台账、批次详情、按版本核对

注意：本路由必须在通用资源路由 /api/{module} 之前注册，
否则 /api/topology-batch 等固定路径会被动态模块路径抢先匹配。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services import service_for
from app.services.task_topology import TaskTopologyService
from app.services.throughput_mgr import ThroughputMgrService
from app.services.topology_batch import batch_service

router = APIRouter(prefix="/api", tags=["拓扑部署联动"])

_topology_raw = service_for("task_topology")
_throughput_raw = service_for("throughput_mgr")
assert isinstance(_topology_raw, TaskTopologyService)
assert isinstance(_throughput_raw, ThroughputMgrService)
topology_service: TaskTopologyService = _topology_raw
throughput_service: ThroughputMgrService = _throughput_raw


class BackpressurePayload(BaseModel):
    topology: str
    current_qps: float | None = None
    mode: str = Field(default="partial", pattern="^(partial|unified)$")


class BatchPayload(BaseModel):
    topologies: list[str] = Field(default_factory=list)
    idempotency_key: str | None = None
    mode: str = Field(default="unified", pattern="^(partial|unified)$")
    initiator: str = "manual"


# ---- 任务拓扑：部署算子链 + 拓扑运行数据 ----

@router.post("/task_topology/{entry_id}/deploy", response_model=dict)
def deploy_topology(entry_id: int) -> dict[str, Any]:
    """部署算子链：读取拓扑配置，逐节点就绪检查后发布拓扑运行数据。

    部署成功与状态存储记录同时可见；节点未全部就绪时不发布运行版本。
    """
    result = topology_service.deploy_operator_chain(entry_id)
    return result


@router.get("/task_topology/{entry_id}/runtime", response_model=dict)
def get_topology_runtime(entry_id: int) -> dict[str, Any]:
    """拓扑运行数据：当前版本快照、降级节点、历史版本。"""
    result = topology_service.runtime_data(entry_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"拓扑配置 {entry_id} 不存在或已归档")
    return result


@router.get("/topology-runtime", response_model=dict)
def list_topology_runtime() -> dict[str, Any]:
    """全部拓扑运行数据（供批量选择与核对）。"""
    items = topology_service.list_runtime_data()
    return {"items": items, "total": len(items)}


# ---- 吞吐管理：背压触发 + 降级状态清单 ----

@router.post("/throughput_mgr/backpressure", response_model=dict)
def trigger_backpressure(payload: BackpressurePayload) -> dict[str, Any]:
    """背压触发：读取部署后的拓扑运行数据，写入降级状态清单并保留拓扑版本。"""
    return throughput_service.trigger_backpressure_for_topology(
        payload.topology,
        payload.current_qps,
        mode=payload.mode,
    )


@router.get("/throughput_mgr/degradations", response_model=dict)
def list_degradations(
    topology: str | None = Query(default=None),
    version: str | None = Query(default=None),
) -> dict[str, Any]:
    """降级状态清单：按拓扑/版本核对受影响节点与限流状态。"""
    items = throughput_service.degradation_list(topology, version)
    return {"items": items, "total": len(items)}


@router.get("/throughput_mgr/topology_status/{topology_name}", response_model=dict)
def topology_throttle_status(topology_name: str) -> dict[str, Any]:
    """拓扑维度核对：当前版本、受影响节点与限流状态。"""
    return throughput_service.topology_throttle_status(topology_name)


# ---- 序号13：批量处理 ----

@router.post("/topology-batch", response_model=dict)
def submit_batch(payload: BatchPayload) -> dict[str, Any]:
    """序号13批量提交：一次提交保留整组结果，并发重试只接收一组版本。"""
    return batch_service.submit_batch(
        payload.topologies,
        idempotency_key=payload.idempotency_key,
        mode=payload.mode,
        initiator=payload.initiator,
    )


@router.get("/topology-batch", response_model=dict)
def list_batches() -> dict[str, Any]:
    """整组处理台账。"""
    items = batch_service.list_batches()
    return {"items": items, "total": len(items)}


@router.get("/topology-batch/{group_id}", response_model=dict)
def get_batch(group_id: str) -> dict[str, Any]:
    """批次详情：逐条成功失败与逐条版本。"""
    result = batch_service.get_batch(group_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"批量组 {group_id} 不存在")
    return result


@router.get("/topology-batch-verify/{topology_name}/{version}", response_model=dict)
def verify_by_version(topology_name: str, version: str) -> dict[str, Any]:
    """按拓扑版本核对拓扑运行数据与降级状态清单。"""
    return batch_service.verify_by_version(topology_name, version)
