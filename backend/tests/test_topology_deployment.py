"""拓扑部署 ↔ 吞吐降级 ↔ 状态存储联动测试（含序号13批量处理）。

测试用例：
  - 部署算子链：读取拓扑配置、发布拓扑运行数据、联动状态存储同时可见
  - 节点未全部就绪时不发布运行版本
  - 同一拓扑重复部署只保留一个当前运行快照（含并发部署）
  - 背压触发写入降级状态清单并保留拓扑版本，拓扑页面可见降级节点
  - 局部降级 / 统一降级两种取舍
  - 序号13：整组结果、逐条版本、幂等重试、并发只记一次、事务异常回滚整组
"""
from __future__ import annotations

import concurrent.futures
import itertools
import time

import pytest

from app.services.task_topology import TaskTopologyService
from app.services.throughput_mgr import ThroughputMgrService
from app.services.topology_batch import TopologyBatchService
from app.store import store

# 种子数据中存在的可就绪节点
READY_SOURCE = "流式源端样例1"
READY_OPERATOR = "计算算子样例1"
READY_INDEX = "滑动指标样例1"

_counter = itertools.count(1)


def _uid(prefix: str) -> str:
    return f"{prefix}_{next(_counter)}_{int(time.time() * 1000) % 100000}"


def _make_topology(name: str, chain: str) -> dict:
    """直接登记一条拓扑配置，返回行记录。"""
    rows = store.rows("task_topology")
    row = {
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "拓扑名称": name,
        "算子链": chain,
        "并行度": "4",
        "数据分发": "hash",
        "故障转移": "restart",
        "资源配额": "cpu:4,mem:8G",
        "运行状态": "未部署",
        "status": "草稿",
        "pending": True,
        "abnormal": False,
    }
    rows.append(row)
    return row


def _make_throughput(source: str, max_qps: str = "1000", threshold: str = "0.75") -> dict:
    rows = store.rows("throughput_mgr")
    row = {
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "配置名称": _uid("吞吐配置"),
        "数据源": source,
        "最大QPS": max_qps,
        "并发分区": "4",
        "背压阈值": threshold,
        "限流策略": "token_bucket",
        "扩容规则": "double",
        "status": "已生效",
        "pending": True,
        "abnormal": False,
    }
    rows.append(row)
    return row


def _state_linkage_rows(topology_name: str) -> list[dict]:
    return [
        r for r in store.rows("state_store")
        if r.get("_link_type") == "topology_runtime" and r.get("_topology") == topology_name
    ]


# ---------------------------------------------------------------------------
# 部署算子链：拓扑运行数据 ↔ 状态存储联动
# ---------------------------------------------------------------------------

class TestOperatorChainDeploy:
    """部署算子链：发布拓扑运行数据并联动状态存储。"""

    def test_deploy_publishes_runtime_and_state_together(self):
        """部署成功与状态记录必须同时可见。"""
        service = TaskTopologyService()
        name = _uid("拓扑A")
        row = _make_topology(name, f"{READY_SOURCE}→{READY_OPERATOR}→{READY_INDEX}")

        result = service.deploy_operator_chain(row["id"])
        assert result["ok"] is True
        assert result["published"] is True
        assert result["version"] == "v1"

        # 拓扑运行数据可见
        current = store.current_runtime(name)
        assert current is not None
        assert current.is_current is True
        assert current.version == "v1"
        assert len(current.node_states) == 3
        assert all(ns["ready"] for ns in current.node_states)

        # 状态存储联动记录同时可见
        linkage = _state_linkage_rows(name)
        assert len(linkage) == 1
        assert linkage[0]["status"] == "已生效"
        assert linkage[0]["_topology_version"] == "v1"
        assert linkage[0]["id"] == current.state_entry_id

        # 拓扑行运行状态同步更新
        assert row["status"] == "运行中"
        assert "v1" in row["运行状态"]

    def test_deploy_blocked_when_nodes_not_ready(self):
        """节点未全部就绪时不能发布运行版本。"""
        service = TaskTopologyService()
        name = _uid("拓扑B")
        row = _make_topology(name, f"{READY_SOURCE}→不存在的算子节点")

        result = service.deploy_operator_chain(row["id"])
        assert result["ok"] is False
        assert result["published"] is False
        assert "未全部就绪" in result["error"]
        # 逐节点结果可见
        assert len(result["nodes"]) == 2
        assert result["nodes"][0]["ready"] is True
        assert result["nodes"][1]["ready"] is False
        # 未发布运行版本，也无状态记录
        assert store.current_runtime(name) is None
        assert _state_linkage_rows(name) == []
        assert row["status"] == "草稿"

    def test_repeated_deploy_keeps_single_current_snapshot(self):
        """同一拓扑重复部署只能保留一个当前运行快照。"""
        service = TaskTopologyService()
        name = _uid("拓扑C")
        row = _make_topology(name, f"{READY_SOURCE}→{READY_OPERATOR}")

        r1 = service.deploy_operator_chain(row["id"])
        r2 = service.deploy_operator_chain(row["id"])
        assert r1["ok"] and r2["ok"]
        assert r2["version"] == "v2"

        history = store.runtime_history(name)
        assert len(history) == 2
        currents = [s for s in history if s.is_current]
        assert len(currents) == 1
        assert currents[0].version == "v2"
        assert history[0].status == "已取代"

        # 状态记录也只保留一个当前生效
        linkage = _state_linkage_rows(name)
        active = [r for r in linkage if r["status"] == "已生效"]
        assert len(active) == 1
        assert active[0]["_topology_version"] == "v2"

    def test_concurrent_deploy_single_current(self):
        """并发部署同一拓扑：只产生一个当前运行快照。"""
        service = TaskTopologyService()
        name = _uid("拓扑D")
        row = _make_topology(name, f"{READY_SOURCE}→{READY_OPERATOR}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(
                lambda _: service.deploy_operator_chain(row["id"]), range(8)
            ))
        assert all(r["ok"] for r in results)
        history = store.runtime_history(name)
        currents = [s for s in history if s.is_current]
        assert len(currents) == 1
        # 版本号无重复、连续递增
        versions = sorted(int(s.version[1:]) for s in history)
        assert versions == list(range(1, len(history) + 1))


# ---------------------------------------------------------------------------
# 背压触发 → 降级状态清单
# ---------------------------------------------------------------------------

class TestBackpressureDegradationList:
    """背压触发时写入降级状态清单并保留拓扑版本。"""

    def _deployed_topology(self, prefix: str) -> tuple[dict, str]:
        service = TaskTopologyService()
        name = _uid(prefix)
        row = _make_topology(name, f"{READY_SOURCE}→{READY_OPERATOR}→{READY_INDEX}")
        result = service.deploy_operator_chain(row["id"])
        assert result["ok"] is True
        return row, name

    def test_backpressure_writes_degradation_list_with_version(self):
        """降级状态清单保留拓扑版本，拓扑页面可判断降级节点。"""
        row, name = self._deployed_topology("拓扑E")
        _make_throughput(READY_SOURCE, max_qps="1000", threshold="0.75")
        service = ThroughputMgrService()

        result = service.trigger_backpressure_for_topology(name, current_qps=900)
        assert result["ok"] is True
        assert result["triggered"] is True
        assert result["version"] == "v1"
        assert result["throttle_limit"] == pytest.approx(700)
        assert len(result["entries"]) == 3

        # 清单条目保留拓扑版本
        entries = store.degradations(name, "v1")
        assert len(entries) == 3
        assert all(e.topology_version == "v1" for e in entries)
        assert all(e.status == "生效中" for e in entries)

        # 拓扑页面可见哪些节点已进入降级状态
        topo_service = TaskTopologyService()
        runtime = topo_service.runtime_data(row["id"])
        degraded = runtime["degraded_nodes"]
        assert len(degraded) == 3
        assert {d["node"] for d in degraded} == {READY_SOURCE, READY_OPERATOR, READY_INDEX}
        assert all(d["throttle_limit"] == pytest.approx(700) for d in degraded)

    def test_backpressure_requires_deployed_version(self):
        """未部署运行版本的拓扑不能生成降级清单。"""
        service = ThroughputMgrService()
        result = service.trigger_backpressure_for_topology(_uid("未部署拓扑"))
        assert result["ok"] is False
        assert "尚未部署运行版本" in result["error"]

    def test_no_trigger_below_threshold(self):
        """负载未超阈值时不写入降级清单。"""
        _, name = self._deployed_topology("拓扑F")
        _make_throughput(READY_SOURCE, max_qps="1000", threshold="0.75")
        service = ThroughputMgrService()
        before = len(store.degradations(name))
        result = service.trigger_backpressure_for_topology(name, current_qps=500)
        assert result["triggered"] is False
        assert len(store.degradations(name)) == before

    def test_partial_vs_unified_degradation(self):
        """局部降级按已部署节点生成；统一降级要求整条拓扑就绪。"""
        _, name = self._deployed_topology("拓扑G")
        _make_throughput(READY_SOURCE)
        service = ThroughputMgrService()

        partial = service.trigger_backpressure_for_topology(name, current_qps=900, mode="partial")
        assert partial["ok"] is True
        assert partial["mode"] == "partial"
        assert len(partial["affected_nodes"]) == 3

        unified = service.trigger_backpressure_for_topology(name, current_qps=950, mode="unified")
        assert unified["ok"] is True
        assert unified["mode"] == "unified"
        assert len(unified["affected_nodes"]) == 3

    def test_redeploy_supersedes_old_degradations(self):
        """重新部署后旧版本降级条目解除，清单只呈现一个可执行拓扑版本。"""
        row, name = self._deployed_topology("拓扑H")
        _make_throughput(READY_SOURCE)
        tp_service = ThroughputMgrService()
        tp_service.trigger_backpressure_for_topology(name, current_qps=900)
        assert len([e for e in store.degradations(name) if e.status == "生效中"]) == 3

        topo_service = TaskTopologyService()
        topo_service.deploy_operator_chain(row["id"])  # v2
        active = [e for e in store.degradations(name) if e.status == "生效中"]
        assert active == []
        released = [e for e in store.degradations(name) if e.status == "已解除"]
        assert len(released) == 3
        # 拓扑页面不再显示降级节点
        assert store.degraded_nodes(name) == []


# ---------------------------------------------------------------------------
# 序号13：批量处理
# ---------------------------------------------------------------------------

class TestTopologyBatch:
    """序号13：整组结果、幂等、并发、回滚。"""

    def _make_deployable(self, prefix: str) -> str:
        name = _uid(prefix)
        _make_topology(name, f"{READY_SOURCE}→{READY_OPERATOR}")
        return name

    def test_batch_group_and_item_versions(self):
        """一次提交保留整组结果：组级版本 + 逐条版本。"""
        batch = TopologyBatchService()
        names = [self._make_deployable("批量甲"), self._make_deployable("批量乙")]
        result = batch.submit_batch(names, idempotency_key=_uid("key"))
        assert result["ok"] is True
        group = result["group"]
        assert group["status"] == "已完成"
        assert group["group_version"].startswith("G")
        assert group["success_count"] == 2
        # 逐条结果带逐条版本与拓扑版本
        for idx, item in enumerate(group["items"], 1):
            assert item["ok"] is True
            assert item["item_version"] == f"{group['group_version']}.{idx}"
            assert item["version"] == "v1"
            assert item["runtime_id"]
        # 台账可查询
        detail = batch.get_batch(group["group_id"])
        assert detail is not None
        assert detail["status"] == "已完成"

    def test_batch_idempotent_retry_single_version_set(self):
        """并发重试只接收一组版本：同一幂等键复用整组结果。"""
        batch = TopologyBatchService()
        names = [self._make_deployable("幂等甲")]
        key = _uid("key")

        first = batch.submit_batch(names, idempotency_key=key)
        second = batch.submit_batch(names, idempotency_key=key)
        assert first["deduplicated"] is False
        assert second["deduplicated"] is True
        assert second["group"]["group_id"] == first["group"]["group_id"]
        # 版本未被重复发布：仍只有 v1 一个当前快照
        history = store.runtime_history(names[0])
        assert len(history) == 1
        assert history[0].version == "v1"

    def test_batch_concurrent_submit_single_group(self):
        """序号13并发操作只产生一组结果。"""
        batch = TopologyBatchService()
        names = [self._make_deployable("并发甲"), self._make_deployable("并发乙")]
        key = _uid("key")

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(
                lambda _: batch.submit_batch(names, idempotency_key=key), range(8)
            ))
        group_ids = {r["group"]["group_id"] for r in results}
        assert len(group_ids) == 1
        created = [r for r in results if not r["deduplicated"]]
        assert len(created) == 1
        # 台账中该幂等键只记一次
        ledger = [g for g in batch.list_batches() if g["idempotency_key"] == key]
        assert len(ledger) == 1
        # 每条拓扑只保留一个当前运行快照
        for name in names:
            currents = [s for s in store.runtime_history(name) if s.is_current]
            assert len(currents) == 1

    def test_batch_rollback_on_transaction_error(self):
        """事务异常后回滚整组：运行快照与状态记录一并撤回。"""
        topo_service = TaskTopologyService()
        batch = TopologyBatchService(topology_service=topo_service)
        good1 = self._make_deployable("回滚甲")
        good2 = self._make_deployable("回滚乙")

        original = topo_service.deploy_by_name
        calls = {"n": 0}

        def flaky(name, **kwargs):
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("模拟事务异常")
            return original(name, **kwargs)

        topo_service.deploy_by_name = flaky  # type: ignore[assignment]
        try:
            result = batch.submit_batch([good1, good2], idempotency_key=_uid("key"))
        finally:
            topo_service.deploy_by_name = original  # type: ignore[assignment]

        assert result["ok"] is False
        assert result["rolled_back"] is True
        group = result["group"]
        assert group["status"] == "已回滚"
        assert group["rolled_back"] is True
        # 第一条已发布的运行快照被撤回
        assert store.current_runtime(good1) is None
        assert store.runtime_history(good1) == []
        # 联动的状态记录一并撤回
        assert _state_linkage_rows(good1) == []

    def test_batch_rollback_restores_previous_current(self):
        """回滚后恢复部署前的当前版本。"""
        topo_service = TaskTopologyService()
        batch = TopologyBatchService(topology_service=topo_service)
        name = self._make_deployable("回滚丙")
        other = self._make_deployable("回滚丁")
        # 先手工部署 v1
        row = next(r for r in store.rows("task_topology") if r["拓扑名称"] == name)
        manual = topo_service.deploy_operator_chain(row["id"])
        assert manual["version"] == "v1"

        original = topo_service.deploy_by_name

        def flaky(target, **kwargs):
            if target == other:
                raise RuntimeError("模拟事务异常")
            return original(target, **kwargs)

        topo_service.deploy_by_name = flaky  # type: ignore[assignment]
        try:
            result = batch.submit_batch([name, other], idempotency_key=_uid("key"))
        finally:
            topo_service.deploy_by_name = original  # type: ignore[assignment]
        assert result["rolled_back"] is True
        # 批量发布的 v2 被撤回，v1 恢复为当前版本
        current = store.current_runtime(name)
        assert current is not None
        assert current.version == "v1"
        linkage = [r for r in _state_linkage_rows(name) if r["status"] == "已生效"]
        assert len(linkage) == 1
        assert linkage[0]["_topology_version"] == "v1"

    def test_batch_partial_failure_no_rollback(self):
        """逐条业务失败记入详情，整组继续，不回滚。"""
        batch = TopologyBatchService()
        good = self._make_deployable("部分甲")
        bad = _uid("不存在拓扑")
        result = batch.submit_batch([good, bad], idempotency_key=_uid("key"))
        group = result["group"]
        assert group["status"] == "部分失败"
        assert group["success_count"] == 1
        assert group["fail_count"] == 1
        items = {it["topology"]: it for it in group["items"]}
        assert items[good]["ok"] is True
        assert items[bad]["ok"] is False
        # 成功条目的快照保留，不回滚
        assert store.current_runtime(good) is not None

    def test_verify_by_version(self):
        """批量结果页按拓扑版本核对运行数据与降级清单。"""
        batch = TopologyBatchService()
        name = self._make_deployable("核对甲")
        _make_throughput(READY_SOURCE)
        submit = batch.submit_batch([name], idempotency_key=_uid("key"))
        assert submit["ok"] is True
        tp = ThroughputMgrService()
        tp.trigger_backpressure_for_topology(name, current_qps=900)

        check = batch.verify_by_version(name, "v1")
        assert check["matched"] is True
        assert check["runtime"]["version"] == "v1"
        assert check["runtime"]["group_id"] == submit["group"]["group_id"]
        assert len(check["degradations"]) == 2
        missing = batch.verify_by_version(name, "v99")
        assert missing["matched"] is False
