"""依赖解析业务规则：状态流转、字段校验与筛选口径都收在这里。"""
from __future__ import annotations

from typing import Any

from app.services import topology
from app.store import store

MODULE = "dependency"
REQUIRED_FIELDS = ["关系编号", "上游作业", "下游作业"]
STATUS_ORDER = ["草稿", "关系评审中", "已生效", "已解除"]
ACTION_RULES = {"提交关系评审": "关系评审中", "生效关系": "已生效", "解除关系": "已解除"}
NEGATIVE_ACTIONS = []


class DependencyService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        source: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        topology.ensure_all_published()
        rows = [self.decorate_row(row) for row in store.rows(MODULE)]
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("关系编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        if source:
            rows = [row for row in rows if str(row.get("来源模块") or "手工登记") == source]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        return self.decorate_row(entry) if entry is not None else None

    def decorate_row(self, entry: dict[str, Any]) -> dict[str, Any]:
        """把编排图当前发布状态同步到投影记录，避免清单残留旧拓扑或旧发布状态。"""
        dag_id = int(entry.get("来源编排图ID") or 0)
        if not dag_id:
            return entry
        snapshot = topology.published_snapshot(dag_id)
        if snapshot is None:
            display = topology.current_snapshot(dag_id)
            state = (display or {}).get("发布状态") or topology.PUBLISH_UNPUBLISHED
            if display is not None and display.get("发布状态") == topology.PUBLISH_PUBLISHED:
                state = topology.PUBLISH_UNPUBLISHED
            self._clear_topology(entry, state)
            return entry
        if entry.get("快照指纹") != snapshot.get("拓扑指纹"):
            # 发布版本已更新，投影记录不能再拿旧拓扑充数。
            self._clear_topology(entry, topology.PUBLISH_SUPERSEDED)
            return entry
        entry["发布状态"] = snapshot.get("发布状态")
        entry["拓扑可用"] = snapshot.get("发布状态") == topology.PUBLISH_PUBLISHED
        entry["来源发布版本"] = snapshot.get("发布版本")
        entry["来源评审版本"] = snapshot.get("来源评审版本")
        entry["来源评审记录ID"] = snapshot.get("来源评审记录编号")
        entry["拓扑结构"] = snapshot.get("拓扑结构")
        entry["失败传播策略"] = snapshot.get("失败传播策略")
        entry["调度优先级"] = snapshot.get("调度优先级")
        entry["拓扑节点"] = list(snapshot.get("节点") or [])
        entry["拓扑依赖边"] = list(snapshot.get("依赖边") or [])
        entry["拓扑快照"] = topology.snapshot_payload(snapshot)
        return entry

    def _clear_topology(self, entry: dict[str, Any], state: str) -> None:
        entry["发布状态"] = state
        entry["拓扑可用"] = False
        entry["拓扑结构"] = None
        entry["失败传播策略"] = None
        entry["调度优先级"] = None
        entry["拓扑节点"] = []
        entry["拓扑依赖边"] = []
        entry["拓扑快照"] = None

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"依赖关系 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于依赖解析可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        if entry.get("来源模块") == topology.PROJECTION_SOURCE and target != "已解除":
            return None, "该依赖关系由任务编排发布回写，不能单独生效；请在任务编排中发布或停用编排图"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return self.decorate_row(entry), f"依赖关系已{action}"
