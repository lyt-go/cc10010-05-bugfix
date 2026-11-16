"""任务编排业务规则：状态流转、字段校验与筛选口径都收在这里。

编排图的生命周期固定为「草稿 → 编排评审中 → 已发布 → 已停用」：
- 提交编排评审会冻结一份带版本的评审记录；
- 发布编排只从「编排评审中」推进，并且只依据评审记录里的拓扑写快照；
- 停用会撤销依赖解析清单里的投影，恢复必须先回到评审环节。
"""
from __future__ import annotations

from typing import Any

from app.store import store
from app.services import topology

MODULE = topology.DAG_MODULE
REQUIRED_FIELDS = ["图编号", "图名称", "拓扑结构"]
STATUS_ORDER = topology.STATUS_ORDER
ACTION_RULES = {
    "提交编排评审": topology.STATUS_REVIEWING,
    "发布编排": topology.STATUS_PUBLISHED,
    "停用编排": topology.STATUS_DISABLED,
    "恢复编排": topology.STATUS_REVIEWING,
}
NEGATIVE_ACTIONS = ["停用编排"]


class DagService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        topology.ensure_all_published()
        rows = [self.decorate_entry(row) for row in store.rows(MODULE)]
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("图编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        return self.decorate_entry(entry) if entry is not None else None

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str], list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing, []
        parsed = topology.parse_topology(values.get("拓扑结构"))
        if parsed["别名冲突"] or parsed["错误"]:
            return None, [], ["拓扑结构校验失败：" + "；".join(topology.diagnostics(parsed))]
        with topology.LOCK:
            rows = store.rows(MODULE)
            entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
            entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
            for field in topology.TOPOLOGY_FIELDS:
                if field not in entry:
                    entry[field] = values.get(field)
            start_count, parallelism = topology.derive_parallelism(parsed["节点"], parsed["依赖边"])
            if not str(entry.get("起始节点数") or "").strip():
                entry["起始节点数"] = start_count
            if not str(entry.get("最大并行度") or "").strip():
                entry["最大并行度"] = parallelism
            entry["status"] = topology.STATUS_DRAFT
            entry["发布状态"] = topology.PUBLISH_UNPUBLISHED
            entry["published_status"] = topology.PUBLISH_UNPUBLISHED
            entry["评审状态"] = topology.REVIEW_UNSUBMITTED
            entry["发布版本"] = 0
            entry["拓扑快照"] = None
            entry["节点"] = parsed["节点"]
            entry["依赖边"] = parsed["依赖边"]
            entry["pending"] = True
            entry["abnormal"] = False
            rows.append(entry)
            return entry, [], []

    def decorate_entry(self, entry: dict[str, Any]) -> dict[str, Any]:
        """把发布快照状态挂到编排图记录上，保证列表、详情与导出读同一份数据。"""
        latest = topology.latest_snapshot_for_entry(entry)
        if latest is not None:
            entry["拓扑快照"] = topology.snapshot_payload(latest)
            entry["published_status"] = latest.get("发布状态")
            entry["发布版本"] = latest.get("发布版本")
            entry["发布状态"] = latest.get("发布状态")
            entry["来源评审记录"] = latest.get("来源评审记录编号")
            if entry.get("status") == topology.STATUS_PUBLISHED:
                # 已发布编排图的字段只能来自快照，避免列表与发布详情各读一份。
                for field in topology.TOPOLOGY_FIELDS:
                    entry[field] = latest.get(field)
                entry["节点"] = list(latest.get("节点") or [])
                entry["依赖边"] = list(latest.get("依赖边") or [])
        else:
            entry.setdefault("发布状态", topology.PUBLISH_UNPUBLISHED)
            entry.setdefault("published_status", entry["发布状态"])
            entry["拓扑快照"] = None
        summary = entry["拓扑快照"] or {}
        entry["拓扑状态"] = {
            "编排状态": entry.get("status"),
            "发布状态": entry.get("发布状态"),
            "published_status": entry.get("published_status"),
            "已发布拓扑可用": entry.get("发布状态") == topology.PUBLISH_PUBLISHED and bool(summary),
            "发布版本": entry.get("发布版本"),
            "评审状态": entry.get("评审状态"),
            "评审版本": entry.get("评审版本"),
            "评审记录编号": entry.get("评审记录编号"),
            "快照编号": summary.get("快照编号"),
            "来源": summary.get("来源"),
            "拓扑指纹": summary.get("拓扑指纹"),
            "发布时间": summary.get("发布时间"),
            "失败传播策略": entry.get("失败传播策略"),
            "调度优先级": entry.get("调度优先级"),
        }
        return entry

    def run_action(self, entry_id: int, action: str, values: dict[str, Any] | None = None) -> tuple[dict[str, Any] | None, str]:
        values = values or {}
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"编排图 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于任务编排可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        if action in ("提交编排评审", "恢复编排"):
            result, message = self._submit(entry, values, resuming=action == "恢复编排")
        elif action == "发布编排":
            result, message = topology.publish(entry, values)
        else:
            result, message = topology.disable(entry, values)
        if result is None:
            return None, message
        result["pending"] = bool(result.get("pending"))
        result["abnormal"] = action in NEGATIVE_ACTIONS
        return self.decorate_entry(result), message

    def _submit(self, entry: dict[str, Any], values: dict[str, Any], *, resuming: bool) -> tuple[dict[str, Any] | None, str]:
        status = entry.get("status")
        if resuming and status != topology.STATUS_DISABLED:
            return None, f"编排图当前状态为「{status}」，只有「{topology.STATUS_DISABLED}」可以恢复编排"
        if status == topology.STATUS_DISABLED and not resuming:
            return None, "编排图已停用，请使用「恢复编排」重新进入编排评审"
        result, message = topology.submit_review(entry, values)
        if result is None:
            return None, message
        if resuming:
            message = "编排图已恢复编排并切换回编排评审，需重新发布后才会回写依赖解析清单"
        return result, message

    def reviews(self, entry_id: int) -> list[dict[str, Any]]:
        rows = [
            topology.review_payload(row)
            for row in topology.review_rows(entry_id)
        ]
        return sorted(rows, key=lambda row: int(row.get("评审版本", 0)), reverse=True)

    def publish_detail(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        entry = self.decorate_entry(entry)
        snapshot = topology.latest_snapshot_for_entry(entry)
        return {
            "entry": entry,
            "publish_status": entry.get("发布状态"),
            "topology": topology.snapshot_payload(snapshot),
            "reviews": self.reviews(entry_id),
            "projection": topology.projection_for(entry_id),
        }
