"""任务编排接口：维护编排图，覆盖提交编排评审、发布编排、停用编排等动作。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.dag import DagService

router = APIRouter(prefix="/api/dag", tags=["任务编排"])

service = DagService()

LIST_FIELDS = ["图编号", "图名称", "拓扑结构", "起始节点数", "最大并行度", "失败传播策略", "调度优先级", "生效时间范围"]
STATUSES = ["草稿", "编排评审中", "已发布", "已停用"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按图编号检索"),
    status: str | None = Query(default=None, description="草稿、编排评审中、已发布、已停用"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按图编号与状态过滤任务编排列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出任务编排清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "dag", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条编排图明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"编排图 {entry_id} 不存在或已归档")
    return entry


@router.get("/{entry_id}/reviews", response_model=dict)
def get_reviews(entry_id: int) -> dict:
    """读取编排图的评审记录，按评审版本从新到旧排列。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"编排图 {entry_id} 不存在或已归档")
    return {"entry_id": entry_id, "items": service.reviews(entry_id)}


@router.get("/{entry_id}/publish", response_model=dict)
def get_publish_detail(entry_id: int) -> dict:
    """发布详情：编排图、已发布拓扑快照、评审记录与依赖解析投影使用同一份快照。"""
    detail = service.publish_detail(entry_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"编排图 {entry_id} 不存在或已归档")
    return detail


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条编排图，缺字段时说明原因而不是静默丢弃。"""
    entry, missing, errors = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    if errors:
        return ActionResult(ok=False, message="；".join(errors))
    return ActionResult(ok=True, message="编排图已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条编排图执行提交编排评审、发布编排、停用编排、恢复编排；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action, payload.values)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
