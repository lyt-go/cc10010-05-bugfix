"""编排图评审、发布与拓扑快照的共享规则。

任务编排的生命周期固定为：草稿 → 编排评审中 → 已发布 → 已停用。
发布只能从「编排评审中」推进，且发布依据是评审记录里冻结的那份拓扑，
而不是编排图当前字段。每次发布都会写出一份带来源（评审记录编号、发布版本、
快照指纹）的完整拓扑数据快照，并把它投影到依赖解析清单。

依赖解析只读取「发布状态=已发布」且仍有效的快照；停用会撤销投影，
恢复必须重新回到评审环节，不能绕过状态变更。
"""
from __future__ import annotations

import copy
import hashlib
import json
import threading
from datetime import datetime
from typing import Any

from app.store import store

DAG_MODULE = "dag"
DEPENDENCY_MODULE = "dependency"

STATUS_DRAFT = "草稿"
STATUS_REVIEWING = "编排评审中"
STATUS_PUBLISHED = "已发布"
STATUS_DISABLED = "已停用"
STATUS_ORDER = [STATUS_DRAFT, STATUS_REVIEWING, STATUS_PUBLISHED, STATUS_DISABLED]

PUBLISH_UNPUBLISHED = "未发布"
PUBLISH_PUBLISHED = "已发布"
PUBLISH_DISABLED = "已停用"
PUBLISH_SUPERSEDED = "已失效"

REVIEW_UNSUBMITTED = "未提交"
REVIEW_PENDING = "待发布"
REVIEW_PUBLISHED = "已发布"
REVIEW_SUPERSEDED = "已被替代"

TOPOLOGY_FIELDS = ["拓扑结构", "起始节点数", "最大并行度", "失败传播策略", "调度优先级", "生效时间范围"]
REQUIRED_PUBLISH_FIELDS = ["拓扑结构", "失败传播策略", "调度优先级"]

REVIEW_SOURCE = "编排图评审记录"
LEGACY_SOURCE = "历史编排图"
PROJECTION_SOURCE = "任务编排发布"
PROJECTION_DEPENDENCY_TYPE = "编排拓扑"

REVIEW_TABLE = "dag_reviews"
SNAPSHOT_TABLE = "dag_topology_snapshots"

NODE_LIST_KEYS = ("节点", "节点列表", "nodes", "node_list", "nodeList")
EDGE_LIST_KEYS = ("依赖边", "依赖关系", "依赖", "连线", "边", "edges", "links")
ALIAS_KEYS = ("节点别名", "别名", "alias", "node_alias", "nodeAlias", "节点名称", "name", "节点", "id")
UPSTREAM_KEYS = ("上游", "上游节点", "上游作业", "from", "source", "src", "parent")
DOWNSTREAM_KEYS = ("下游", "下游节点", "下游作业", "to", "target", "dst", "child")

FIELD_ALIASES = {
    "拓扑结构": ("拓扑结构", "拓扑", "topology", "structure"),
    "起始节点数": ("起始节点数", "起始节点", "start_nodes", "startNodes"),
    "最大并行度": ("最大并行度", "并行度", "max_parallel", "maxParallel", "parallelism"),
    "失败传播策略": ("失败传播策略", "失败传播", "失败策略", "failure_policy", "failurePolicy", "on_failure"),
    "调度优先级": ("调度优先级", "优先级", "schedule_priority", "schedulePriority", "priority"),
    "生效时间范围": ("生效时间范围", "生效时间", "effective_range", "effectiveRange"),
}

VERSION_KEYS = ("expected_version", "expectedVersion", "评审版本", "version", "预期版本")
LOCK = threading.RLock()


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return str(value).strip()


def _alias_key(value: Any) -> str:
    return " ".join(_text(value).split()).casefold()


def _get_key(mapping: Any, keys: tuple[str, ...]) -> tuple[bool, Any]:
    if not isinstance(mapping, dict):
        return False, None
    lowered = {str(key).strip().casefold(): key for key in mapping}
    for key in keys:
        if key in mapping:
            return True, mapping[key]
        actual = lowered.get(key.casefold())
        if actual is not None:
            return True, mapping[actual]
    return False, None


def _first_value(mapping: Any, keys: tuple[str, ...]) -> Any:
    found, value = _get_key(mapping, keys)
    return value if found else None


def _split_by_any(text: str, separators: tuple[str, ...]) -> list[str]:
    parts = [text]
    for separator in separators:
        next_parts: list[str] = []
        for part in parts:
            next_parts.extend(part.split(separator))
        parts = next_parts
    return parts


def _split_edge(text: str) -> tuple[str, str] | None:
    for arrow in ("->", "→", "=>", "⟶", "⇒"):
        if arrow in text:
            left, right = text.split(arrow, 1)
            return left.strip(), right.strip()
    return None


def _node_alias(item: Any) -> str:
    if isinstance(item, dict):
        value = _first_value(item, ALIAS_KEYS)
        return _text(value)
    if isinstance(item, (list, tuple)) and item:
        return _text(item[0])
    return _text(item)


def _edge_endpoints(item: Any) -> tuple[str, str] | None:
    if isinstance(item, dict):
        upstream = _first_value(item, UPSTREAM_KEYS)
        downstream = _first_value(item, DOWNSTREAM_KEYS)
        if upstream is None or downstream is None:
            return None
        return _text(upstream), _text(downstream)
    if isinstance(item, (list, tuple)) and len(item) >= 2:
        return _text(item[0]), _text(item[1])
    if isinstance(item, str):
        return _split_edge(item.strip())
    return None


def _try_json(text: str) -> tuple[Any, bool, str]:
    stripped = text.strip()
    if not stripped:
        return None, False, ""
    if stripped[0] in "{[\\\"":
        try:
            return json.loads(stripped), True, ""
        except Exception:
            return None, True, "拓扑结构不是合法的 JSON"
    return None, False, ""


def _normalize_nodes(raw_nodes: Any) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    nodes: list[dict[str, Any]] = []
    seen: dict[str, str] = {}
    conflicts: list[str] = []
    errors: list[str] = []
    if raw_nodes is None:
        return nodes, conflicts, errors
    if isinstance(raw_nodes, (str, dict)):
        raw_nodes = [raw_nodes]
    if not isinstance(raw_nodes, list):
        return nodes, conflicts, ["拓扑结构里的节点列表格式不正确"]
    for item in raw_nodes:
        alias = _node_alias(item)
        key = _alias_key(alias)
        if not key:
            errors.append("存在节点别名为空")
            continue
        if key in seen:
            conflicts.append(alias)
            continue
        node: dict[str, Any] = {"别名": alias}
        if isinstance(item, dict):
            for raw_key, raw_value in item.items():
                if _alias_key(raw_key) in {_alias_key(one) for one in ALIAS_KEYS}:
                    continue
                node[str(raw_key)] = copy.deepcopy(raw_value)
        nodes.append(node)
        seen[key] = alias
    return nodes, conflicts, errors


def _normalize_edges(raw_edges: Any) -> tuple[list[dict[str, str]], list[str]]:
    edges: list[dict[str, str]] = []
    errors: list[str] = []
    if raw_edges is None:
        return edges, errors
    if isinstance(raw_edges, (str, dict)):
        raw_edges = [raw_edges]
    if not isinstance(raw_edges, list):
        return edges, ["拓扑结构里的依赖边列表格式不正确"]
    seen: set[tuple[str, str]] = set()
    for item in raw_edges:
        endpoints = _edge_endpoints(item)
        if endpoints is None:
            errors.append("存在依赖边缺少上游或下游节点")
            continue
        upstream, downstream = endpoints
        if not upstream or not downstream:
            errors.append("存在依赖边的上游或下游节点别名为空")
            continue
        key = (_alias_key(upstream), _alias_key(downstream))
        if key in seen:
            continue
        seen.add(key)
        edges.append({"上游": upstream, "下游": downstream})
    return edges, errors


def parse_topology(raw: Any) -> dict[str, Any]:
    """把不同形态的拓扑结构统一解析成节点与依赖边。

    支持 JSON 对象/数组、``A->B`` 文本、换行或分号分隔的节点清单，
    解析结果保留原始拓扑文本，保证发布快照与消费方读到的是同一份数据。
    """
    parsed: dict[str, Any] = {
        "源数据": None,
        "节点": [],
        "依赖边": [],
        "别名冲突": [],
        "错误": [],
    }
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        parsed["错误"].append("拓扑结构不能为空")
        return parsed

    source: Any = raw
    if isinstance(raw, str):
        value, is_json, error = _try_json(raw)
        if error:
            parsed["错误"].append(error)
            return parsed
        source = value if is_json else raw

    parsed["源数据"] = source
    raw_nodes: Any = None
    raw_edges: Any = None
    if isinstance(source, dict):
        found_nodes, raw_nodes = _get_key(source, NODE_LIST_KEYS)
        found_edges, raw_edges = _get_key(source, EDGE_LIST_KEYS)
        if not found_nodes and not found_edges:
            # 没有显式的节点/边字段时，如果对象形如 {上游: [下游...]} 也当作邻接表。
            adjacency = {
                key: value
                for key, value in source.items()
                if isinstance(value, (list, tuple))
            }
            if adjacency:
                raw_edges = []
                for upstream, downstream in adjacency.items():
                    targets = downstream if isinstance(downstream, (list, tuple)) else [downstream]
                    for target in targets:
                        raw_edges.append({"上游": str(upstream), "下游": target})
            else:
                parsed["错误"].append("拓扑结构缺少节点或依赖边字段")
                return parsed
    elif isinstance(source, list):
        if source and all(isinstance(item, dict) and _edge_endpoints(item) for item in source):
            raw_edges = source
        else:
            raw_nodes = source
    elif isinstance(source, str):
        raw_nodes = []
        raw_edges = []
        for line in source.splitlines():
            for segment in _split_by_any(line, (";", "；", "|", "，", ",")):
                cleaned = segment.strip()
                if not cleaned:
                    continue
                endpoints = _split_edge(cleaned)
                if endpoints is None:
                    raw_nodes.append(cleaned)
                else:
                    raw_edges.append({"上游": endpoints[0], "下游": endpoints[1]})
    else:
        parsed["错误"].append("拓扑结构格式不正确")
        return parsed

    nodes, conflicts, node_errors = _normalize_nodes(raw_nodes)
    edges, edge_errors = _normalize_edges(raw_edges)
    parsed["节点"] = nodes
    parsed["依赖边"] = edges
    parsed["别名冲突"] = conflicts
    parsed["错误"].extend(node_errors + edge_errors)

    known = {_alias_key(node["别名"]) for node in nodes}
    for edge in edges:
        for alias in (edge["上游"], edge["下游"]):
            key = _alias_key(alias)
            if key not in known:
                nodes.append({"别名": alias})
                known.add(key)
    if not nodes and not parsed["错误"]:
        parsed["错误"].append("拓扑结构不包含任何节点")
    return parsed


def topology_text(raw: Any) -> str:
    if isinstance(raw, str):
        return raw.strip()
    return _text(raw)


def extract_field(source: Any, field: str) -> Any:
    return _first_value(source, FIELD_ALIASES.get(field, (field,)))


def derive_parallelism(nodes: list[dict[str, Any]], edges: list[dict[str, str]]) -> tuple[int, int]:
    aliases = [node["别名"] for node in nodes]
    incoming = {alias: 0 for alias in aliases}
    outgoing: dict[str, list[str]] = {alias: [] for alias in aliases}
    for edge in edges:
        if edge["上游"] in incoming and edge["下游"] in incoming:
            incoming[edge["下游"]] += 1
            outgoing[edge["上游"]].append(edge["下游"])
    starts = [alias for alias in aliases if incoming[alias] == 0]
    level = {alias: 1 for alias in aliases}
    queue = list(starts)
    visited = 0
    while queue and visited <= len(aliases) * len(aliases) + 1:
        current = queue.pop(0)
        visited += 1
        for child in outgoing.get(current, []):
            level[child] = max(level[child], level[current] + 1)
            queue.append(child)
    widths: dict[int, int] = {}
    for value in level.values():
        widths[value] = widths.get(value, 0) + 1
    return len(starts), max(widths.values(), default=0)


def _fields_from_sources(values: dict[str, Any], entry: dict[str, Any], parsed: dict[str, Any]) -> dict[str, Any]:
    source_data = parsed.get("源数据")
    fields: dict[str, Any] = {}
    for field in TOPOLOGY_FIELDS:
        candidate = values.get(field)
        if _text(candidate) == "":
            candidate = extract_field(source_data, field)
        if _text(candidate) == "":
            candidate = entry.get(field)
        fields[field] = candidate
    for field in ("图编号", "图名称"):
        candidate = values.get(field)
        if _text(candidate) == "":
            candidate = entry.get(field)
        fields[field] = candidate
    start_count, parallelism = derive_parallelism(parsed["节点"], parsed["依赖边"])
    if _text(fields.get("起始节点数")) == "":
        fields["起始节点数"] = start_count
    if _text(fields.get("最大并行度")) == "":
        fields["最大并行度"] = parallelism
    return fields


def fingerprint(topology_value: str, nodes: list[dict[str, Any]], edges: list[dict[str, str]], fields: dict[str, Any]) -> str:
    payload = {
        "拓扑结构": topology_value,
        "节点": nodes,
        "依赖边": edges,
        "字段": {field: _text(fields.get(field)) for field in TOPOLOGY_FIELDS if field != "拓扑结构"},
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def diagnostics(parsed: dict[str, Any]) -> list[str]:
    errors = list(parsed.get("错误") or [])
    conflicts = parsed.get("别名冲突") or []
    if conflicts:
        errors.append("节点别名冲突：" + "、".join(str(alias) for alias in conflicts))
    return errors


def validate(parsed: dict[str, Any], fields: dict[str, Any]) -> list[str]:
    errors = diagnostics(parsed)
    for field in REQUIRED_PUBLISH_FIELDS:
        if _text(fields.get(field)) == "":
            errors.append(f"缺少必填字段：{field}")
    return errors


# ---------------------------------------------------------------------------
# 评审记录：编排图每次提交评审都冻结一份拓扑，发布只能使用待发布的那一份。
# ---------------------------------------------------------------------------

def review_rows(dag_id: int) -> list[dict[str, Any]]:
    return [row for row in store.aux_rows(REVIEW_TABLE) if int(row.get("编排图ID", 0)) == dag_id]


def _latest_review(dag_id: int) -> dict[str, Any] | None:
    rows = [row for row in review_rows(dag_id) if row.get("评审状态") != REVIEW_SUPERSEDED]
    if not rows:
        return None
    return max(rows, key=lambda row: int(row.get("评审版本", 0)))


def _next_review_id() -> int:
    rows = store.aux_rows(REVIEW_TABLE)
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


def _next_snapshot_id() -> int:
    rows = store.aux_rows(SNAPSHOT_TABLE)
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


def review_payload(review: dict[str, Any]) -> dict[str, Any]:
    return {
        "评审记录编号": review.get("id"),
        "编排图ID": review.get("编排图ID"),
        "评审版本": review.get("评审版本"),
        "评审状态": review.get("评审状态"),
        "提交时间": review.get("提交时间"),
        "评审人": review.get("评审人"),
        "备注": review.get("备注"),
        "拓扑结构": review.get("拓扑结构"),
        "起始节点数": review.get("起始节点数"),
        "最大并行度": review.get("最大并行度"),
        "失败传播策略": review.get("失败传播策略"),
        "调度优先级": review.get("调度优先级"),
        "生效时间范围": review.get("生效时间范围"),
        "拓扑指纹": review.get("拓扑指纹"),
    }


def create_review(entry: dict[str, Any], values: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """冻结当前提交的拓扑；校验不通过时不落评审记录，避免半成品进入发布环节。"""
    with LOCK:
        topology_raw = values.get("拓扑结构")
        if _text(topology_raw) == "":
            topology_raw = entry.get("拓扑结构")
        parsed = parse_topology(topology_raw)
        fields = _fields_from_sources(values, entry, parsed)
        errors = validate(parsed, fields)
        if errors:
            return {}, errors
        dag_id = int(entry["id"])
        for row in review_rows(dag_id):
            if row.get("评审状态") == REVIEW_PENDING:
                row["评审状态"] = REVIEW_SUPERSEDED
                row["替代时间"] = _now()
        base_version = max((int(row.get("评审版本", 0)) for row in review_rows(dag_id)), default=0)
        # 评审版本在编排图生命周期内单调递增，停用恢复也不会回到旧版本号。
        base_version = max(base_version, int(entry.get("评审版本") or 0))
        version = base_version + 1
        review = {
            "id": _next_review_id(),
            "编排图ID": dag_id,
            "图编号": fields.get("图编号"),
            "图名称": fields.get("图名称"),
            "评审版本": version,
            "评审状态": REVIEW_PENDING,
            "提交时间": _now(),
            "评审人": values.get("评审人"),
            "备注": values.get("remark") or values.get("备注"),
            "拓扑结构": topology_text(topology_raw),
            "节点": copy.deepcopy(parsed["节点"]),
            "依赖边": copy.deepcopy(parsed["依赖边"]),
            "别名冲突": copy.deepcopy(parsed["别名冲突"]),
            "拓扑指纹": fingerprint(topology_text(topology_raw), parsed["节点"], parsed["依赖边"], fields),
            "起始节点数": fields.get("起始节点数"),
            "最大并行度": fields.get("最大并行度"),
            "失败传播策略": fields.get("失败传播策略"),
            "调度优先级": fields.get("调度优先级"),
            "生效时间范围": fields.get("生效时间范围"),
        }
        store.aux_rows(REVIEW_TABLE).append(review)
        return review, []


def submit_review(entry: dict[str, Any], values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    """提交评审：草稿、已停用、或已发布后的修订都从这里重新进入流程。"""
    status = entry.get("status")
    if status not in (STATUS_DRAFT, STATUS_REVIEWING, STATUS_DISABLED):
        return None, (
            f"编排图当前状态为「{status}」，不能提交编排评审；"
            "已发布拓扑如需变更新版本，请先停用编排，再通过恢复编排回到评审环节"
        )
    review, errors = create_review(entry, values)
    if errors:
        return None, "提交编排评审失败：" + "；".join(errors)
    entry["status"] = STATUS_REVIEWING
    entry["评审状态"] = REVIEW_PENDING
    entry["评审记录编号"] = review["id"]
    entry["评审版本"] = review["评审版本"]
    entry["published_status"] = PUBLISH_UNPUBLISHED
    _apply_review_fields(entry, review)
    entry["pending"] = True
    entry["abnormal"] = False
    return entry, f"编排图已提交编排评审（评审版本 {review['评审版本']}）"


def _apply_review_fields(entry: dict[str, Any], review: dict[str, Any]) -> None:
    for field in TOPOLOGY_FIELDS:
        entry[field] = review.get(field)
    entry["节点"] = copy.deepcopy(review.get("节点") or [])
    entry["依赖边"] = copy.deepcopy(review.get("依赖边") or [])
    entry["拓扑指纹"] = review.get("拓扑指纹")


def _snapshot_rows(dag_id: int) -> list[dict[str, Any]]:
    return [row for row in store.aux_rows(SNAPSHOT_TABLE) if int(row.get("编排图ID", 0)) == dag_id]


def _latest_snapshot(dag_id: int) -> dict[str, Any] | None:
    rows = _snapshot_rows(dag_id)
    if not rows:
        return None
    return max(rows, key=lambda row: int(row.get("发布版本", 0)))


def snapshot_payload(snapshot: dict[str, Any] | None) -> dict[str, Any] | None:
    if snapshot is None:
        return None
    return copy.deepcopy({
        "快照编号": snapshot.get("id"),
        "编排图ID": snapshot.get("编排图ID"),
        "图编号": snapshot.get("图编号"),
        "图名称": snapshot.get("图名称"),
        "发布版本": snapshot.get("发布版本"),
        "发布状态": snapshot.get("发布状态"),
        "来源": snapshot.get("来源"),
        "来源评审记录": snapshot.get("来源评审记录编号"),
        "来源评审版本": snapshot.get("来源评审版本"),
        "发布时间": snapshot.get("发布时间"),
        "停用时间": snapshot.get("停用时间"),
        "失效时间": snapshot.get("失效时间"),
        "拓扑指纹": snapshot.get("拓扑指纹"),
        "拓扑结构": snapshot.get("拓扑结构"),
        "起始节点数": snapshot.get("起始节点数"),
        "最大并行度": snapshot.get("最大并行度"),
        "失败传播策略": snapshot.get("失败传播策略"),
        "调度优先级": snapshot.get("调度优先级"),
        "生效时间范围": snapshot.get("生效时间范围"),
        "节点": copy.deepcopy(snapshot.get("节点") or []),
        "依赖边": copy.deepcopy(snapshot.get("依赖边") or []),
    })


def _review_source(entry: dict[str, Any], values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    """确定发布依据：优先使用当前待发布评审，其次按显式版本号查找。

    返回 (评审记录, 失败原因)。已有评审但版本对不上时拒绝发布，
    绝不用编排图当前字段覆盖已评审的拓扑。
    """
    dag_id = int(entry["id"])
    expected = None
    for key in VERSION_KEYS:
        if key in values and _text(values.get(key)) != "":
            expected = _text(values.get(key))
            break
    latest = _latest_review(dag_id)
    pending = latest if latest and latest.get("评审状态") == REVIEW_PENDING else None
    if expected is None:
        return pending, "" if pending else "编排图没有待发布的评审记录，请先提交编排评审"
    if expected.isdigit():
        version = int(expected)
        found = None
        for row in review_rows(dag_id):
            if int(row.get("评审版本", 0)) == version:
                found = row
                break
        if found is None:
            return None, f"评审版本 {version} 不存在，请重新提交评审"
        if pending is None:
            return None, f"评审版本 {version} 不是当前待发布评审，发布被拒绝"
        if int(found.get("评审版本", 0)) != int(pending.get("评审版本", 0)):
            return None, (
                f"评审版本 {version} 已被评审版本 {pending.get('评审版本')} 替代，"
                "发布被拒绝；请确认后再发布最新评审版本"
            )
        return found, ""
    if pending and expected == str(pending.get("拓扑指纹")):
        return pending, ""
    return None, "提交的评审版本与待发布评审不一致，请刷新后重试"


def _revoke_projection(dag_id: int, status: str, snapshot: dict[str, Any] | None) -> None:
    version = snapshot.get("发布版本") if snapshot else None
    for row in store.rows(DEPENDENCY_MODULE):
        if int(row.get("来源编排图ID") or 0) != dag_id:
            continue
        row["status"] = "已解除"
        row["pending"] = False
        row["发布状态"] = status
        row["拓扑可用"] = False
        if version is not None:
            row["来源发布版本"] = version


def _project_to_dependency(snapshot: dict[str, Any]) -> None:
    """把已发布快照投影成依赖解析清单里的一组关系记录（按快照指纹幂等）。"""
    dag_id = int(snapshot["编排图ID"])
    fp = snapshot.get("拓扑指纹")
    rows = store.rows(DEPENDENCY_MODULE)
    # 新发布版本生效后，旧版本的投影立即失效，依赖解析清单只保留当前已发布拓扑。
    for row in rows:
        if int(row.get("来源编排图ID") or 0) != dag_id or row.get("快照指纹") == fp:
            continue
        row["status"] = "已解除"
        row["pending"] = False
        row["发布状态"] = PUBLISH_SUPERSEDED
        row["拓扑可用"] = False
        row["撤销时间"] = _now()
    existing = [
        row for row in rows
        if int(row.get("来源编排图ID") or 0) == dag_id and row.get("快照指纹") == fp
    ]
    if existing:
        for row in existing:
            row["status"] = "已生效"
            row["pending"] = False
            row["abnormal"] = False
            row["发布状态"] = PUBLISH_PUBLISHED
            row["拓扑可用"] = True
            row["来源发布版本"] = snapshot.get("发布版本")
        return
    edges = snapshot.get("依赖边") or []
    nodes = snapshot.get("节点") or []
    # 只投影评审记录里真实存在的依赖边，不臆造上下游；没有边的拓扑
    # 通过“已发布拓扑”入口提供给消费方，而不是往清单里塞无来源的关系。
    pairs = [(edge.get("上游"), edge.get("下游")) for edge in edges if edge.get("上游") and edge.get("下游")]
    for index, (upstream, downstream) in enumerate(pairs, start=1):
        rows.append({
            "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
            "status": "已生效",
            "pending": False,
            "abnormal": False,
            "关系编号": f"DEP-{snapshot.get('图编号') or dag_id}-{snapshot.get('发布版本')}-{index:02d}",
            "上游作业": upstream,
            "下游作业": downstream,
            "依赖类型": PROJECTION_DEPENDENCY_TYPE,
            "偏移量分钟": "0",
            "触发条件": snapshot.get("失败传播策略"),
            "等待超时": snapshot.get("调度优先级"),
            "跳过标志": "否",
            "来源模块": PROJECTION_SOURCE,
            "来源编排图ID": dag_id,
            "来源图编号": snapshot.get("图编号"),
            "来源评审记录ID": snapshot.get("来源评审记录编号"),
            "来源评审版本": snapshot.get("来源评审版本"),
            "来源发布版本": snapshot.get("发布版本"),
            "发布状态": PUBLISH_PUBLISHED,
            "拓扑可用": True,
            "拓扑结构": snapshot.get("拓扑结构"),
            "失败传播策略": snapshot.get("失败传播策略"),
            "调度优先级": snapshot.get("调度优先级"),
            "起始节点数": snapshot.get("起始节点数"),
            "最大并行度": snapshot.get("最大并行度"),
            "拓扑节点": copy.deepcopy(nodes),
            "拓扑依赖边": copy.deepcopy(edges),
            "快照指纹": fp,
        })


def publish(entry: dict[str, Any], values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    """发布编排：只从编排评审中推进，并用评审记录冻结的拓扑写出快照。"""
    with LOCK:
        if entry.get("status") != STATUS_REVIEWING:
            return None, (
                f"编排图当前状态为「{entry.get('status')}」，发布编排只能从「{STATUS_REVIEWING}」推进；"
                "停用后请先重新提交编排评审"
            )
        review, error = _review_source(entry, values)
        if error:
            return None, error
        assert review is not None
        if review.get("评审状态") != REVIEW_PENDING:
            return None, "该评审版本已发布，不能重复发布；如需更新拓扑请重新提交编排评审"
        parsed = {
            "节点": copy.deepcopy(review.get("节点") or []),
            "依赖边": copy.deepcopy(review.get("依赖边") or []),
            "别名冲突": copy.deepcopy(review.get("别名冲突") or []),
            "错误": [],
        }
        fields = {field: review.get(field) for field in TOPOLOGY_FIELDS}
        errors = validate(parsed, fields)
        if errors:
            return None, "发布编排失败：" + "；".join(errors)
        topology_value = topology_text(review.get("拓扑结构"))
        fp = fingerprint(topology_value, parsed["节点"], parsed["依赖边"], fields)
        if fp != review.get("拓扑指纹"):
            # 评审记录里的拓扑与字段被外部改写过，拒绝发布而不是悄悄发布一份无来源的数据。
            return None, "评审记录中的拓扑与评审指纹不一致，请重新提交编排评审"
        dag_id = int(entry["id"])
        previous = _latest_snapshot(dag_id)
        version = max((int(row.get("发布版本", 0)) for row in _snapshot_rows(dag_id)), default=0) + 1
        for row in _snapshot_rows(dag_id):
            if row.get("发布状态") == PUBLISH_PUBLISHED:
                row["发布状态"] = PUBLISH_SUPERSEDED
                row["失效时间"] = _now()
        review["评审状态"] = REVIEW_PUBLISHED
        review["发布时间"] = _now()
        snapshot = {
            "id": _next_snapshot_id(),
            "编排图ID": dag_id,
            "图编号": fields.get("图编号") or entry.get("图编号"),
            "图名称": fields.get("图名称") or entry.get("图名称"),
            "发布版本": version,
            "发布状态": PUBLISH_PUBLISHED,
            "来源": REVIEW_SOURCE,
            "来源评审记录编号": review.get("id"),
            "来源评审版本": review.get("评审版本"),
            "发布时间": _now(),
            "停用时间": None,
            "失效时间": None,
            "拓扑指纹": fp,
            "拓扑结构": topology_value,
            "节点": copy.deepcopy(parsed["节点"]),
            "依赖边": copy.deepcopy(parsed["依赖边"]),
            "起始节点数": fields.get("起始节点数"),
            "最大并行度": fields.get("最大并行度"),
            "失败传播策略": fields.get("失败传播策略"),
            "调度优先级": fields.get("调度优先级"),
            "生效时间范围": fields.get("生效时间范围"),
        }
        store.aux_rows(SNAPSHOT_TABLE).append(snapshot)
        _project_to_dependency(snapshot)
        entry["status"] = STATUS_PUBLISHED
        entry["published_status"] = PUBLISH_PUBLISHED
        entry["评审状态"] = REVIEW_PUBLISHED
        entry["评审记录编号"] = review.get("id")
        entry["评审版本"] = review.get("评审版本")
        entry["发布版本"] = version
        entry["快照编号"] = snapshot["id"]
        entry["拓扑快照"] = snapshot_payload(snapshot)
        _apply_review_fields(entry, snapshot)
        entry["pending"] = False
        entry["abnormal"] = False
        message = f"编排图已发布编排（发布版本 {version}，来源评审记录 {review.get('id')}）"
        if previous is not None:
            message += f"，上一发布版本 {previous.get('发布版本')} 已失效"
        return entry, message


def disable(entry: dict[str, Any], values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    with LOCK:
        if entry.get("status") != STATUS_PUBLISHED:
            return None, f"编排图当前状态为「{entry.get('status')}」，只有「{STATUS_PUBLISHED}」可以停用"
        snapshot = _latest_snapshot(int(entry["id"]))
        if snapshot and snapshot.get("发布状态") == PUBLISH_PUBLISHED:
            snapshot["发布状态"] = PUBLISH_DISABLED
            snapshot["停用时间"] = _now()
        _revoke_projection(int(entry["id"]), PUBLISH_DISABLED, snapshot)
        entry["status"] = STATUS_DISABLED
        entry["published_status"] = PUBLISH_DISABLED
        entry["pending"] = False
        entry["abnormal"] = True
        return entry, "编排图已停用，依赖解析清单中的对应拓扑已撤销"


# ---------------------------------------------------------------------------
# 历史编排图兼容：没有评审记录与快照的老数据，按「已发布即为已评审」补齐来源。
# ---------------------------------------------------------------------------

def ensure_snapshot(entry: dict[str, Any]) -> dict[str, Any] | None:
    """返回编排图当前生效或最近一次已发布快照（历史数据首次读取时补齐）。"""
    with LOCK:
        dag_id = int(entry.get("id", 0))
        snapshot = _latest_snapshot(dag_id)
        # 停用或评审中的编排图保留最后一次已发布快照用于展示，
        # 但它的发布状态由快照自身表达，不会被依赖解析消费。
        if snapshot is not None or entry.get("status") != STATUS_PUBLISHED:
            return snapshot
        return _adopt_legacy_snapshot(entry)


def _adopt_legacy_snapshot(entry: dict[str, Any]) -> dict[str, Any] | None:
    """历史已发布编排图没有评审记录时，按原拓扑字段补齐可追溯的来源。"""
    dag_id = int(entry.get("id", 0))
    review = _latest_review(dag_id)
    if review is None:
        review = _adopt_legacy(entry)
    if review is None:
        return None
    version = max((int(row.get("发布版本", 0)) for row in _snapshot_rows(dag_id)), default=0) + 1
    fields = {field: review.get(field) for field in TOPOLOGY_FIELDS}
    nodes = copy.deepcopy(review.get("节点") or [])
    edges = copy.deepcopy(review.get("依赖边") or [])
    topology_value = topology_text(review.get("拓扑结构"))
    adopted = {
        "id": _next_snapshot_id(),
        "编排图ID": dag_id,
        "图编号": review.get("图编号") or entry.get("图编号"),
        "图名称": review.get("图名称") or entry.get("图名称"),
        "发布版本": version,
        "发布状态": PUBLISH_PUBLISHED,
        "来源": review.get("来源") or LEGACY_SOURCE,
        "来源评审记录编号": review.get("id"),
        "来源评审版本": review.get("评审版本"),
        "发布时间": review.get("提交时间") or _now(),
        "停用时间": None,
        "失效时间": None,
        "拓扑指纹": fingerprint(topology_value, nodes, edges, fields),
        "拓扑结构": topology_value,
        "节点": nodes,
        "依赖边": edges,
        "起始节点数": fields.get("起始节点数"),
        "最大并行度": fields.get("最大并行度"),
        "失败传播策略": fields.get("失败传播策略"),
        "调度优先级": fields.get("调度优先级"),
        "生效时间范围": fields.get("生效时间范围"),
    }
    store.aux_rows(SNAPSHOT_TABLE).append(adopted)
    _project_to_dependency(adopted)
    entry["快照编号"] = adopted["id"]
    entry["发布版本"] = version
    entry["评审记录编号"] = review.get("id")
    entry["评审版本"] = review.get("评审版本")
    entry["published_status"] = PUBLISH_PUBLISHED
    entry["评审状态"] = REVIEW_PUBLISHED
    entry["拓扑快照"] = snapshot_payload(adopted)
    return adopted


def _adopt_legacy(entry: dict[str, Any]) -> dict[str, Any] | None:
    """把历史已发布编排图就地补成一条评审记录，保留原来的拓扑字段。"""
    parsed = parse_topology(entry.get("拓扑结构"))
    if parsed["别名冲突"] or parsed["错误"]:
        return None
    fields = {
        "拓扑结构": entry.get("拓扑结构"),
        "起始节点数": entry.get("起始节点数"),
        "最大并行度": entry.get("最大并行度"),
        "失败传播策略": entry.get("失败传播策略"),
        "调度优先级": entry.get("调度优先级"),
        "生效时间范围": entry.get("生效时间范围"),
    }
    start_count, parallelism = derive_parallelism(parsed["节点"], parsed["依赖边"])
    if _text(fields["起始节点数"]) == "":
        fields["起始节点数"] = start_count
    if _text(fields["最大并行度"]) == "":
        fields["最大并行度"] = parallelism
    review = {
        "id": _next_review_id(),
        "编排图ID": int(entry["id"]),
        "图编号": entry.get("图编号"),
        "图名称": entry.get("图名称"),
        "评审版本": 1,
        "评审状态": REVIEW_PUBLISHED,
        "提交时间": _now(),
        "评审人": "历史数据补齐",
        "备注": "历史已发布编排图按原拓扑字段补齐评审记录",
        "来源": LEGACY_SOURCE,
        "拓扑结构": topology_text(entry.get("拓扑结构")),
        "节点": copy.deepcopy(parsed["节点"]),
        "依赖边": copy.deepcopy(parsed["依赖边"]),
        "别名冲突": [],
        "拓扑指纹": fingerprint(topology_text(entry.get("拓扑结构")), parsed["节点"], parsed["依赖边"], fields),
        "起始节点数": fields["起始节点数"],
        "最大并行度": fields["最大并行度"],
        "失败传播策略": fields["失败传播策略"],
        "调度优先级": fields["调度优先级"],
        "生效时间范围": fields["生效时间范围"],
    }
    store.aux_rows(REVIEW_TABLE).append(review)
    return review


def published_snapshot(dag_id: int) -> dict[str, Any] | None:
    """依赖解析入口使用的唯一口径：编排图与快照都处于已发布状态。"""
    entry = store.find(DAG_MODULE, dag_id)
    if entry is None or entry.get("status") != STATUS_PUBLISHED:
        return None
    snapshot = ensure_snapshot(entry)
    if snapshot is None or snapshot.get("发布状态") != PUBLISH_PUBLISHED:
        return None
    return snapshot


def latest_snapshot_for_entry(entry: dict[str, Any]) -> dict[str, Any] | None:
    with LOCK:
        return ensure_snapshot(entry)


def current_snapshot(dag_id: int) -> dict[str, Any] | None:
    """最近一次发布快照（无论是否仍然生效），用于编排图与清单展示发布状态。"""
    with LOCK:
        return _latest_snapshot(dag_id)


def published_topologies() -> list[dict[str, Any]]:
    """依赖解析消费方读取的已发布拓扑清单（只含当前已发布状态的编排图）。"""
    ensure_all_published()
    items: list[dict[str, Any]] = []
    with LOCK:
        for entry in store.rows(DAG_MODULE):
            if entry.get("status") != STATUS_PUBLISHED:
                continue
            snapshot = _latest_snapshot(int(entry.get("id", 0)))
            if snapshot is None or snapshot.get("发布状态") != PUBLISH_PUBLISHED:
                continue
            payload = snapshot_payload(snapshot) or {}
            payload["可用依赖边数"] = len(payload.get("依赖边") or [])
            payload["清单关系"] = [
                {
                    "关系编号": row.get("关系编号"),
                    "上游作业": row.get("上游作业"),
                    "下游作业": row.get("下游作业"),
                    "status": row.get("status"),
                }
                for row in store.rows(DEPENDENCY_MODULE)
                if int(row.get("来源编排图ID") or 0) == int(entry.get("id", 0))
                and row.get("快照指纹") == snapshot.get("拓扑指纹")
                and row.get("status") == "已生效"
            ]
            items.append(payload)
    return items


def ensure_all_published() -> None:
    """读取依赖解析前先补齐历史已发布编排图的快照，保证清单不因入口顺序而空。"""
    with LOCK:
        for entry in store.rows(DAG_MODULE):
            if entry.get("status") == STATUS_PUBLISHED:
                ensure_snapshot(entry)


def projection_for(dag_id: int) -> list[dict[str, Any]]:
    """依赖解析清单里来自该编排图的投影记录（按发布版本倒序）。"""
    rows = [
        copy.deepcopy(row)
        for row in store.rows(DEPENDENCY_MODULE)
        if int(row.get("来源编排图ID") or 0) == dag_id
    ]
    return sorted(rows, key=lambda row: int(row.get("来源发布版本") or 0), reverse=True)
