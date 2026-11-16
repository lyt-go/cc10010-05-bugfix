"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。
"""
from __future__ import annotations

import copy
from typing import Any

from app.seed import SEED_ROWS


class Store:
    def __init__(self) -> None:
        self._seed: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        self.reset()

    def reset(self) -> None:
        """回到示例数据：业务表深拷贝一份，辅助表（评审记录、快照）清空。"""
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [copy.deepcopy(row) for row in rows] for name, rows in self._seed.items()
        }
        # 辅助表（评审记录、拓扑快照等）不参与运营概览与模块统计，
        # 但仍和业务表一样随内存仓库复位，避免发布数据散落在进程全局变量里。
        self._aux: dict[str, list[dict[str, Any]]] = {}

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def aux_rows(self, name: str) -> list[dict[str, Any]]:
        return self._aux.setdefault(name, [])

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
        return {"cards": cards, "modules": modules}


store = Store()
