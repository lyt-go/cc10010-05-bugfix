"""编排图发布、拓扑快照与依赖解析清单的回归测试。

不依赖 pytest 或 httpx：直接用标准库 unittest 调用 service，
保证在只装了运行依赖的环境里也能验证状态流转口径。
"""
from __future__ import annotations

import json
import unittest

from app.services import topology
from app.services.dag import DagService
from app.services.dependency import DependencyService
from app.store import Store

TOPO = json.dumps(
    {
        "节点": [{"别名": "ods"}, {"别名": "dwd"}, {"别名": "ads"}],
        "依赖边": [{"上游": "ods", "下游": "dwd"}, {"上游": "dwd", "下游": "ads"}],
        "失败传播策略": "下游阻断",
        "调度优先级": "P0",
    },
    ensure_ascii=False,
)


class DagPublishTestCase(unittest.TestCase):
    def setUp(self) -> None:
        # 每个用例独立的仓库，避免共享快照表互相影响。
        import app.store as store_module

        self.store = store_module.store
        self.store.reset()
        self.dag = DagService()
        self.dep = DependencyService()

    def tearDown(self) -> None:
        self.store.reset()

    def create(self, code: str = "DAG-T01") -> int:
        entry, missing, errors = self.dag.create_entry({
            "图编号": code,
            "图名称": "测试链路",
            "拓扑结构": TOPO,
            "失败传播策略": "下游阻断",
            "调度优先级": "P0",
        })
        self.assertEqual((missing, errors), ([], []))
        return int(entry["id"])

    def projection(self, entry_id: int) -> tuple[list[dict], int]:
        items, total = self.dep.list_entries(page=1, size=100, source=topology.PROJECTION_SOURCE)
        mine = [row for row in items if int(row.get("来源编排图ID") or 0) == entry_id]
        return mine, len(mine)

    def topologies(self, entry_id: int) -> list[dict]:
        return [
            item for item in topology.published_topologies()
            if int(item.get("编排图ID") or 0) == entry_id
        ]

    def submit(self, entry_id: int, values: dict | None = None) -> tuple:
        return self.dag.run_action(entry_id, "提交编排评审", values or {})

    def publish(self, entry_id: int, values: dict | None = None) -> tuple:
        return self.dag.run_action(entry_id, "发布编排", values or {})

    def test_publish_must_follow_lifecycle(self) -> None:
        entry_id = self.create()
        entry, message = self.publish(entry_id)
        self.assertIsNone(entry)
        self.assertIn("草稿", message)

        entry, _ = self.submit(entry_id)
        self.assertEqual(entry["status"], "编排评审中")
        self.assertEqual(entry["评审状态"], "待发布")

        entry, message = self.publish(entry_id)
        self.assertEqual(entry["status"], "已发布")
        self.assertEqual(entry["发布状态"], "已发布")
        self.assertEqual(entry["发布版本"], 1)
        snapshot = entry["拓扑快照"]
        self.assertEqual(snapshot["来源"], topology.REVIEW_SOURCE)
        self.assertEqual(snapshot["来源评审版本"], 1)
        self.assertEqual(snapshot["失败传播策略"], "下游阻断")
        self.assertEqual(snapshot["调度优先级"], "P0")
        self.assertEqual(len(snapshot["节点"]), 3)
        self.assertEqual(len(snapshot["依赖边"]), 2)
        self.assertIn("发布版本 1", message)

    def test_publish_uses_review_topology_not_current_fields(self) -> None:
        entry_id = self.create()
        self.submit(entry_id)

        # 发布时携带另一份未评审的拓扑，必须被忽略，快照仍来自评审记录。
        forged = json.loads(TOPO)
        forged["失败传播策略"] = "整链路阻断"
        forged["调度优先级"] = "P9"
        entry, _ = self.publish(entry_id, {
            "拓扑结构": json.dumps(forged, ensure_ascii=False),
            "失败传播策略": "整链路阻断",
            "调度优先级": "P9",
        })
        self.assertEqual(entry["拓扑快照"]["失败传播策略"], "下游阻断")
        self.assertEqual(entry["拓扑快照"]["调度优先级"], "P0")

    def test_alias_conflict_blocks_review_and_publish(self) -> None:
        entry_id = self.create()
        conflict = json.dumps(
            {"节点": [{"别名": "ods"}, {"别名": "ods"}], "失败传播策略": "下游阻断", "调度优先级": "P1"},
            ensure_ascii=False,
        )
        entry, message = self.submit(entry_id, {"拓扑结构": conflict})
        self.assertIsNone(entry)
        self.assertIn("节点别名冲突", message)
        self.assertIsNone(self.dag.get_entry(entry_id)["拓扑快照"])

        # 正常评审仍可继续，冲突提交没有落成新评审版本。
        entry, _ = self.submit(entry_id, {"拓扑结构": TOPO, "失败传播策略": "下游阻断", "调度优先级": "P0"})
        self.assertEqual(entry["评审版本"], 1)
        versions = [review["评审版本"] for review in self.dag.reviews(entry_id)]
        self.assertEqual(versions, [1])

    def test_superseded_review_cannot_overwrite_latest(self) -> None:
        entry_id = self.create()
        self.submit(entry_id, {"拓扑结构": TOPO, "失败传播策略": "仅当前节点失败", "调度优先级": "P2"})
        entry, message = self.submit(
            entry_id,
            {"拓扑结构": TOPO, "失败传播策略": "整链路阻断", "调度优先级": "P0"},
        )
        self.assertEqual(entry["评审版本"], 2)
        states = {review["评审版本"]: review["评审状态"] for review in self.dag.reviews(entry_id)}
        self.assertEqual(states, {1: "已被替代", 2: "待发布"})

        stale, message = self.publish(entry_id, {"expected_version": 1})
        self.assertIsNone(stale)
        self.assertIn("替代", message)
        self.assertEqual(self.store.aux_rows(topology.SNAPSHOT_TABLE), [])

        entry, _ = self.publish(entry_id, {"expected_version": 2})
        self.assertEqual(entry["拓扑快照"]["失败传播策略"], "整链路阻断")
        self.assertEqual(entry["拓扑快照"]["调度优先级"], "P0")

    def test_dependency_only_consumes_published_snapshot(self) -> None:
        entry_id = self.create()
        items, total = self.projection(entry_id)
        self.assertEqual(total, 0)
        self.assertEqual(self.topologies(entry_id), [])

        self.submit(entry_id)
        self.assertEqual(self.projection(entry_id)[1], 0)

        published, _ = self.publish(entry_id)
        fingerprint = published["拓扑快照"]["拓扑指纹"]
        items, total = self.projection(entry_id)
        self.assertEqual(total, 2)
        for row in items:
            self.assertEqual(row["发布状态"], "已发布")
            self.assertTrue(row["拓扑可用"])
            self.assertEqual(row["失败传播策略"], "下游阻断")
            self.assertEqual(row["拓扑快照"]["拓扑指纹"], fingerprint)
        self.assertEqual({(row["上游作业"], row["下游作业"]) for row in items}, {("ods", "dwd"), ("dwd", "ads")})

        topologies = self.topologies(entry_id)
        self.assertEqual(len(topologies), 1)
        self.assertEqual(topologies[0]["拓扑指纹"], fingerprint)

    def test_disable_revokes_projection_and_restore_returns_to_review(self) -> None:
        entry_id = self.create()
        self.submit(entry_id)
        self.publish(entry_id)

        entry, _ = self.dag.run_action(entry_id, "停用编排")
        self.assertEqual(entry["status"], "已停用")
        self.assertFalse(entry["拓扑状态"]["已发布拓扑可用"])
        projection, _ = self.projection(entry_id)
        self.assertTrue(all(row["status"] == "已解除" for row in projection))
        self.assertTrue(all(not row["拓扑可用"] for row in projection))
        self.assertEqual(self.topologies(entry_id), [])

        blocked, message = self.publish(entry_id)
        self.assertIsNone(blocked)
        self.assertIn("停用后", message)

        entry, message = self.dag.run_action(entry_id, "恢复编排")
        self.assertEqual(entry["status"], "编排评审中")
        self.assertEqual(entry["评审状态"], "待发布")
        self.assertIn("重新发布", message)
        restored_rows, restored_total = self.projection(entry_id)
        self.assertEqual(restored_total, 2)
        self.assertTrue(all(row["status"] == "已解除" for row in restored_rows))
        self.assertTrue(all(not row["拓扑可用"] for row in restored_rows))
        self.assertEqual(self.topologies(entry_id), [])

        entry, _ = self.publish(entry_id)
        self.assertEqual(entry["发布版本"], 2)
        self.assertEqual(entry["评审版本"], 2)
        projection, _ = self.projection(entry_id)
        self.assertEqual({row["status"] for row in projection}, {"已生效"})
        self.assertEqual({row["来源发布版本"] for row in projection}, {2})

    def test_list_and_publish_detail_share_snapshot(self) -> None:
        entry_id = self.create()
        self.submit(entry_id)
        self.publish(entry_id)

        listed = next(row for row in self.dag.list_entries(page=1, size=100)[0] if row["id"] == entry_id)
        detail = self.dag.publish_detail(entry_id)
        self.assertEqual(listed["拓扑快照"]["拓扑指纹"], detail["topology"]["拓扑指纹"])
        self.assertEqual(listed["发布状态"], detail["topology"]["发布状态"])
        self.assertEqual(listed["失败传播策略"], detail["topology"]["失败传播策略"])
        self.assertEqual(detail["entry"]["拓扑状态"]["快照编号"], detail["topology"]["快照编号"])

    def test_legacy_published_dag_is_adopted_with_source(self) -> None:
        # 种子里的 DAG-0003 已是已发布但没有评审记录。
        legacy = self.dag.get_entry(3)
        self.assertEqual(legacy["status"], "已发布")
        snapshot = legacy["拓扑快照"]
        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot["来源"], topology.LEGACY_SOURCE)
        self.assertIsNotNone(snapshot["来源评审记录"])
        reviews = self.dag.reviews(3)
        self.assertEqual([review["评审状态"] for review in reviews], ["已发布"])

    def test_non_ascii_aliases_survive_round_trip(self) -> None:
        topology_json = json.dumps(
            {
                "节点": [{"别名": "订单抽取"}, {"别名": "订单汇总"}],
                "依赖边": [{"上游": "订单抽取", "下游": "订单汇总"}],
                "失败传播策略": "下游阻断",
                "调度优先级": "P1",
            },
            ensure_ascii=False,
        )
        entry, missing, errors = self.dag.create_entry({
            "图编号": "DAG-T02", "图名称": "中文别名", "拓扑结构": topology_json,
            "失败传播策略": "下游阻断", "调度优先级": "P1",
        })
        self.assertEqual((missing, errors), ([], []))
        self.dag.run_action(entry["id"], "提交编排评审")
        published, _ = self.publish(entry["id"])
        pairs = {(row["上游作业"], row["下游作业"]) for row in self.projection(entry["id"])[0]}
        self.assertEqual(pairs, {("订单抽取", "订单汇总")})
        self.assertEqual(published["拓扑快照"]["失败传播策略"], "下游阻断")


if __name__ == "__main__":
    unittest.main(verbosity=2)
