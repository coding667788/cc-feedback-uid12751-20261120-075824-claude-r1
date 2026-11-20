"""交错的确定性测试：同步点固定先后，另一进程按固定顺序先提交。

这里覆盖需求里点名的三种交错：

1. 同一套两分区策略里，两个分区各有不同生效边界，由两个进程分别写入。
2. 另一进程先提交发布，导致本进程随后的整次策略切换失败。
3. 旧空洞与新规则的重放，跨进程读取时冻结值一致。

每个用例的先后都由文件同步点决定；不使用 sleep 猜测顺序。
"""

from __future__ import annotations

import os
import tempfile
import unittest

try:
    from helpers import Worker, make_workdir, wait_for_file
except ImportError:  # 直接以 tests.test_* 运行时
    from tests.helpers import Worker, make_workdir, wait_for_file

from solution import Ledger


class InterleavingTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="ledger-interleave-")
        self.root = self._tmp.name
        self.db = os.path.join(self.root, "ledger.sqlite3")
        self.workers: list[Worker] = []
        with Ledger(self.db) as ledger:
            self._bootstrap(ledger)

    def tearDown(self) -> None:
        for worker in self.workers:
            if worker.proc is not None and worker.proc.poll() is None:
                worker.terminate()
        self._tmp.cleanup()

    # ------------------------------------------------------------------ 构造

    @staticmethod
    def _bootstrap(ledger: Ledger) -> None:
        for partition, topic in ((0, "orders.eu"), (1, "orders.us")):
            response = ledger.execute(
                {
                    "op": "declare_partition",
                    "request_id": f"boot-p{partition}",
                    "tenant": "acme",
                    "partition": partition,
                    "topic": topic,
                }
            )
            assert response["class"] == "ok", response
        response = ledger.execute(
            {
                "op": "register_policy",
                "request_id": "boot-v1",
                "tenant": "acme",
                "version": 1,
                "coverage": [
                    {"partition": 0, "effective_offset": 0},
                    {"partition": 1, "effective_offset": 0},
                ],
                "rules": [
                    {"subscriber": "eu", "pattern": "orders.eu", "effect": "allow"},
                    {"subscriber": "us", "pattern": "orders.us", "effect": "allow"},
                ],
            }
        )
        assert response["class"] == "ok", response

    def start(self, name: str, steps: list[dict], **extra) -> Worker:
        spec = {"steps": steps}
        spec.update(extra)
        workdir = make_workdir(self.root, name)
        worker = Worker(self.db, spec, workdir, name, sync_dir=self.root)
        self.workers.append(worker)
        return worker.start()

    def touch(self, name: str) -> None:
        with open(os.path.join(self.root, name), "w", encoding="utf-8") as fh:
            fh.write("1")

    def await_shared(self, name: str, timeout: float = 60.0) -> None:
        wait_for_file(os.path.join(self.root, name), f"未等到 {name}", timeout)

    # ------------------------------------------------------------------ 交错

    def test_partition_boundaries_are_fixed_by_two_writers(self) -> None:
        """两个进程各自只写一个分区；版本选择只取决于该分区的生效边界。"""
        with Ledger(self.db) as ledger:
            response = ledger.execute(
                {
                    "op": "register_policy",
                    "request_id": "boot-v2",
                    "tenant": "acme",
                    "version": 2,
                    "coverage": [
                        {"partition": 0, "effective_offset": 5},
                        {"partition": 1, "effective_offset": 9},
                    ],
                    "rules": [
                        {"subscriber": "eu", "pattern": "orders.eu", "effect": "allow"},
                        {"subscriber": "us", "pattern": "orders.us", "effect": "allow"},
                        {"subscriber": "both", "pattern": "orders.*", "effect": "allow"},
                    ],
                }
            )
            self.assertEqual(response["class"], "ok", response)

        writer_a = self.start(
            "writer-a",
            [
                {"step": "p0-4", "op": "publish", "request_id": "p0-4", "tenant": "acme",
                 "partition": 0, "offset": 4, "payload": {"n": 1}},
                {"step": "p0-5", "op": "publish", "request_id": "p0-5", "tenant": "acme",
                 "partition": 0, "offset": 5, "payload": {"n": 2}},
                {"touch": "a.done", "wait_for": "b.done"},
                {"step": "p0-9", "op": "publish", "request_id": "p0-9", "tenant": "acme",
                 "partition": 0, "offset": 9, "payload": {"n": 3}},
                {"touch": "a.final"},
            ],
        )
        writer_b = self.start(
            "writer-b",
            [
                {"wait_for": "a.done"},
                {"step": "p1-8", "op": "publish", "request_id": "p1-8", "tenant": "acme",
                 "partition": 1, "offset": 8, "payload": {"n": 4}},
                {"step": "p1-9", "op": "publish", "request_id": "p1-9", "tenant": "acme",
                 "partition": 1, "offset": 9, "payload": {"n": 5}},
                {"touch": "b.done"},
            ],
        )

        a_results = {r["step"]: r["response"] for r in writer_a.finish()}
        b_results = {r["step"]: r["response"] for r in writer_b.finish()}

        # 边界由版本声明固定：p0 的 5 是 v2，p1 的 9 是 v2，其余是 v1。
        self.assertEqual(a_results["p0-4"]["result"]["policy_version"], 1)
        self.assertEqual(a_results["p0-5"]["result"]["policy_version"], 2)
        self.assertEqual(a_results["p0-9"]["result"]["policy_version"], 2)
        self.assertEqual(b_results["p1-8"]["result"]["policy_version"], 1)
        self.assertEqual(b_results["p1-9"]["result"]["policy_version"], 2)
        # 订阅者集合随版本变化：v1 只有 us，v2 增加 both。
        self.assertEqual(b_results["p1-8"]["result"]["subscribers"], ["us"])
        self.assertEqual(b_results["p1-9"]["result"]["subscribers"], ["both", "us"])

    def test_other_process_commits_first_and_fails_this_switch(self) -> None:
        """另一进程先提交了偏移 7；随后本进程的整次切换因 p1 边界偏低被全拒。"""
        writer = self.start(
            "early-writer",
            [
                {"step": "p1-7", "op": "publish", "request_id": "early-p1-7",
                 "tenant": "acme", "partition": 1, "offset": 7, "payload": {"n": 7}},
                {"touch": "early.done"},
            ],
        )
        self.await_shared("early.done")
        results = {r["step"]: r["response"] for r in writer.finish()}
        self.assertEqual(results["p1-7"]["class"], "ok")

        with Ledger(self.db) as ledger:
            response = ledger.execute(
                {
                    "op": "register_policy",
                    "request_id": "switch-v2",
                    "tenant": "acme",
                    "version": 2,
                    "coverage": [
                        {"partition": 0, "effective_offset": 8},
                        {"partition": 1, "effective_offset": 7},
                    ],
                    "rules": [],
                }
            )
            self.assertEqual(response["class"], "rejected", response)
            self.assertEqual(response["error"]["code"], "VERSION_BELOW_WATERMARK")
            self.assertEqual(response["error"]["details"]["partition"], 1)
            self.assertEqual(response["error"]["details"]["max_seen"], 7)
            # 全部分区整次拒绝：分区 0 的覆盖也没写入。
            state = ledger.execute({"op": "state", "tenant": "acme"})["result"]
            self.assertNotIn(2, [p["version"] for p in state["policies"]])

    def test_old_hole_late_arrival_is_resolved_with_old_version(self) -> None:
        """p0 上偏移 1 迟到，但 p1 已经推进到版本 2；迟到消息仍按 p0 的 v1 结算。"""
        with Ledger(self.db) as ledger:
            ledger.execute(
                {"op": "publish", "request_id": "seed-0", "tenant": "acme",
                 "partition": 0, "offset": 0, "payload": {"n": 1}}
            )
            response = ledger.execute(
                {
                    "op": "register_policy",
                    "request_id": "switch-v2",
                    "tenant": "acme",
                    "version": 2,
                    "coverage": [
                        {"partition": 0, "effective_offset": 3},
                        {"partition": 1, "effective_offset": 3},
                    ],
                    "rules": [
                        {"subscriber": "new", "pattern": "orders.*", "effect": "allow"}
                    ],
                }
            )
            self.assertEqual(response["class"], "ok", response)

        late = self.start(
            "late-writer",
            [
                {"step": "p0-1", "op": "publish", "request_id": "late-p0-1",
                 "tenant": "acme", "partition": 0, "offset": 1, "payload": {"n": 2}},
                {"step": "p0-3", "op": "publish", "request_id": "late-p0-3",
                 "tenant": "acme", "partition": 0, "offset": 3, "payload": {"n": 3}},
            ],
        )
        results = {r["step"]: r["response"] for r in late.finish()}
        self.assertEqual(results["p0-1"]["class"], "ok")
        self.assertEqual(results["p0-1"]["result"]["policy_version"], 1)
        self.assertEqual(results["p0-1"]["result"]["subscribers"], ["eu"])
        self.assertEqual(results["p0-3"]["result"]["policy_version"], 2)
        self.assertEqual(results["p0-3"]["result"]["subscribers"], ["new"])

    def test_stale_rule_replay_sees_frozen_subscribers(self) -> None:
        """旧规则下发布的消息，即使规则已改为 v2，历史读取/重放仍返回旧集合。"""
        with Ledger(self.db) as ledger:
            ledger.execute(
                {"op": "publish", "request_id": "old-msg", "tenant": "acme",
                 "partition": 0, "offset": 0, "payload": {"n": 1}}
            )
            response = ledger.execute(
                {
                    "op": "register_policy",
                    "request_id": "switch-v2",
                    "tenant": "acme",
                    "version": 2,
                    "coverage": [
                        {"partition": 0, "effective_offset": 1},
                        {"partition": 1, "effective_offset": 1},
                    ],
                    "rules": [
                        {"subscriber": "new", "pattern": "orders.*", "effect": "allow"}
                    ],
                }
            )
            self.assertEqual(response["class"], "ok", response)

        reader = self.start(
            "reader",
            [
                {"step": "read-old", "op": "get_message", "tenant": "acme",
                 "partition": 0, "offset": 0},
                # 换一个请求 ID：走消息级重复判定，而不是回执重放。
                {"step": "replay-old", "op": "publish", "request_id": "old-msg-again",
                 "tenant": "acme", "partition": 0, "offset": 0, "payload": {"n": 1}},
                {"step": "read-new", "op": "get_message", "tenant": "acme",
                 "partition": 0, "offset": 5},
            ],
        )
        results = {r["step"]: r["response"] for r in reader.finish()}
        self.assertEqual(results["read-old"]["result"]["policy_version"], 1)
        self.assertEqual(results["read-old"]["result"]["subscribers"], ["eu"])
        self.assertEqual(results["replay-old"]["result"]["duplicate"], True)
        self.assertEqual(results["replay-old"]["result"]["policy_version"], 1)
        self.assertEqual(results["replay-old"]["result"]["subscribers"], ["eu"])
        self.assertEqual(results["read-new"]["class"], "rejected")
        self.assertEqual(results["read-new"]["error"]["code"], "MESSAGE_NOT_FOUND")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
