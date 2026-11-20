"""跨进程并发与交错测试。

所有顺序都由文件同步点固定，不使用 sleep 猜时序；每个子进程都是本测试
用 Popen 启动的，终止时只作用于该 Popen 对象。
"""

from __future__ import annotations

import json
import os
import tempfile
import unittest

try:  # discover -s tests 把 tests 目录本身当导入根
    from helpers import Worker, make_workdir, wait_for_file
except ImportError:  # 直接以 tests.test_* 运行时
    from tests.helpers import Worker, make_workdir, wait_for_file

from solution import Ledger


def _setup_two_partitions(db: str) -> None:
    with Ledger(db) as ledger:
        for partition, topic in ((0, "orders.eu"), (1, "orders.us")):
            response = ledger.execute(
                {
                    "op": "declare_partition",
                    "request_id": f"setup-p{partition}",
                    "tenant": "acme",
                    "partition": partition,
                    "topic": topic,
                }
            )
            assert response["class"] == "ok", response
        response = ledger.execute(
            {
                "op": "register_policy",
                "request_id": "setup-v1",
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


class ConcurrencyTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="ledger-conc-")
        self.root = self._tmp.name
        self.db = os.path.join(self.root, "ledger.sqlite3")
        self.workers: list[Worker] = []

    def tearDown(self) -> None:
        for worker in self.workers:
            if worker.proc is not None and worker.proc.poll() is None:
                worker.terminate()
        self._tmp.cleanup()

    def worker(self, name: str, spec: dict) -> Worker:
        workdir = make_workdir(self.root, name)
        w = Worker(self.db, spec, workdir, name, sync_dir=self.root)
        self.workers.append(w)
        return w.start()

    def release(self, name: str) -> None:
        """触碰所有工作进程共享的同步点文件。"""
        with open(os.path.join(self.root, name), "w", encoding="utf-8") as fh:
            fh.write("1")

    def wait_shared(self, name: str, timeout: float = 60.0) -> None:
        wait_for_file(os.path.join(self.root, name), f"主进程未等到 {name}", timeout)

    # ------------------------------------------------------------------ 用例

    def test_two_partitions_fixed_boundaries_across_processes(self) -> None:
        """两个分区各有不同生效边界；跨进程的发布各自按所属边界选版本。"""
        _setup_two_partitions(self.db)
        with Ledger(self.db) as ledger:
            response = ledger.execute(
                {
                    "op": "register_policy",
                    "request_id": "setup-v2",
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

        # 顺序固定：p0 先写 4/5，等 p1 写完 8/9，再回到 p0 写 4 之后的位置校验。
        p0 = self.worker(
            "p0",
            {
                "steps": [
                    {"step": "p0-4", "op": "publish", "request_id": "p0-4", "tenant": "acme",
                     "partition": 0, "offset": 4, "payload": {"order": "A-1"}},
                    {"step": "p0-5", "op": "publish", "request_id": "p0-5", "tenant": "acme",
                     "partition": 0, "offset": 5, "payload": {"order": "A-2"}},
                    {"touch": "p0.done", "wait_for": "p1.done"},
                ]
            },
        )
        p1 = self.worker(
            "p1",
            {
                "steps": [
                    {"wait_for": "p0.done"},
                    {"step": "p1-8", "op": "publish", "request_id": "p1-8", "tenant": "acme",
                     "partition": 1, "offset": 8, "payload": {"order": "A-3"}},
                    {"step": "p1-9", "op": "publish", "request_id": "p1-9", "tenant": "acme",
                     "partition": 1, "offset": 9, "payload": {"order": "A-1"}},
                    {"touch": "p1.done"},
                ]
            },
        )

        p0_results = {r["step"]: r["response"] for r in p0.finish()}
        p1_results = {r["step"]: r["response"] for r in p1.finish()}

        self.assertEqual(p0_results["p0-4"]["result"]["policy_version"], 1)
        self.assertEqual(p0_results["p0-4"]["result"]["subscribers"], ["eu"])
        self.assertEqual(p0_results["p0-5"]["result"]["policy_version"], 2)
        self.assertEqual(p0_results["p0-5"]["result"]["subscribers"], ["both", "eu"])
        self.assertEqual(p1_results["p1-8"]["result"]["policy_version"], 1)
        self.assertEqual(p1_results["p1-8"]["result"]["subscribers"], ["us"])
        self.assertEqual(p1_results["p1-9"]["result"]["policy_version"], 2)
        self.assertEqual(p1_results["p1-9"]["result"]["subscribers"], ["both", "us"])

    def test_other_process_publish_forces_whole_switch_rejection(self) -> None:
        """另进程先把 p1 推到偏移 7；切换时 p1 生效偏移 7 不再「高于已看到」，整次拒绝。"""
        _setup_two_partitions(self.db)

        writer = self.worker(
            "writer",
            {
                "steps": [
                    {"step": "p1-7", "op": "publish", "request_id": "writer-p1-7", "tenant": "acme",
                     "partition": 1, "offset": 7, "payload": {"order": "A-1"}},
                    {"touch": "writer.done"},
                ]
            },
        )
        self.wait_shared("writer.done")

        with Ledger(self.db) as ledger:
            response = ledger.execute(
                {
                    "op": "register_policy",
                    "request_id": "switch-v2",
                    "tenant": "acme",
                    "version": 2,
                    "coverage": [
                        {"partition": 0, "effective_offset": 100},
                        {"partition": 1, "effective_offset": 7},
                    ],
                    "rules": [
                        {"subscriber": "both", "pattern": "orders.*", "effect": "allow"}
                    ],
                }
            )
            self.assertEqual(response["class"], "rejected", response)
            self.assertEqual(response["error"]["code"], "VERSION_BELOW_WATERMARK")
            self.assertEqual(response["error"]["details"]["partition"], 1)
            self.assertEqual(response["error"]["details"]["max_seen"], 7)

            state = ledger.execute({"op": "state", "tenant": "acme"})["result"]
            self.assertEqual([p["version"] for p in state["policies"]], [1])
            self.assertEqual(state["current_policy_version"], 1)

            # 分区 0 的生效偏移 100 虽然合法，也一并被拒绝（全成或全拒）。
            late = ledger.execute(
                {
                    "op": "publish",
                    "request_id": "after-failure",
                    "tenant": "acme",
                    "partition": 0,
                    "offset": 50,
                    "payload": {"order": "A-2"},
                }
            )
            self.assertEqual(late["class"], "ok", late)
            self.assertEqual(late["result"]["policy_version"], 1)

        writer.finish()

    def test_old_hole_late_and_rule_replay_across_processes(self) -> None:
        """v1 的旧洞迟到：仍按 v1 结算；v2 落点按 v2 结算；重放拿到各自冻结的集合。"""
        _setup_two_partitions(self.db)

        early = self.worker(
            "early",
            {
                "steps": [
                    {"step": "p0-0", "op": "publish", "request_id": "early-p0-0", "tenant": "acme",
                     "partition": 0, "offset": 0, "payload": {"order": "A-1"}},
                    {"step": "p0-2", "op": "publish", "request_id": "early-p0-2", "tenant": "acme",
                     "partition": 0, "offset": 2, "payload": {"order": "A-2"}},
                    {"touch": "early.done", "wait_for": "switch.done"},
                    {"step": "p0-3", "op": "publish", "request_id": "early-p0-3", "tenant": "acme",
                     "partition": 0, "offset": 3, "payload": {"order": "A-3"}},
                    {"step": "p0-1-late", "op": "publish", "request_id": "early-p0-1",
                     "tenant": "acme", "partition": 0, "offset": 1, "payload": {"order": "A-4"}},
                    {"step": "p0-3-replay", "op": "publish", "request_id": "early-p0-3-replay",
                     "tenant": "acme", "partition": 0, "offset": 3, "payload": {"order": "A-3"}},
                    {"touch": "early.final"},
                ]
            },
        )
        self.wait_shared("early.done")

        with Ledger(self.db) as ledger:
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
            self.assertEqual(
                response["result"]["max_seen_before"], {"0": 2, "1": -1}, response["result"]
            )

        self.release("switch.done")
        early.wait_file("early.final")

        results = {r["step"]: r["response"] for r in early.finish()}
        self.assertEqual(results["p0-3"]["class"], "ok")
        self.assertEqual(results["p0-3"]["result"]["policy_version"], 2)
        self.assertEqual(results["p0-3"]["result"]["subscribers"], ["new"])
        # 旧洞迟到：偏移 1 落在 v1 区间，按 v1 规则结算。
        self.assertEqual(results["p0-1-late"]["class"], "ok")
        self.assertEqual(results["p0-1-late"]["result"]["policy_version"], 1)
        self.assertEqual(results["p0-1-late"]["result"]["subscribers"], ["eu"])
        # 重放：不重算，返回 v2 冻结集合。
        self.assertEqual(results["p0-3-replay"]["result"]["duplicate"], True)
        self.assertEqual(results["p0-3-replay"]["result"]["policy_version"], 2)
        self.assertEqual(results["p0-3-replay"]["result"]["subscribers"], ["new"])

    def test_concurrent_policy_registration_serializes(self) -> None:
        """两个进程同时登记版本 2：一个成功，另一个因版本不单调被拒。"""
        _setup_two_partitions(self.db)

        a = self.worker(
            "reg-a",
            {
                "steps": [
                    {"touch": "a.ready", "wait_for": "b.ready"},
                    {"step": "a-v2", "op": "register_policy", "request_id": "reg-a-v2",
                     "tenant": "acme", "version": 2,
                     "coverage": [{"partition": 0, "effective_offset": 10},
                                  {"partition": 1, "effective_offset": 10}],
                     "rules": [{"subscriber": "a", "pattern": "orders.*", "effect": "allow"}]},
                ]
            },
        )
        b = self.worker(
            "reg-b",
            {
                "steps": [
                    {"touch": "b.ready", "wait_for": "a.ready"},
                    {"step": "b-v2", "op": "register_policy", "request_id": "reg-b-v2",
                     "tenant": "acme", "version": 2,
                     "coverage": [{"partition": 0, "effective_offset": 10},
                                  {"partition": 1, "effective_offset": 10}],
                     "rules": [{"subscriber": "b", "pattern": "orders.*", "effect": "allow"}]},
                ]
            },
        )
        a_results = {r["step"]: r["response"] for r in a.finish()}
        b_results = {r["step"]: r["response"] for r in b.finish()}

        outcomes = [a_results["a-v2"], b_results["b-v2"]]
        ok = [r for r in outcomes if r["class"] == "ok"]
        rejected = [r for r in outcomes if r["class"] == "rejected"]
        self.assertEqual(len(ok), 1, outcomes)
        self.assertEqual(len(rejected), 1, outcomes)
        self.assertEqual(rejected[0]["error"]["code"], "VERSION_CONFLICT")

        with Ledger(self.db) as ledger:
            state = ledger.execute({"op": "state", "tenant": "acme"})["result"]
            self.assertEqual([p["version"] for p in state["policies"]], [1, 2])

    def test_same_request_id_across_processes_is_first_wins(self) -> None:
        """两个进程用同一请求 ID 并发发布同一偏移：第二个必须拿到第一次的持久化结果。"""
        _setup_two_partitions(self.db)

        writer_a = self.worker(
            "idem-a",
            {
                "steps": [
                    {"touch": "a.ready", "wait_for": "b.ready"},
                    {"step": "publish", "op": "publish", "request_id": "shared-id",
                     "tenant": "acme", "partition": 0, "offset": 0,
                     "payload": {"order": "A-1"}},
                ]
            },
        )
        writer_b = self.worker(
            "idem-b",
            {
                "steps": [
                    {"touch": "b.ready", "wait_for": "a.ready"},
                    {"step": "publish", "op": "publish", "request_id": "shared-id",
                     "tenant": "acme", "partition": 0, "offset": 0,
                     "payload": {"order": "A-1"}},
                ]
            },
        )
        a = writer_a.finish()[0]["response"]
        b = writer_b.finish()[0]["response"]
        self.assertEqual(a["class"], "ok")
        self.assertEqual(b["class"], "ok")
        # 恰有一个是首次提交，另一个是回执重放；两者业务结果一致。
        self.assertEqual(a["result"]["digest"], b["result"]["digest"])
        self.assertEqual(a["result"]["policy_version"], b["result"]["policy_version"])
        self.assertEqual(
            {a["replayed"], b["replayed"]}, {True, False}
        )
        self.assertEqual(
            {a["result"]["original_request_id"], b["result"]["original_request_id"]},
            {"shared-id"},
        )

        with Ledger(self.db) as ledger:
            receipt = ledger.execute(
                {"op": "get_receipt", "tenant": "acme", "request_id": "shared-id"}
            )["result"]
            self.assertEqual(receipt["receipt"]["class"], "ok")

    def test_concurrent_watermark_and_publish(self) -> None:
        """同步点固定顺序：水位先推到 6，之后偏移 3 的发布必须永久空洞拒绝。"""
        _setup_two_partitions(self.db)

        with Ledger(self.db) as ledger:
            ledger.execute(
                {
                    "op": "publish",
                    "request_id": "seed-5",
                    "tenant": "acme",
                    "partition": 0,
                    "offset": 5,
                    "payload": {"order": "A-1"},
                }
            )
            response = ledger.execute(
                {"op": "advance_watermark", "request_id": "seed-wm", "tenant": "acme",
                 "partition": 0, "target": 6}
            )
            self.assertEqual(response["result"]["sealed_holes"], [0, 1, 2, 3, 4])

        late = self.worker(
            "late",
            {
                "steps": [
                    {"step": "late-3", "op": "publish", "request_id": "late-3", "tenant": "acme",
                     "partition": 0, "offset": 3, "payload": {"order": "A-2"}},
                    {"step": "late-6", "op": "publish", "request_id": "late-6", "tenant": "acme",
                     "partition": 0, "offset": 6, "payload": {"order": "A-3"}},
                ]
            },
        )
        results = {r["step"]: r["response"] for r in late.finish()}
        self.assertEqual(results["late-3"]["class"], "rejected")
        self.assertEqual(results["late-3"]["error"]["code"], "WATERMARK_BELOW_HOLE")
        self.assertEqual(results["late-6"]["class"], "ok")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
