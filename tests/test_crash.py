"""真实终止子进程：检验提交前后回执与消息/版本绑定。

只用本测试启动的 Popen 对象来终止进程，从不按进程名查找。
"""

from __future__ import annotations

import os
import tempfile
import unittest

try:
    from helpers import Worker, make_workdir
except ImportError:  # 直接以 tests.test_* 运行时
    from tests.helpers import Worker, make_workdir

from solution import Ledger


def _snapshot(db: str) -> dict:
    """只用代码外的 SQL 读取账本持久状态（独立于实现内部对象）。"""
    import sqlite3

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    try:
        messages = [
            dict(r)
            for r in conn.execute(
                "SELECT partition, offset, digest, policy_version, subscribers "
                "FROM messages ORDER BY partition, offset"
            )
        ]
        receipts = [
            dict(r)
            for r in conn.execute("SELECT request_id, request_key, result FROM receipts")
        ]
        policies = [dict(r) for r in conn.execute("SELECT version FROM policies ORDER BY version")]
        watermarks = [
            dict(r) for r in conn.execute("SELECT partition, watermark FROM watermarks ORDER BY partition")
        ]
        return {
            "messages": messages,
            "receipts": receipts,
            "policies": policies,
            "watermarks": watermarks,
        }
    finally:
        conn.close()


class CrashTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="ledger-crash-")
        self.root = self._tmp.name
        self.db = os.path.join(self.root, "ledger.sqlite3")
        self.workers: list[Worker] = []
        with Ledger(self.db) as ledger:
            for partition, topic in ((0, "orders.eu"), (1, "orders.us")):
                ledger.execute(
                    {
                        "op": "declare_partition",
                        "request_id": f"setup-p{partition}",
                        "tenant": "acme",
                        "partition": partition,
                        "topic": topic,
                    }
                )
            ledger.execute(
                {
                    "op": "register_policy",
                    "request_id": "setup-v1",
                    "tenant": "acme",
                    "version": 1,
                    "coverage": [
                        {"partition": 0, "effective_offset": 0},
                        {"partition": 1, "effective_offset": 0},
                    ],
                    "rules": [{"subscriber": "eu", "pattern": "orders.eu", "effect": "allow"}],
                }
            )

    def tearDown(self) -> None:
        for worker in self.workers:
            if worker.proc is not None and worker.proc.poll() is None:
                worker.terminate()
        self._tmp.cleanup()

    def start(self, name: str, spec: dict) -> Worker:
        workdir = make_workdir(self.root, name)
        w = Worker(self.db, spec, workdir, name, sync_dir=self.root)
        self.workers.append(w)
        return w.start()

    # ------------------------------------------------------------------ 用例

    def test_kill_before_commit_leaves_no_trace(self) -> None:
        """在实现内部「已写行、未提交」处硬退出：重启后既无消息也无回执。"""
        worker = self.start(
            "crash-before",
            {
                "crash_points": ["publish.after_write"],
                "steps": [
                    {"step": "p", "op": "publish", "request_id": "crash-before-1",
                     "tenant": "acme", "partition": 0, "offset": 0,
                     "payload": {"order": "A-1"}},
                ],
            },
        )
        code = worker.finish_any()
        self.assertEqual(code, 70)

        snap = _snapshot(self.db)
        self.assertEqual([m for m in snap["messages"] if m["offset"] >= 0], [])
        self.assertEqual(
            [r for r in snap["receipts"] if r["request_id"].startswith("crash-before")], []
        )

        # 重启后的数据库可直接接纳同一请求，不会看到半成品。
        with Ledger(self.db) as ledger:
            response = ledger.execute(
                {
                    "op": "publish",
                    "request_id": "crash-before-1",
                    "tenant": "acme",
                    "partition": 0,
                    "offset": 0,
                    "payload": {"order": "A-1"},
                }
            )
            self.assertEqual(response["class"], "ok", response)
            self.assertEqual(response["replayed"], False)

    def test_kill_after_commit_keeps_receipt_and_version_binding(self) -> None:
        """在提交后、回包前的崩溃点硬退出：消息、回执、版本绑定都已持久化。"""
        with Ledger(self.db) as ledger:
            ledger.execute(
                {
                    "op": "register_policy",
                    "request_id": "setup-v2",
                    "tenant": "acme",
                    "version": 2,
                    "coverage": [
                        {"partition": 0, "effective_offset": 40},
                        {"partition": 1, "effective_offset": 40},
                    ],
                    "rules": [{"subscriber": "new", "pattern": "orders.*", "effect": "allow"}],
                }
            )

        worker = self.start(
            "crash-after",
            {
                "crash_points": ["publish.after_commit"],
                "steps": [
                    {"step": "p", "op": "publish", "request_id": "crash-after-1",
                     "tenant": "acme", "partition": 0, "offset": 41,
                     "payload": {"order": "A-2"}},
                ],
            },
        )
        code = worker.finish_any()
        self.assertEqual(code, 70)

        snap = _snapshot(self.db)
        self.assertEqual(len(snap["messages"]), 1)
        stored = snap["messages"][0]
        self.assertEqual((stored["partition"], stored["offset"]), (0, 41))
        self.assertEqual(stored["policy_version"], 2)
        self.assertEqual(
            __import__("json").loads(stored["subscribers"]), ["new"]
        )
        crash_receipts = [
            r for r in snap["receipts"] if r["request_id"].startswith("crash-after")
        ]
        self.assertEqual(len(crash_receipts), 1)
        self.assertEqual(crash_receipts[0]["request_id"], "crash-after-1")

        # 重启后：同请求重放必须返回「已持久化的第一次结果」，而不是重算。
        with Ledger(self.db) as ledger:
            replay = ledger.execute(
                {
                    "op": "publish",
                    "request_id": "crash-after-1",
                    "tenant": "acme",
                    "partition": 0,
                    "offset": 41,
                    "payload": {"order": "A-2"},
                }
            )
            self.assertEqual(replay["class"], "ok")
            self.assertEqual(replay["replayed"], True)
            self.assertEqual(replay["result"]["policy_version"], 2)
            self.assertEqual(replay["result"]["subscribers"], ["new"])

            fetched = ledger.execute(
                {"op": "get_message", "tenant": "acme", "partition": 0, "offset": 41}
            )
            self.assertEqual(fetched["result"]["first_request_id"], "crash-after-1")

    def test_kill_mid_watermark_advance_does_not_partially_advance(self) -> None:
        """推进水位时在「已写、未提交」处被杀：重启后水位仍是旧值，封洞没有发生。"""
        with Ledger(self.db) as ledger:
            ledger.execute(
                {
                    "op": "publish",
                    "request_id": "seed-2",
                    "tenant": "acme",
                    "partition": 0,
                    "offset": 2,
                    "payload": {"order": "A-1"},
                }
            )

        worker = self.start(
            "crash-wm",
            {
                "crash_points": ["advance_watermark.after_write"],
                "steps": [
                    {"step": "wm", "op": "advance_watermark", "request_id": "wm-1",
                     "tenant": "acme", "partition": 0, "target": 3},
                ],
            },
        )
        self.assertEqual(worker.finish_any(), 70)

        snap = _snapshot(self.db)
        self.assertEqual(snap["watermarks"], [])
        self.assertNotIn("wm-1", [r["request_id"] for r in snap["receipts"]])

        with Ledger(self.db) as ledger:
            state = ledger.execute({"op": "state", "tenant": "acme"})["result"]
            self.assertEqual(state["partitions"][0]["watermark"], 0)
            # 未提交的推进没有留下永久空洞：偏移 1 仍可发布。
            late = ledger.execute(
                {
                    "op": "publish",
                    "request_id": "late-1",
                    "tenant": "acme",
                    "partition": 0,
                    "offset": 1,
                    "payload": {"order": "A-2"},
                }
            )
            self.assertEqual(late["class"], "ok", late)

    def test_watermark_advance_committed_before_kill_is_durable(self) -> None:
        """提交后被杀：水位与封洞结果、回执都已持久化。"""
        with Ledger(self.db) as ledger:
            ledger.execute(
                {
                    "op": "publish",
                    "request_id": "seed-2",
                    "tenant": "acme",
                    "partition": 0,
                    "offset": 2,
                    "payload": {"order": "A-1"},
                }
            )

        worker = self.start(
            "crash-wm-committed",
            {
                "crash_points": ["advance_watermark.after_commit"],
                "steps": [
                    {"step": "wm", "op": "advance_watermark", "request_id": "wm-1",
                     "tenant": "acme", "partition": 0, "target": 3},
                ],
            },
        )
        self.assertEqual(worker.finish_any(), 70)

        snap = _snapshot(self.db)
        self.assertEqual(snap["watermarks"], [{"partition": 0, "watermark": 3}])
        self.assertIn("wm-1", [r["request_id"] for r in snap["receipts"]])

        with Ledger(self.db) as ledger:
            state = ledger.execute({"op": "state", "tenant": "acme"})["result"]
            partition0 = state["partitions"][0]
            self.assertEqual(partition0["watermark"], 3)
            self.assertEqual(partition0["permanent_holes"], 2)  # 偏移 0、1 被永久封存
            late = ledger.execute(
                {
                    "op": "publish",
                    "request_id": "late-1",
                    "tenant": "acme",
                    "partition": 0,
                    "offset": 1,
                    "payload": {"order": "A-2"},
                }
            )
            self.assertEqual(late["error"]["code"], "WATERMARK_BELOW_HOLE")

    def test_policy_registration_killed_after_write_is_atomic(self) -> None:
        """在策略登记写入后、提交前被杀：重启后版本 2 完全不存在（全成或全拒）。"""
        worker = self.start(
            "crash-policy",
            {
                "crash_points": ["register_policy.after_write"],
                "steps": [
                    {"step": "v2", "op": "register_policy", "request_id": "v2-1",
                     "tenant": "acme", "version": 2,
                     "coverage": [{"partition": 0, "effective_offset": 10},
                                  {"partition": 1, "effective_offset": 10}],
                     "rules": [{"subscriber": "x", "pattern": "orders.*", "effect": "allow"}]},
                ],
            },
        )
        self.assertEqual(worker.finish_any(), 70)

        snap = _snapshot(self.db)
        self.assertEqual([p["version"] for p in snap["policies"]], [1])

        with Ledger(self.db) as ledger:
            response = ledger.execute(
                {
                    "op": "register_policy",
                    "request_id": "v2-1",
                    "tenant": "acme",
                    "version": 2,
                    "coverage": [{"partition": 0, "effective_offset": 10},
                                 {"partition": 1, "effective_offset": 10}],
                    "rules": [{"subscriber": "x", "pattern": "orders.*", "effect": "allow"}],
                }
            )
            self.assertEqual(response["class"], "ok", response)
            self.assertEqual(response["replayed"], False)

    def test_rejected_receipt_is_persisted_across_processes(self) -> None:
        """业务拒绝的回执与业务状态可区分地持久化：另一进程重放仍返回同一拒绝。"""
        with Ledger(self.db) as ledger:
            ledger.execute(
                {"op": "publish", "request_id": "seed-5", "tenant": "acme",
                 "partition": 0, "offset": 5, "payload": {"order": "A-1"}}
            )
            ledger.execute(
                {"op": "advance_watermark", "request_id": "seed-wm", "tenant": "acme",
                 "partition": 0, "target": 6}
            )

        worker = self.start(
            "reject-writer",
            {
                "steps": [
                    {"step": "late", "op": "publish", "request_id": "late-3",
                     "tenant": "acme", "partition": 0, "offset": 3,
                     "payload": {"order": "A-2"}},
                ],
            },
        )
        results = worker.finish()
        self.assertEqual(results[0]["response"]["class"], "rejected")
        self.assertEqual(results[0]["response"]["error"]["code"], "WATERMARK_BELOW_HOLE")

        with Ledger(self.db) as ledger:
            replay = ledger.execute(
                {"op": "publish", "request_id": "late-3", "tenant": "acme",
                 "partition": 0, "offset": 3, "payload": {"order": "A-2"}}
            )
            self.assertEqual(replay["class"], "rejected")
            self.assertEqual(replay["replayed"], True)
            self.assertEqual(replay["error"]["code"], "WATERMARK_BELOW_HOLE")

            receipt = ledger.execute(
                {"op": "get_receipt", "tenant": "acme", "request_id": "late-3"}
            )["result"]
            self.assertEqual(receipt["receipt"]["class"], "rejected")
            self.assertEqual(
                receipt["receipt"]["error"]["code"], "WATERMARK_BELOW_HOLE"
            )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
