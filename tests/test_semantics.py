"""纯函数与单进程语义测试。

期望值全部由规格直接写出，或由独立的小实现（正则/哈希）重新算出，
不从被测模块反向获取。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import unittest

from solution import Ledger
from solution.errors import (
    BUSINESS_REJECTION_CODES,
    ERROR_CODES,
    INPUT_INVALID_CODES,
    LedgerError,
)
from solution.model import (
    canonical_payload,
    digest_text,
    pattern_matches,
    validate_pattern,
    validate_topic,
)


def expected_digest(payload: dict) -> str:
    """独立于实现：sha256( b"message\\0" + canonical_json + b"\\0" )。"""
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(b"message\x00" + text.encode("utf-8") + b"\x00").hexdigest()


class PatternTest(unittest.TestCase):
    def test_exact_pattern_matches_only_itself(self) -> None:
        self.assertTrue(pattern_matches("a.b.c", "a.b.c"))
        self.assertFalse(pattern_matches("a.b.c", "a.b.d"))
        self.assertFalse(pattern_matches("a.b", "a.b.c"))

    def test_trailing_segment_wildcard(self) -> None:
        self.assertTrue(pattern_matches("a.b.*", "a.b.c"))
        self.assertTrue(pattern_matches("a.b.*", "a.b.anything-here"))
        self.assertFalse(pattern_matches("a.b.*", "a.b"))  # 通配是一个完整段，不匹配空
        self.assertFalse(pattern_matches("a.b.*", "a.b.c.d"))  # 不跨段
        self.assertFalse(pattern_matches("a.*.c", "a.b.c"))  # 中间通配非法
        with self.assertRaises(LedgerError):
            validate_pattern("*")  # 顶层通配在登记时就被拒，不会进入匹配

    def test_validate_pattern_rejects_bad_forms(self) -> None:
        good = ["a.b", "a.b.c", "a.b.*", "x-y.z_1.*"]
        bad = ["a", "a.*.c", "*.b", "a.b*", "a..b", "a.b.", "", "*", "a.b.*.c", "a.b.**"]
        for item in good:
            self.assertEqual(validate_pattern(item), item)
        for item in bad:
            with self.assertRaises(LedgerError) as ctx:
                validate_pattern(item)
            self.assertEqual(ctx.exception.code, "RULE_INVALID", item)

    def test_validate_topic(self) -> None:
        self.assertEqual(validate_topic("a.b"), "a.b")
        for bad in ["", "a", "a..b", ".a", "a.", "a b.c", "a.*", "a/b.c"]:
            with self.assertRaises(LedgerError) as ctx:
                validate_topic(bad)
            self.assertEqual(ctx.exception.code, "TOPIC_INVALID", bad)


class ErrorCodeTableTest(unittest.TestCase):
    def test_codes_partition_cleanly(self) -> None:
        self.assertEqual(
            BUSINESS_REJECTION_CODES | INPUT_INVALID_CODES, set(ERROR_CODES)
        )
        self.assertFalse(BUSINESS_REJECTION_CODES & INPUT_INVALID_CODES)

    def test_digest_is_stable_and_domain_separated(self) -> None:
        payload = {"order": "A-1", "n": 2}
        self.assertEqual(
            digest_text("message", canonical_payload(payload)), expected_digest(payload)
        )
        self.assertNotEqual(
            digest_text("message", canonical_payload(payload)),
            digest_text("request", canonical_payload(payload)),
        )


class LedgerSemanticsTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="ledger-unit-")
        self.db = os.path.join(self._tmp.name, "l.sqlite3")
        self.ledger = Ledger(self.db)
        self.ledger.execute(
            {"op": "declare_partition", "request_id": "s0", "tenant": "t",
             "partition": 0, "topic": "a.b"}
        )
        self.ledger.execute(
            {"op": "declare_partition", "request_id": "s1", "tenant": "t",
             "partition": 1, "topic": "a.c"}
        )
        self.ledger.execute(
            {"op": "register_policy", "request_id": "s2", "tenant": "t", "version": 1,
             "coverage": [{"partition": 0, "effective_offset": 0},
                          {"partition": 1, "effective_offset": 0}],
             "rules": [{"subscriber": "s", "pattern": "a.b", "effect": "allow"}]}
        )

    def tearDown(self) -> None:
        self.ledger.close()
        self._tmp.cleanup()

    def run_op(self, **request) -> dict:
        request.setdefault("tenant", "t")
        return self.ledger.execute(request)

    def test_uncovered_prefix_cannot_be_backfilled(self) -> None:
        """首版生效偏移之前的偏移永远无法覆盖：之后任何版本都会被单调性拦住。"""
        self.ledger.execute(
            {"op": "declare_partition", "request_id": "s20", "tenant": "gap",
             "partition": 0, "topic": "a.b"}
        )
        self.assertEqual(
            self.ledger.execute(
                {"op": "register_policy", "request_id": "g-v1", "tenant": "gap",
                 "version": 1, "coverage": [{"partition": 0, "effective_offset": 10}],
                 "rules": []}
            )["class"],
            "ok",
        )
        uncovered = self.ledger.execute(
            {"op": "publish", "request_id": "g-p5", "tenant": "gap", "partition": 0,
             "offset": 5, "payload": {"n": 1}}
        )
        self.assertEqual(uncovered["error"]["code"], "POLICY_UNCOVERED")
        backfill = self.ledger.execute(
            {"op": "register_policy", "request_id": "g-v2", "tenant": "gap",
             "version": 2, "coverage": [{"partition": 0, "effective_offset": 0}],
             "rules": []}
        )
        self.assertEqual(backfill["error"]["code"], "VERSION_NOT_MONOTONIC")

    def test_partition_declaration_after_policy_is_rejected(self) -> None:
        late = self.run_op(op="declare_partition", request_id="late-p2",
                           partition=2, topic="a.d")
        self.assertEqual(late["class"], "rejected")
        self.assertEqual(late["error"]["code"], "PARTITION_AFTER_POLICY")

    def test_received_message_is_not_a_hole(self) -> None:
        """水位跨过「已收到」的偏移不算空洞；只封未收到的那一个。"""
        for offset in (0, 2):
            self.assertEqual(
                self.run_op(op="publish", request_id=f"p{offset}", partition=0,
                            offset=offset, payload={"n": offset})["class"],
                "ok",
            )
        response = self.run_op(op="advance_watermark", request_id="w1", partition=0, target=3)
        self.assertEqual(response["result"]["sealed_holes"], [1])
        self.assertEqual(response["result"]["sealed_hole_count"], 1)

    def test_watermark_equal_target_is_noop(self) -> None:
        self.run_op(op="advance_watermark", request_id="w1", partition=0, target=2)
        again = self.run_op(op="advance_watermark", request_id="w2", partition=0, target=2)
        self.assertEqual(again["class"], "ok")
        self.assertEqual(again["result"]["advanced"], False)
        self.assertEqual(again["result"]["sealed_hole_count"], 0)

    def test_watermark_requires_a_policy(self) -> None:
        self.ledger.execute(
            {"op": "declare_partition", "request_id": "s9", "tenant": "fresh",
             "partition": 0, "topic": "a.b"}
        )
        response = self.ledger.execute(
            {"op": "advance_watermark", "request_id": "w", "tenant": "fresh",
             "partition": 0, "target": 1}
        )
        self.assertEqual(response["error"]["code"], "POLICY_MISSING")

    def test_publish_does_not_move_watermark(self) -> None:
        self.run_op(op="publish", request_id="p1", partition=0, offset=50,
                    payload={"n": 1})
        state = self.run_op(op="state")["result"]
        self.assertEqual(state["partitions"][0]["watermark"], 0)
        self.assertEqual(state["partitions"][0]["max_offset"], 50)

    def test_policy_effective_offset_equal_to_max_seen_is_rejected(self) -> None:
        self.run_op(op="publish", request_id="p1", partition=0, offset=4,
                    payload={"n": 1})
        response = self.run_op(
            op="register_policy", request_id="v2", version=2,
            coverage=[{"partition": 0, "effective_offset": 4},
                      {"partition": 1, "effective_offset": 0}],
            rules=[],
        )
        self.assertEqual(response["error"]["code"], "VERSION_BELOW_WATERMARK")
        self.assertEqual(response["error"]["details"]["max_seen"], 4)

    def test_sealed_watermark_counts_toward_max_seen(self) -> None:
        """水位 5 但分区没有消息：新策略生效偏移必须 > 4。"""
        self.run_op(op="advance_watermark", request_id="w", partition=0, target=5)
        response = self.run_op(
            op="register_policy", request_id="v2", version=2,
            coverage=[{"partition": 0, "effective_offset": 4},
                      {"partition": 1, "effective_offset": 4}],
            rules=[],
        )
        self.assertEqual(response["error"]["code"], "VERSION_BELOW_WATERMARK")
        self.assertEqual(response["error"]["details"]["max_seen"], 4)
        ok = self.run_op(
            op="register_policy", request_id="v3", version=3,
            coverage=[{"partition": 0, "effective_offset": 5},
                      {"partition": 1, "effective_offset": 5}],
            rules=[],
        )
        self.assertEqual(ok["class"], "ok", ok)

    def test_error_details_are_deterministic(self) -> None:
        """同一业务拒绝在重放时 details 完全一致（含键顺序）。"""
        first = self.run_op(op="register_policy", request_id="v2", version=2,
                            coverage=[{"partition": 0, "effective_offset": 0}], rules=[])
        second = self.run_op(op="register_policy", request_id="v2", version=2,
                            coverage=[{"partition": 0, "effective_offset": 0}], rules=[])
        self.assertEqual(
            json.dumps(first["error"], sort_keys=True),
            json.dumps(second["error"], sort_keys=True),
        )

    def test_receipt_is_written_for_rejections_only_after_normalization(self) -> None:
        bad = self.run_op(op="register_policy", request_id="bad", version=1,
                          coverage="nope", rules=[])
        self.assertEqual(bad["class"], "invalid")
        missing = self.run_op(op="get_receipt", request_id="bad")
        self.assertEqual(missing["class"], "rejected")
        self.assertEqual(missing["error"]["code"], "RECEIPT_NOT_FOUND")

    def test_no_op_receipt_for_reads(self) -> None:
        self.run_op(op="state")
        self.assertEqual(
            self.run_op(op="get_receipt", request_id="")["error"]["code"],
            "REQUEST_ID_INVALID",
        )


class InputContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="ledger-input-")
        self.ledger = Ledger(os.path.join(self._tmp.name, "l.sqlite3"))

    def tearDown(self) -> None:
        self.ledger.close()
        self._tmp.cleanup()

    def test_non_object_and_missing_fields(self) -> None:
        cases = [
            ([], "INVALID_REQUEST"),
            ({}, "INVALID_REQUEST"),
            ({"op": "state"}, "TENANT_INVALID"),
            ({"op": "nope", "tenant": "t"}, "UNKNOWN_OP"),
            ({"op": "state", "tenant": "bad tenant"}, "TENANT_INVALID"),
            ({"op": "publish", "tenant": "t", "partition": 0, "offset": 0}, "REQUEST_ID_INVALID"),
            ({"op": "publish", "tenant": "t", "request_id": "r", "partition": -1,
              "offset": 0, "payload": {}}, "PARTITION_INVALID"),
            ({"op": "publish", "tenant": "t", "request_id": "r", "partition": 0,
              "offset": 1.5, "payload": {}}, "OFFSET_INVALID"),
            ({"op": "publish", "tenant": "t", "request_id": "r", "partition": 0,
              "offset": 0, "payload": [1, 2]}, "PAYLOAD_INVALID"),
        ]
        for request, code in cases:
            response = self.ledger.execute(request)
            self.assertEqual(response["class"], "invalid", request)
            self.assertEqual(response["error"]["code"], code, request)

    def test_service_never_raises_for_bad_input(self) -> None:
        for request in [None, 5, "x", {"op": None}, {"op": "publish"}]:
            response = self.ledger.execute(request)
            self.assertIn(response["class"], ("invalid",), request)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
