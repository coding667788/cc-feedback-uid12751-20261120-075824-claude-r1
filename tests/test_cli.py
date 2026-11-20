"""CLI 与 cases.json 的可重放性测试。"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest

from solution.cases import compare, load_cases, run_cases

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CASES_PATH = os.path.join(REPO_ROOT, "cases.json")


class CaseReplayTest(unittest.TestCase):
    def test_all_shipped_cases_pass(self) -> None:
        doc = load_cases(CASES_PATH)
        result = run_cases(doc)
        failed = [c for c in result["cases"] if not c["passed"]]
        detail = []
        for case in failed:
            for step in case["steps"]:
                if not step["match"]:
                    detail.append(
                        f"{case['case_id']}#{step['index']} {step['op']}: "
                        + "; ".join(step["mismatches"])
                    )
        self.assertEqual(failed, [], "\n".join(detail))
        self.assertEqual(result["summary"]["failed"], 0)
        self.assertGreaterEqual(result["summary"]["cases"], 10)

    def test_cases_are_stable_across_runs(self) -> None:
        """同一输入两次运行结果完全一致（逐 step 比对，忽略临时路径）。"""
        doc = load_cases(CASES_PATH)
        first = run_cases(doc)
        second = run_cases(doc)
        self.assertEqual(json.dumps(first, sort_keys=True), json.dumps(second, sort_keys=True))

    def test_case_ids_unique_and_well_formed(self) -> None:
        doc = load_cases(CASES_PATH)
        ids = [c["case_id"] for c in doc["cases"]]
        self.assertEqual(len(ids), len(set(ids)))
        for case in doc["cases"]:
            self.assertEqual(len(case["requests"]), len(case["expect"]))


class CliTest(unittest.TestCase):
    def run_cli(self, *args: str) -> subprocess.CompletedProcess:
        # 注意：-I 会同时忽略 PYTHONPATH 且不把 cwd 放进 sys.path，因此不能用它跑 -m；
        # 这里运行的是本仓库自己的代码，输入路径由参数显式给出。
        return subprocess.run(
            [sys.executable, "-m", "solution", *args],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=180,
        )

    def test_cli_writes_result_file(self) -> None:
        with tempfile.TemporaryDirectory(prefix="ledger-cli-") as tmp:
            out = os.path.join(tmp, "result.json")
            proc = self.run_cli("--input", CASES_PATH, "--output", out)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            with open(out, "r", encoding="utf-8") as fh:
                result = json.load(fh)
            self.assertEqual(result["schema"], "ledger-result/1")
            self.assertEqual(result["summary"]["failed"], 0)
            self.assertEqual(result["failed_case_ids"], [])
            self.assertIn("passed=", proc.stdout)

    def test_cli_rejects_malformed_input(self) -> None:
        with tempfile.TemporaryDirectory(prefix="ledger-cli-") as tmp:
            bad = os.path.join(tmp, "bad.json")
            with open(bad, "w", encoding="utf-8") as fh:
                fh.write("{not json")
            out = os.path.join(tmp, "result.json")
            proc = self.run_cli("--input", bad, "--output", out)
            self.assertEqual(proc.returncode, 2, proc.stderr)
            self.assertFalse(os.path.exists(out))


class CompareHelperTest(unittest.TestCase):
    """比对器本身的语义：子集、类型、长度。"""

    def test_subset_semantics(self) -> None:
        self.assertEqual(compare({"class": "ok", "result": {"a": 1, "b": 2}}, {"class": "ok"}), [])
        self.assertEqual(
            compare({"class": "ok", "result": {"a": 1, "b": 2}}, {"result": {"a": 1}}), []
        )
        problems = compare({"class": "ok", "result": {"a": 1}}, {"result": {"a": 2}})
        self.assertEqual(len(problems), 1)

    def test_list_must_match_length_and_order(self) -> None:
        self.assertTrue(compare({"result": [1, 2]}, {"result": [1]}))
        self.assertTrue(compare({"result": [2, 1]}, {"result": [1, 2]}))
        self.assertFalse(compare({"result": [1, 2]}, {"result": [1, 2]}))

    def test_error_code_mismatch_is_reported(self) -> None:
        problems = compare({"class": "rejected", "error_code": "A"}, {"class": "ok"})
        self.assertTrue(any("class" in p for p in problems))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
