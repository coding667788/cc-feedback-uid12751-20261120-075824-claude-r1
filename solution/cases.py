"""cases.json 的解析、执行与结果比对。

cases.json 里的 ``expect`` 是人工判定的期望值（非由实现产出）。比对规则：

* ``class`` / ``error_code`` 精确匹配；
* ``result`` 用「子集」语义：期望里出现的字段必须一致，未出现的字段不约束；
  嵌套 dict 递归子集，list 按长度与顺序逐项比对；
* ``error_details`` 同样用子集语义。
"""

from __future__ import annotations

import json
import os
import tempfile
from typing import Any

from .errors import LedgerError
from .service import Ledger

RESULT_SCHEMA = "ledger-result/1"


class CaseError(Exception):
    pass


def load_cases(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        doc = json.load(fh)
    if not isinstance(doc, dict) or not isinstance(doc.get("cases"), list):
        raise CaseError("cases.json 顶层必须是包含 cases 数组的对象")
    seen: set[str] = set()
    for case in doc["cases"]:
        cid = case.get("case_id")
        if not isinstance(cid, str) or not cid:
            raise CaseError(f"case 缺少合法 case_id: {case!r}")
        if cid in seen:
            raise CaseError(f"case_id 重复: {cid}")
        seen.add(cid)
        if not isinstance(case.get("requests"), list):
            raise CaseError(f"{cid}: requests 必须是数组")
        if not isinstance(case.get("expect"), list) or len(case["expect"]) != len(case["requests"]):
            raise CaseError(f"{cid}: expect 必须与 requests 等长")
    return doc


def run_cases(doc: dict, *, workdir: str | None = None) -> dict:
    results = []
    for case in doc["cases"]:
        results.append(_run_case(case, workdir))
    failed = [r for r in results if not r["passed"]]
    return {
        "schema": RESULT_SCHEMA,
        "cases": results,
        "summary": {
            "cases": len(results),
            "passed": len(results) - len(failed),
            "failed": len(failed),
            "steps": sum(len(r["steps"]) for r in results),
        },
        "failed_case_ids": [r["case_id"] for r in failed],
    }


def _run_case(case: dict, workdir: str | None) -> dict:
    cid = case["case_id"]
    default_tenant = case.get("tenant")
    root = workdir or tempfile.mkdtemp(prefix="ledger-case-")
    db_path = os.path.join(root, f"{_safe(cid)}.sqlite3")
    for suffix in ("", "-wal", "-shm", "-journal"):
        try:
            os.remove(db_path + suffix)
        except FileNotFoundError:
            pass

    steps = []
    matched_all = True
    ledger = Ledger(db_path)
    try:
        for idx, (request, expected) in enumerate(zip(case["requests"], case["expect"])):
            payload = dict(request)
            if default_tenant is not None and "tenant" not in payload:
                payload["tenant"] = default_tenant
            observed = _run_request(ledger, payload)
            mismatches = compare(observed, expected)
            if mismatches:
                matched_all = False
            steps.append(
                {
                    "index": idx,
                    "op": payload.get("op"),
                    "request_id": payload.get("request_id"),
                    "observed": observed,
                    "expected": expected,
                    "match": not mismatches,
                    "mismatches": mismatches,
                }
            )
    finally:
        ledger.close()

    return {
        "case_id": cid,
        "description": case.get("description", ""),
        "passed": matched_all,
        "steps": steps,
    }


def _run_request(ledger: Ledger, request: dict) -> dict:
    try:
        response = ledger.execute(request)
    except LedgerError as exc:  # 防御性：execute 已把业务错误转成响应
        return {"class": "invalid", "error_code": exc.code, "error_details": exc.details}
    out = {"class": response["class"], "replayed": response["replayed"]}
    if response["class"] == "ok":
        out["result"] = response.get("result")
        out["error_code"] = None
    else:
        err = response.get("error") or {}
        out["error_code"] = err.get("code")
        out["error_details"] = err.get("details") or {}
    return out


def compare(observed: dict, expected: dict) -> list[str]:
    """返回不匹配项的描述；空列表表示匹配。"""
    problems: list[str] = []
    if "class" in expected and observed.get("class") != expected["class"]:
        problems.append(f"class: 期望 {expected['class']!r} 实得 {observed.get('class')!r}")
    if "error_code" in expected and observed.get("error_code") != expected["error_code"]:
        problems.append(
            f"error_code: 期望 {expected['error_code']!r} 实得 {observed.get('error_code')!r}"
        )
    if "persisted" in expected and observed.get("persisted") != expected["persisted"]:
        problems.append(
            f"persisted: 期望 {expected['persisted']!r} 实得 {observed.get('persisted')!r}"
        )
    if "replayed" in expected and observed.get("replayed") != expected["replayed"]:
        problems.append(
            f"replayed: 期望 {expected['replayed']!r} 实得 {observed.get('replayed')!r}"
        )
    if "result" in expected:
        problems.extend(_subset(observed.get("result"), expected["result"], "result"))
    if "error_details" in expected:
        problems.extend(_subset(observed.get("error_details") or {}, expected["error_details"], "error_details"))
    return problems


def _subset(actual: Any, want: Any, path: str) -> list[str]:
    if isinstance(want, dict):
        if not isinstance(actual, dict):
            return [f"{path}: 期望对象，实得 {type(actual).__name__}"]
        out: list[str] = []
        for key, val in want.items():
            if key not in actual:
                out.append(f"{path}.{key}: 缺失")
            else:
                out.extend(_subset(actual[key], val, f"{path}.{key}"))
        return out
    if isinstance(want, list):
        if not isinstance(actual, list):
            return [f"{path}: 期望数组，实得 {type(actual).__name__}"]
        if len(actual) != len(want):
            return [f"{path}: 期望长度 {len(want)}，实得 {len(actual)}"]
        out = []
        for i, (a, w) in enumerate(zip(actual, want)):
            out.extend(_subset(a, w, f"{path}[{i}]"))
        return out
    if actual != want:
        return [f"{path}: 期望 {want!r} 实得 {actual!r}"]
    return []


def _safe(text: str) -> str:
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in text)[:80]
