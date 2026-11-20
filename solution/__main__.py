"""命令行入口：``python -m solution --input cases.json --output result.json``。"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import traceback

from .cases import CaseError, load_cases, run_cases


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m solution", description="本地消息授权账本")
    parser.add_argument("--input", required=True, help="cases.json 路径")
    parser.add_argument("--output", required=True, help="result.json 输出路径")
    parser.add_argument(
        "--workdir",
        default=None,
        help="可选：存放每个 case 的 SQLite 文件（缺省用临时目录）",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        doc = load_cases(args.input)
    except (OSError, json.JSONDecodeError, CaseError) as exc:
        print(f"输入错误: {exc}", file=sys.stderr)
        return 2

    workdir = args.workdir or tempfile.mkdtemp(prefix="ledger-run-")
    try:
        result = run_cases(doc, workdir=workdir)
    except Exception:  # pragma: no cover - 顶层兜底
        traceback.print_exc()
        return 3

    # 先写同目录临时文件并 fsync，再原子替换：中途被杀不会留下半个 result.json。
    tmp_output = args.output + ".tmp"
    with open(tmp_output, "w", encoding="utf-8") as fh:
        json.dump(result, fh, ensure_ascii=False, indent=2, sort_keys=False)
        fh.write("\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp_output, args.output)

    summary = result["summary"]
    print(
        f"cases={summary['cases']} passed={summary['passed']} "
        f"failed={summary['failed']} steps={summary['steps']}"
    )
    if summary["failed"]:
        print("失败用例: " + ", ".join(result["failed_case_ids"]), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
