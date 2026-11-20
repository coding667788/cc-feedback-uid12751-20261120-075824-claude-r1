"""被测试启动的账本工作进程。

用法：
    python -I worker.py REPO_ROOT DB_PATH SPEC_JSON

Spec 是一个 JSON 对象：

    {
      "steps": [
        {"op": "publish", ...},                          # 一条账本请求
        {"touch": "name", "wait_for": "other"},          # 同步点：先触碰 name，再等 other
        {"wait_for": "name"},                            # 纯等待
        {"kill_self": true, "code": 77}                  # 硬退出，模拟宿主被杀
      ],
      "crash_points": ["publish.after_write"],           # 命中即在实现内部硬退出
      "ready": "...", "go": "...", "output": "...", "timeout": 60
    }

就绪后触碰 ready、等 go，再逐步执行；每步响应立即落盘（fsync + rename），
因此「被杀前已响应」与「被杀后能看到」可分别检验。
"""

from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, sys.argv[1])

from solution import Ledger  # noqa: E402


def _touch(path: str) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("1")


def _wait(path: str, timeout: float) -> None:
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if os.path.exists(path):
            return
        time.sleep(0.01)
    raise SystemExit(f"worker 等待超时: {path}")


def main() -> int:
    repo, db_path, spec_path = sys.argv[1], sys.argv[2], sys.argv[3]
    with open(spec_path, "r", encoding="utf-8") as fh:
        spec = json.load(fh)
    workdir = os.path.dirname(os.path.abspath(spec_path))
    sync_dir = spec.get("sync_dir") or workdir
    ready = spec.get("ready") or os.path.join(workdir, "ready")
    go = spec.get("go") or os.path.join(workdir, "go")
    output = spec.get("output") or os.path.join(workdir, "out.json")
    timeout = float(spec.get("timeout", 60))

    results: list[dict] = []

    def flush() -> None:
        tmp = output + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(results, fh, ensure_ascii=False)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, output)

    crash_points = set(spec.get("crash_points", ()))

    def hook(point: str) -> None:
        """在实现内部指定点硬退出，区分「提交前」与「提交后」。"""
        if point in crash_points:
            flush()
            _touch(os.path.join(sync_dir, "crashed." + point))
            os._exit(70)

    ledger = Ledger(db_path, hook=hook)
    _touch(ready)
    _wait(go, timeout)

    try:
        for step in spec["steps"]:
            if "op" in step:
                response = ledger.execute(step)
                results.append({"step": step.get("step"), "response": response})
                flush()
                continue
            if step.get("kill_self"):
                flush()
                os._exit(int(step.get("code", 77)))
            if "touch" in step:
                _touch(os.path.join(sync_dir, step["touch"]))
            if "wait_for" in step:
                _wait(os.path.join(sync_dir, step["wait_for"]), timeout)
    finally:
        try:
            ledger.close()
        except Exception:
            pass

    flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
