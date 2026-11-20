"""测试辅助：在独立进程里驱动账本，用文件信号作同步点。

刻意只用文件信号，不用 sleep 猜时序；唯一允许的定时是等待超时。
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time

HELPERS = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(HELPERS))
WORKER = os.path.join(HELPERS, "worker.py")

DEFAULT_TIMEOUT = 60.0


class WorkerTimeout(AssertionError):
    pass


class Worker:
    """一个由本测试启动、且只由本测试终止的账本子进程。"""

    def __init__(
        self, db_path: str, spec: dict, workdir: str, name: str, sync_dir: str | None = None
    ) -> None:
        self.db_path = db_path
        self.workdir = workdir
        self.name = name
        sync_dir = sync_dir or workdir
        self.sync_dir = sync_dir
        self.ready = os.path.join(workdir, f"{name}.ready")
        self._go = os.path.join(workdir, f"{name}.go")
        self.output = os.path.join(workdir, f"{name}.out.json")
        self.spec_path = os.path.join(workdir, f"{name}.spec.json")
        full = dict(spec)
        full.setdefault("ready", self.ready)
        full.setdefault("go", self._go)
        full.setdefault("output", self.output)
        full.setdefault("timeout", DEFAULT_TIMEOUT)
        full.setdefault("sync_dir", sync_dir)
        self.spec = full
        self.proc: subprocess.Popen | None = None

    # ---------------------------------------------------------------- 生命周期

    def start(self) -> "Worker":
        with open(self.spec_path, "w", encoding="utf-8") as fh:
            json.dump(self.spec, fh, ensure_ascii=False)
        self.proc = subprocess.Popen(
            [sys.executable, "-I", WORKER, REPO_ROOT, self.db_path, self.spec_path],
            cwd=self.workdir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if self.spec.get("auto_go", True):
            # 步骤内的文件同步点已经负责排序，默认不再额外闸门。
            self.go()
        return self

    def wait_ready(self, timeout: float = DEFAULT_TIMEOUT) -> None:
        wait_for_file(self.ready, f"{self.name} 未就绪", timeout=timeout)
        if self.proc is not None and self.proc.poll() is not None:
            raise AssertionError(f"{self.name} 就绪前已退出: {self._stderr()}")

    def go(self) -> None:
        _touch(self._go)

    def release(self, name: str) -> None:
        """触碰一个同步点文件（与子进程同一同步目录）。"""
        _touch(os.path.join(self.sync_dir, name))

    @property
    def go_file(self) -> str:
        return os.path.join(self.workdir, "go")

    def file(self, name: str) -> str:
        return os.path.join(self.sync_dir, name)

    def wait_file(self, name: str, timeout: float = DEFAULT_TIMEOUT) -> None:
        wait_for_file(self.file(name), f"{self.name} 未到达同步点 {name}", timeout=timeout)

    def finish(self, timeout: float = DEFAULT_TIMEOUT) -> list[dict]:
        assert self.proc is not None, "worker 未启动"
        try:
            out, err = self.proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.terminate()
            raise WorkerTimeout(f"{self.name} 超时未结束")
        if self.proc.returncode != 0:
            raise AssertionError(f"{self.name} 退出码 {self.proc.returncode}: {err}")
        if not os.path.exists(self.output):
            raise AssertionError(f"{self.name} 没有产出结果文件: {out} {err}")
        with open(self.output, "r", encoding="utf-8") as fh:
            return json.load(fh)

    def finish_any(self, timeout: float = DEFAULT_TIMEOUT) -> int:
        """允许非零退出码（崩溃用例），返回退出码。"""
        assert self.proc is not None, "worker 未启动"
        try:
            self.proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.terminate()
            raise WorkerTimeout(f"{self.name} 超时未结束")
        return self.proc.returncode

    def terminate(self) -> int:
        """只终止本对象启动的子进程（TerminateProcess），绝不按名字找进程。"""
        assert self.proc is not None, "worker 未启动"
        if self.proc.poll() is None:
            self.proc.terminate()
        try:
            self.proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=15)
        return self.proc.returncode

    def _stderr(self) -> str:
        if self.proc is None:
            return ""
        try:
            return (self.proc.stderr.read() or "")[-4000:]
        except Exception:
            return ""


def _touch(path: str) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("1")


def wait_for_file(path: str, message: str, timeout: float = DEFAULT_TIMEOUT) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if os.path.exists(path):
            return
        time.sleep(0.01)
    raise WorkerTimeout(f"{message}（等待 {path}）")


def make_workdir(root: str, name: str) -> str:
    path = os.path.join(root, name)
    os.makedirs(path, exist_ok=True)
    return path
