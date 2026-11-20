"""SQLite 持久化层。

所有写操作都在 ``BEGIN IMMEDIATE`` 事务里完成，跨进程串行化；
读操作也使用事务，保证读到一致的快照。WAL + synchronous=FULL 使
「提交后被杀」与「提交前被杀」在重启后有明显不同的持久化结果。
"""

from __future__ import annotations

import os
import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS tenants (
    tenant     TEXT PRIMARY KEY,
    created_at TEXT NOT NULL
);

-- 分区与主题的绑定在租户登记时固定，之后不可更改。
CREATE TABLE IF NOT EXISTS tenant_partitions (
    tenant    TEXT NOT NULL,
    partition INTEGER NOT NULL,
    topic     TEXT NOT NULL,
    PRIMARY KEY (tenant, partition),
    FOREIGN KEY (tenant) REFERENCES tenants(tenant)
);

-- 策略版本（冻结序言）。同一租户内版本严格单调。
CREATE TABLE IF NOT EXISTS policies (
    tenant  TEXT NOT NULL,
    version INTEGER NOT NULL,
    PRIMARY KEY (tenant, version),
    FOREIGN KEY (tenant) REFERENCES tenants(tenant)
);

-- 每策略版本对每个分区的生效偏移。
CREATE TABLE IF NOT EXISTS policy_coverage (
    tenant           TEXT NOT NULL,
    version          INTEGER NOT NULL,
    partition        INTEGER NOT NULL,
    effective_offset INTEGER NOT NULL,
    PRIMARY KEY (tenant, version, partition),
    FOREIGN KEY (tenant, version) REFERENCES policies(tenant, version)
);

-- 每策略版本自带的订阅规则；deny 优先于 allow。
CREATE TABLE IF NOT EXISTS policy_rules (
    tenant     TEXT NOT NULL,
    version    INTEGER NOT NULL,
    subscriber TEXT NOT NULL,
    pattern    TEXT NOT NULL,
    effect     TEXT NOT NULL CHECK (effect IN ('allow', 'deny')),
    PRIMARY KEY (tenant, version, subscriber, pattern, effect),
    FOREIGN KEY (tenant, version) REFERENCES policies(tenant, version)
);

-- 分区安全水位：偏移 < watermark 视为已决（收到或永久空洞）。
CREATE TABLE IF NOT EXISTS watermarks (
    tenant    TEXT    NOT NULL,
    partition INTEGER NOT NULL,
    watermark INTEGER NOT NULL,
    PRIMARY KEY (tenant, partition),
    FOREIGN KEY (tenant) REFERENCES tenants(tenant)
);

-- 分区内消息。主键即消息身份 (租户, 分区, 偏移)。
CREATE TABLE IF NOT EXISTS messages (
    tenant              TEXT    NOT NULL,
    partition           INTEGER NOT NULL,
    offset              INTEGER NOT NULL,
    topic               TEXT    NOT NULL,
    digest              TEXT    NOT NULL,
    payload_canonical   TEXT    NOT NULL,
    policy_version      INTEGER NOT NULL,
    subscribers         TEXT    NOT NULL,
    first_request_id    TEXT    NOT NULL,
    PRIMARY KEY (tenant, partition, offset),
    FOREIGN KEY (tenant) REFERENCES tenants(tenant)
);

-- 回执：请求 ID 在租户内唯一；记录请求参数指纹与首次结果。
-- 这里刻意不加 tenants 外键：对未知租户的写请求也要留下拒绝回执，
-- 但绝不能让该请求顺手「登记」出一个租户。
CREATE TABLE IF NOT EXISTS receipts (
    tenant      TEXT NOT NULL,
    request_id  TEXT NOT NULL,
    request_key TEXT NOT NULL,
    result      TEXT NOT NULL,
    PRIMARY KEY (tenant, request_id)
);
"""

BUSY_TIMEOUT_MS = 30000


def connect(path: str) -> sqlite3.Connection:
    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)
    conn = sqlite3.connect(path, timeout=BUSY_TIMEOUT_MS / 1000.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = %d" % BUSY_TIMEOUT_MS)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = FULL")
    conn.execute("PRAGMA foreign_keys = ON")
    _init(conn)
    return conn


def _init(conn: sqlite3.Connection) -> None:
    # executescript 自带提交语义；schema 全部是 IF NOT EXISTS，可重复执行。
    conn.executescript(SCHEMA)


class tx:
    """BEGIN IMMEDIATE 事务上下文；异常时回滚。

    写路径一律用它：BEGIN IMMEDIATE 立刻取写锁，跨进程串行化，
    避免「先读后写」的竞态。
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def __enter__(self) -> sqlite3.Connection:
        self.conn.execute("BEGIN IMMEDIATE")
        return self.conn

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc_type is None:
            self.conn.execute("COMMIT")
        else:
            self.conn.execute("ROLLBACK")
        return False


class read_tx:
    """只读快照：DEFERRED 事务，不阻塞其它读者。"""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def __enter__(self) -> sqlite3.Connection:
        self.conn.execute("BEGIN")
        return self.conn

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc_type is None:
            self.conn.execute("COMMIT")
        else:
            self.conn.execute("ROLLBACK")
        return False


class savepoint:
    """事务内的请求级保存点：业务错误只回滚本条请求的写入。"""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def __enter__(self) -> sqlite3.Connection:
        self.conn.execute("SAVEPOINT request")
        return self.conn

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc_type is None:
            self.conn.execute("RELEASE request")
        else:
            self.conn.execute("ROLLBACK TO request")
            self.conn.execute("RELEASE request")
        return False


def read_receipt(conn: sqlite3.Connection, tenant: str, request_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT request_key, result FROM receipts WHERE tenant=? AND request_id=?",
        (tenant, request_id),
    ).fetchone()


def write_receipt(conn: sqlite3.Connection, tenant: str, request_id: str, key: str, result: str) -> None:
    conn.execute(
        "INSERT INTO receipts(tenant, request_id, request_key, result) VALUES (?, ?, ?, ?)",
        (tenant, request_id, key, result),
    )
