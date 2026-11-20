"""授权账本核心逻辑。

不变量（README 有完整表述）：

1. 消息身份 = (租户, 分区, 偏移)；主题在分区登记时绑定，之后不可改。
2. 发布可以乱序，但不推进水位；水位只能单调前进，跨过的缺口即永久空洞。
3. 重复判定先于水位判定：已存在的 (分区, 偏移) 总能拿到首次结果或内容冲突。
4. 策略版本在租户内严格单调；一次切换要么覆盖全部分区，要么整次拒绝。
5. 分区生效偏移必须高于该分区已决的最大偏移，且随版本严格递增。
6. 每条消息提交时冻结其策略版本与订阅者集合，重放永不重算。
7. 请求 ID 在租户内唯一：同参回放已持久化结果，异参拒绝。

响应里的 ``class``：

* ``ok``       —— 成功；写操作的业务结果与回执同事务提交。
* ``rejected`` —— 业务拒绝。写操作会持久化该拒绝的回执，重放原样返回。
* ``invalid``  —— 请求不合法；不写回执，除读之外也不触碰数据库。
"""

from __future__ import annotations

import json
import sqlite3
from typing import Callable

from . import store
from .errors import LedgerError
from .model import (
    canonical_payload,
    digest_text,
    pattern_matches,
    require_offset,
    require_partition,
    require_subscriber,
    require_tenant,
    validate_pattern,
    validate_topic,
)

Hook = Callable[[str], None]

_RULE_EFFECTS = ("allow", "deny")


class Ledger:
    """一个 SQLite 文件对应租户隔离的授权账本库。"""

    def __init__(self, path: str, *, hook: Hook | None = None) -> None:
        self.path = path
        self._hook = hook
        self._conn = store.connect(path)

    # -------------------------------------------------------------- 生命周期

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "Ledger":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False

    def _fire(self, point: str) -> None:
        if self._hook is not None:
            self._hook(point)

    # -------------------------------------------------------------- 请求入口

    def execute(self, request: object) -> dict:
        raw_id = request.get("request_id") if isinstance(request, dict) else None
        raw_op = request.get("op") if isinstance(request, dict) else None
        try:
            req = self._normalize(request)
        except LedgerError as exc:
            return _response(raw_id, raw_op, "invalid", error=exc.to_dict())

        if req["write"]:
            return self._execute_write(req)
        return self._execute_read(req)

    # -------------------------------------------------------------- 归一化

    def _normalize(self, request: object) -> dict:
        if not isinstance(request, dict):
            raise LedgerError("INVALID_REQUEST", "请求必须是 JSON 对象")
        op = request.get("op")
        if not isinstance(op, str):
            raise LedgerError("INVALID_REQUEST", "缺少 op")
        spec = _OPS.get(op)
        if spec is None:
            raise LedgerError("UNKNOWN_OP", f"未知操作: {op!r}")
        tenant = request.get("tenant")
        if not isinstance(tenant, str):
            raise LedgerError("TENANT_INVALID", "缺少 tenant")
        require_tenant(tenant)

        rid = request.get("request_id")
        if spec["write"]:
            if not isinstance(rid, str) or not rid or len(rid) > 128:
                raise LedgerError("REQUEST_ID_INVALID", "写操作必须携带 1..128 字符的 request_id")
        else:
            if rid is not None and not isinstance(rid, str):
                raise LedgerError("REQUEST_ID_INVALID", "request_id 必须是字符串")
            rid = rid or ""

        params = spec["normalize"](request)
        key_material = json.dumps(
            {"op": op, "tenant": tenant, "params": params},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return {
            "op": op,
            "tenant": tenant,
            "request_id": rid,
            "params": params,
            "request_key": digest_text("request", key_material),
            "write": spec["write"],
        }

    # -------------------------------------------------------------- 读路径

    def _execute_read(self, req: dict) -> dict:
        try:
            with store.read_tx(self._conn) as conn:
                result = _READ_HANDLERS[req["op"]](self, conn, req["tenant"], req["params"])
        except LedgerError as exc:
            return _response(req["request_id"], req["op"], "rejected", error=exc.to_dict())
        return _response(req["request_id"], req["op"], "ok", result=result)

    def _read_state(self, conn: sqlite3.Connection, tenant: str, params: dict) -> dict:
        if not _tenant_exists(conn, tenant):
            raise LedgerError("TENANT_UNKNOWN", f"租户不存在: {tenant}")
        parts = []
        for row in conn.execute(
            "SELECT partition, topic FROM tenant_partitions WHERE tenant=? ORDER BY partition",
            (tenant,),
        ).fetchall():
            part = int(row["partition"])
            wm_row = conn.execute(
                "SELECT watermark FROM watermarks WHERE tenant=? AND partition=?",
                (tenant, part),
            ).fetchone()
            wm = int(wm_row["watermark"]) if wm_row is not None else 0
            agg = conn.execute(
                "SELECT COUNT(*) AS n, COALESCE(MAX(offset), -1) AS mx, "
                "COALESCE(SUM(CASE WHEN offset < ? THEN 1 ELSE 0 END), 0) AS below "
                "FROM messages WHERE tenant=? AND partition=?",
                (wm, tenant, part),
            ).fetchone()
            parts.append(
                {
                    "partition": part,
                    "topic": row["topic"],
                    "watermark": wm,
                    "max_offset": int(agg["mx"]),
                    "message_count": int(agg["n"]),
                    "permanent_holes": wm - int(agg["below"]),
                }
            )
        policies = []
        for row in conn.execute(
            "SELECT version FROM policies WHERE tenant=? ORDER BY version", (tenant,)
        ).fetchall():
            version = int(row["version"])
            cov = conn.execute(
                "SELECT partition, effective_offset FROM policy_coverage "
                "WHERE tenant=? AND version=? ORDER BY partition",
                (tenant, version),
            ).fetchall()
            subs = conn.execute(
                "SELECT DISTINCT subscriber FROM policy_rules "
                "WHERE tenant=? AND version=? ORDER BY subscriber",
                (tenant, version),
            ).fetchall()
            policies.append(
                {
                    "version": version,
                    "coverage": {str(int(r["partition"])): int(r["effective_offset"]) for r in cov},
                    "subscribers": [r["subscriber"] for r in subs],
                }
            )
        return {
            "tenant": tenant,
            "partitions": parts,
            "policies": policies,
            "current_policy_version": policies[-1]["version"] if policies else None,
        }

    def _read_receipt(self, conn: sqlite3.Connection, tenant: str, params: dict) -> dict:
        row = store.read_receipt(conn, tenant, params["request_id"])
        if row is None:
            raise LedgerError("RECEIPT_NOT_FOUND", f"没有该回执: {params['request_id']}")
        return {
            "request_id": params["request_id"],
            "request_key": row["request_key"],
            "receipt": json.loads(row["result"]),
        }

    def _read_message(self, conn: sqlite3.Connection, tenant: str, params: dict) -> dict:
        row = conn.execute(
            "SELECT topic, digest, policy_version, subscribers, first_request_id "
            "FROM messages WHERE tenant=? AND partition=? AND offset=?",
            (tenant, params["partition"], params["offset"]),
        ).fetchone()
        if row is None:
            raise LedgerError(
                "MESSAGE_NOT_FOUND",
                f"消息不存在: {tenant}/{params['partition']}/{params['offset']}",
            )
        return {
            "tenant": tenant,
            "partition": params["partition"],
            "offset": params["offset"],
            "topic": row["topic"],
            "digest": row["digest"],
            "policy_version": int(row["policy_version"]),
            "subscribers": json.loads(row["subscribers"]),
            "first_request_id": row["first_request_id"],
        }

    # -------------------------------------------------------------- 写路径

    def _execute_write(self, req: dict) -> dict:
        tenant, rid = req["tenant"], req["request_id"]
        with store.tx(self._conn) as conn:
            existing = store.read_receipt(conn, tenant, rid)
            if existing is not None:
                if existing["request_key"] != req["request_key"]:
                    # 请求 ID 已被另一组参数占用：拒绝，但绝不覆盖已存回执。
                    return _response(
                        rid,
                        req["op"],
                        "rejected",
                        error={
                            "code": "REQUEST_CONFLICT",
                            "message": "同一请求 ID 以不同参数重放",
                            "details": {
                                "stored_request_key": existing["request_key"],
                                "incoming_request_key": req["request_key"],
                            },
                        },
                        replayed=True,
                    )
                stored = json.loads(existing["result"])
                return _response(
                    rid,
                    req["op"],
                    stored["class"],
                    result=stored.get("result"),
                    error=stored.get("error"),
                    replayed=True,
                )

            try:
                with store.savepoint(conn):
                    result = _WRITE_HANDLERS[req["op"]](self, conn, tenant, req)
                payload = json.dumps(
                    {"class": "ok", "result": result}, ensure_ascii=False, sort_keys=True
                )
                store.write_receipt(conn, tenant, rid, req["request_key"], payload)
                cls, err, res = "ok", None, result
            except LedgerError as exc:
                payload = json.dumps(
                    {"class": "rejected", "error": exc.to_dict()},
                    ensure_ascii=False,
                    sort_keys=True,
                )
                store.write_receipt(conn, tenant, rid, req["request_key"], payload)
                cls, err, res = "rejected", exc.to_dict(), None

        # 已提交、尚未回包。此处被终止 ⇔ 重启后业务结果与回执都应存在。
        self._fire(f"{req['op']}.after_commit")
        return _response(rid, req["op"], cls, result=res, error=err)

    # -------------------------------------------------------------- 各操作

    def _declare_partition(self, conn, tenant: str, req: dict) -> dict:
        partition = req["params"]["partition"]
        topic = req["params"]["topic"]
        row = conn.execute(
            "SELECT topic FROM tenant_partitions WHERE tenant=? AND partition=?",
            (tenant, partition),
        ).fetchone()
        if row is not None:
            if row["topic"] != topic:
                raise LedgerError(
                    "PARTITION_TOPIC_CONFLICT",
                    "分区已绑定到另一个主题",
                    partition=partition,
                    bound=row["topic"],
                    requested=topic,
                )
            return {"partition": partition, "topic": topic, "created": False}
        if _policy_count(conn, tenant) > 0:
            raise LedgerError(
                "PARTITION_AFTER_POLICY",
                "策略已冻结序言，新增分区会让既有策略覆盖不完整",
                partition=partition,
            )
        _ensure_tenant(conn, tenant)
        conn.execute(
            "INSERT INTO tenant_partitions(tenant, partition, topic) VALUES (?, ?, ?)",
            (tenant, partition, topic),
        )
        return {"partition": partition, "topic": topic, "created": True}

    def _register_policy(self, conn, tenant: str, req: dict) -> dict:
        p = req["params"]
        version = p["version"]
        coverage = {part: eff for part, eff in p["coverage"]}
        rules = [tuple(r) for r in p["rules"]]

        _ensure_tenant_known(conn, tenant)
        declared = [
            int(r["partition"])
            for r in conn.execute(
                "SELECT partition FROM tenant_partitions WHERE tenant=? ORDER BY partition",
                (tenant,),
            ).fetchall()
        ]
        if not declared:
            raise LedgerError("PARTITION_MISSING", "租户尚未登记任何分区，无法登记策略")

        missing = [x for x in declared if x not in coverage]
        extra = [x for x in coverage if x not in declared]
        if missing or extra:
            raise LedgerError(
                "COVERAGE_PARTIAL",
                "策略切换必须给全部分区登记生效偏移；缺项或多项整次拒绝",
                missing=missing,
                extra=extra,
                declared=declared,
            )

        last_row = conn.execute(
            "SELECT MAX(version) AS v FROM policies WHERE tenant=?", (tenant,)
        ).fetchone()
        last_version = int(last_row["v"]) if last_row["v"] is not None else None
        if last_version is not None and version <= last_version:
            raise LedgerError(
                "VERSION_CONFLICT",
                "策略版本必须全租户严格单调递增",
                version=version,
                last_version=last_version,
            )

        # 先算完所有分区的判定，再写任何一行：一次切换要么全成要么全拒。
        seen_offsets: dict[int, int] = {}
        for part in declared:
            eff = coverage[part]
            wm_row = conn.execute(
                "SELECT watermark FROM watermarks WHERE tenant=? AND partition=?",
                (tenant, part),
            ).fetchone()
            watermark = int(wm_row["watermark"]) if wm_row is not None else 0
            max_msg = int(
                conn.execute(
                    "SELECT COALESCE(MAX(offset), -1) AS m FROM messages "
                    "WHERE tenant=? AND partition=?",
                    (tenant, part),
                ).fetchone()["m"]
            )
            max_seen = max(max_msg, watermark - 1)
            seen_offsets[part] = max_seen
            if eff <= max_seen:
                raise LedgerError(
                    "VERSION_BELOW_WATERMARK",
                    "新策略生效偏移必须高于该分区已决的最大偏移",
                    partition=part,
                    effective_offset=eff,
                    max_seen=max_seen,
                )
            if last_version is not None:
                prev = conn.execute(
                    "SELECT effective_offset FROM policy_coverage "
                    "WHERE tenant=? AND version=? AND partition=?",
                    (tenant, last_version, part),
                ).fetchone()
                if prev is not None and eff <= int(prev["effective_offset"]):
                    raise LedgerError(
                        "VERSION_NOT_MONOTONIC",
                        "同一分区连续策略版本的生效偏移必须严格递增",
                        partition=part,
                        effective_offset=eff,
                        previous_version=last_version,
                        previous_effective_offset=int(prev["effective_offset"]),
                    )

        self._fire("register_policy.before_write")
        conn.execute("INSERT INTO policies(tenant, version) VALUES (?, ?)", (tenant, version))
        for part in declared:
            conn.execute(
                "INSERT INTO policy_coverage(tenant, version, partition, effective_offset) "
                "VALUES (?, ?, ?, ?)",
                (tenant, version, part, coverage[part]),
            )
        for subscriber, pattern, effect in rules:
            conn.execute(
                "INSERT INTO policy_rules(tenant, version, subscriber, pattern, effect) "
                "VALUES (?, ?, ?, ?, ?)",
                (tenant, version, subscriber, pattern, effect),
            )
        self._fire("register_policy.after_write")
        return {
            "version": version,
            "coverage": {str(x): coverage[x] for x in declared},
            "subscribers": sorted({r[0] for r in rules}),
            "rules_count": len(rules),
            "previous_version": last_version,
            "max_seen_before": {str(x): seen_offsets[x] for x in declared},
            "committed": True,
        }

    def _publish(self, conn, tenant: str, req: dict) -> dict:
        p = req["params"]
        partition, offset = p["partition"], p["offset"]
        digest = p["digest"]
        _ensure_tenant_known(conn, tenant)
        part_row = conn.execute(
            "SELECT topic FROM tenant_partitions WHERE tenant=? AND partition=?",
            (tenant, partition),
        ).fetchone()
        if part_row is None:
            raise LedgerError("UNKNOWN_PARTITION", f"分区未登记: {partition}")
        topic = part_row["topic"]
        if p["topic"] is not None and p["topic"] != topic:
            raise LedgerError(
                "PARTITION_TOPIC_CONFLICT",
                "消息主题与分区绑定主题不一致",
                partition=partition,
                bound=topic,
                requested=p["topic"],
            )

        # 1) 历史重复先于水位判定。
        existing = conn.execute(
            "SELECT digest, policy_version, subscribers, first_request_id FROM messages "
            "WHERE tenant=? AND partition=? AND offset=?",
            (tenant, partition, offset),
        ).fetchone()
        if existing is not None:
            if existing["digest"] != digest:
                raise LedgerError(
                    "MESSAGE_CONFLICT",
                    "同一分区偏移已存在不同内容的消息",
                    partition=partition,
                    offset=offset,
                    existing_digest=existing["digest"],
                    incoming_digest=digest,
                    original_request_id=existing["first_request_id"],
                )
            return _publish_result(
                tenant,
                partition,
                offset,
                topic,
                digest,
                int(existing["policy_version"]),
                json.loads(existing["subscribers"]),
                duplicate=True,
                original_request_id=existing["first_request_id"],
            )

        # 2) 水位以下且从未收到：已永久封存。
        wm_row = conn.execute(
            "SELECT watermark FROM watermarks WHERE tenant=? AND partition=?",
            (tenant, partition),
        ).fetchone()
        watermark = int(wm_row["watermark"]) if wm_row is not None else 0
        if offset < watermark:
            raise LedgerError(
                "WATERMARK_BELOW_HOLE",
                "该偏移位于安全水位之下且从未收到，已永久封存",
                partition=partition,
                offset=offset,
                watermark=watermark,
            )

        # 3) 冻结覆盖该偏移的确切策略版本。
        version = _select_version(conn, tenant, partition, offset)
        if version is None:
            raise LedgerError(
                "POLICY_UNCOVERED",
                "没有任何策略版本覆盖该偏移",
                partition=partition,
                offset=offset,
            )
        if p["policy_version"] is not None and p["policy_version"] != version:
            raise LedgerError(
                "VERSION_NOT_APPLICABLE",
                "请求声明的策略版本与覆盖该偏移的实际版本不一致",
                partition=partition,
                offset=offset,
                requested_version=p["policy_version"],
                effective_version=version,
            )

        subscribers = _subscribers_for(conn, tenant, version, topic)
        self._fire("publish.before_write")
        conn.execute(
            "INSERT INTO messages(tenant, partition, offset, topic, digest, "
            "payload_canonical, policy_version, subscribers, first_request_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                tenant,
                partition,
                offset,
                topic,
                digest,
                p["payload_canonical"],
                version,
                json.dumps(subscribers, ensure_ascii=False),
                req["request_id"],
            ),
        )
        self._fire("publish.after_write")
        return _publish_result(
            tenant,
            partition,
            offset,
            topic,
            digest,
            version,
            subscribers,
            duplicate=False,
            original_request_id=req["request_id"],
        )

    def _advance_watermark(self, conn, tenant: str, req: dict) -> dict:
        p = req["params"]
        partition, target = p["partition"], p["target"]
        _ensure_tenant_known(conn, tenant)
        if conn.execute(
            "SELECT 1 FROM tenant_partitions WHERE tenant=? AND partition=?",
            (tenant, partition),
        ).fetchone() is None:
            raise LedgerError("UNKNOWN_PARTITION", f"分区未登记: {partition}")
        if _policy_count(conn, tenant) == 0:
            raise LedgerError("POLICY_MISSING", "没有任何已登记策略，无法推进水位")

        wm_row = conn.execute(
            "SELECT watermark FROM watermarks WHERE tenant=? AND partition=?",
            (tenant, partition),
        ).fetchone()
        current = int(wm_row["watermark"]) if wm_row is not None else 0
        if target < current:
            raise LedgerError(
                "WATERMARK_REGRESS",
                "安全水位不可回退",
                partition=partition,
                current=current,
                target=target,
            )
        if target == current:
            return {
                "partition": partition,
                "previous": current,
                "watermark": current,
                "advanced": False,
                "sealed_holes": [],
                "sealed_hole_count": 0,
                "committed": True,
            }
        # 跳跃推进也允许，但只封「从未收到」的偏移；已收到的照常留在账上。
        present = {
            int(r["offset"])
            for r in conn.execute(
                "SELECT offset FROM messages WHERE tenant=? AND partition=? "
                "AND offset >= ? AND offset < ?",
                (tenant, partition, current, target),
            ).fetchall()
        }
        holes = sorted(set(range(current, target)) - present)
        self._fire("advance_watermark.before_write")
        if wm_row is None:
            conn.execute(
                "INSERT INTO watermarks(tenant, partition, watermark) VALUES (?, ?, ?)",
                (tenant, partition, target),
            )
        else:
            conn.execute(
                "UPDATE watermarks SET watermark=? WHERE tenant=? AND partition=?",
                (target, tenant, partition),
            )
        self._fire("advance_watermark.after_write")
        return {
            "partition": partition,
            "previous": current,
            "watermark": target,
            "advanced": True,
            "sealed_holes": holes[:64],
            "sealed_hole_count": len(holes),
            "committed": True,
        }


# -------------------------------------------------------------------- 辅助函数


def _response(
    request_id: object,
    op: object,
    cls: str,
    *,
    result: object = None,
    error: object = None,
    replayed: bool = False,
) -> dict:
    out = {"request_id": request_id, "op": op, "class": cls, "replayed": replayed}
    if cls == "ok":
        out["result"] = result
    else:
        out["error"] = error
    return out


def _publish_result(
    tenant: str,
    partition: int,
    offset: int,
    topic: str,
    digest: str,
    version: int,
    subscribers: list[str],
    *,
    duplicate: bool,
    original_request_id: str,
) -> dict:
    return {
        "tenant": tenant,
        "partition": partition,
        "offset": offset,
        "topic": topic,
        "digest": digest,
        "policy_version": version,
        "subscribers": subscribers,
        "duplicate": duplicate,
        "original_request_id": original_request_id,
        "committed": True,
    }


def _tenant_exists(conn: sqlite3.Connection, tenant: str) -> bool:
    return conn.execute("SELECT 1 FROM tenants WHERE tenant=?", (tenant,)).fetchone() is not None


def _ensure_tenant(conn: sqlite3.Connection, tenant: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO tenants(tenant, created_at) VALUES (?, 'now')", (tenant,)
    )


def _ensure_tenant_known(conn: sqlite3.Connection, tenant: str) -> None:
    if not _tenant_exists(conn, tenant):
        raise LedgerError("TENANT_UNKNOWN", f"租户不存在: {tenant}")


def _policy_count(conn: sqlite3.Connection, tenant: str) -> int:
    return int(
        conn.execute("SELECT COUNT(*) AS n FROM policies WHERE tenant=?", (tenant,)).fetchone()["n"]
    )


def _select_version(
    conn: sqlite3.Connection, tenant: str, partition: int, offset: int
) -> int | None:
    """覆盖该偏移的确切策略版本：生效偏移最大且不超过 offset。"""
    row = conn.execute(
        "SELECT version FROM policy_coverage "
        "WHERE tenant=? AND partition=? AND effective_offset <= ? "
        "ORDER BY effective_offset DESC, version DESC LIMIT 1",
        (tenant, partition, offset),
    ).fetchone()
    return int(row["version"]) if row else None


def _subscribers_for(
    conn: sqlite3.Connection, tenant: str, version: int, topic: str
) -> list[str]:
    allow: set[str] = set()
    deny: set[str] = set()
    for row in conn.execute(
        "SELECT subscriber, pattern, effect FROM policy_rules WHERE tenant=? AND version=?",
        (tenant, version),
    ).fetchall():
        if not pattern_matches(row["pattern"], topic):
            continue
        (deny if row["effect"] == "deny" else allow).add(row["subscriber"])
    return sorted(allow - deny)


# -------------------------------------------------------------------- 参数归一化


def _norm_declare_partition(request: dict) -> dict:
    return {
        "partition": require_partition(request.get("partition")),
        "topic": validate_topic(request.get("topic")),
    }


def _norm_register_policy(request: dict) -> dict:
    version = request.get("version")
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise LedgerError("VERSION_INVALID", f"策略版本必须是正整数: {version!r}")
    raw_cov = request.get("coverage")
    if not isinstance(raw_cov, list) or not raw_cov:
        raise LedgerError("COVERAGE_INVALID", "coverage 必须是非空数组")
    coverage: dict[int, int] = {}
    for item in raw_cov:
        if not isinstance(item, dict):
            raise LedgerError("COVERAGE_INVALID", "coverage 项必须是对象")
        part = require_partition(item.get("partition"))
        eff = require_offset(item.get("effective_offset"), field="effective_offset")
        if part in coverage:
            raise LedgerError("COVERAGE_INVALID", f"分区重复登记: {part}")
        coverage[part] = eff
    raw_rules = request.get("rules", [])
    if not isinstance(raw_rules, list):
        raise LedgerError("RULE_INVALID", "rules 必须是数组")
    rules: set[tuple[str, str, str]] = set()
    for item in raw_rules:
        if not isinstance(item, dict):
            raise LedgerError("RULE_INVALID", "rule 项必须是对象")
        subscriber = require_subscriber(item.get("subscriber"))
        pattern = validate_pattern(item.get("pattern"))
        effect = item.get("effect")
        if effect not in _RULE_EFFECTS:
            raise LedgerError("RULE_INVALID", f"effect 必须是 allow 或 deny: {effect!r}")
        key = (subscriber, pattern, effect)
        if key in rules:
            raise LedgerError(
                "RULE_DUPLICATE",
                "同一策略版本内重复登记了相同规则",
                subscriber=subscriber,
                pattern=pattern,
                effect=effect,
            )
        rules.add(key)
    return {
        "version": version,
        "coverage": [[x, coverage[x]] for x in sorted(coverage)],
        "rules": [list(r) for r in sorted(rules)],
    }


def _norm_publish(request: dict) -> dict:
    if "payload" not in request:
        raise LedgerError("PAYLOAD_INVALID", "publish 必须携带 payload")
    topic = request.get("topic")
    if topic is not None:
        topic = validate_topic(topic)
    version = request.get("policy_version")
    if version is not None and (isinstance(version, bool) or not isinstance(version, int)):
        raise LedgerError("POLICY_VERSION_INVALID", f"policy_version 必须是整数: {version!r}")
    payload = request["payload"]
    canonical = canonical_payload(payload)
    return {
        "partition": require_partition(request.get("partition")),
        "offset": require_offset(request.get("offset")),
        "topic": topic,
        "payload_canonical": canonical,
        "digest": digest_text("message", canonical),
        "policy_version": version,
    }


def _norm_advance_watermark(request: dict) -> dict:
    return {
        "partition": require_partition(request.get("partition")),
        "target": require_offset(request.get("target"), field="target"),
    }


def _norm_state(request: dict) -> dict:
    return {}


def _norm_get_receipt(request: dict) -> dict:
    rid = request.get("request_id")
    if not isinstance(rid, str) or not rid:
        raise LedgerError("REQUEST_ID_INVALID", "get_receipt 需要 request_id")
    return {"request_id": rid}


def _norm_get_message(request: dict) -> dict:
    return {
        "partition": require_partition(request.get("partition")),
        "offset": require_offset(request.get("offset")),
    }


_OPS: dict[str, dict] = {
    "declare_partition": {"write": True, "normalize": _norm_declare_partition},
    "register_policy": {"write": True, "normalize": _norm_register_policy},
    "publish": {"write": True, "normalize": _norm_publish},
    "advance_watermark": {"write": True, "normalize": _norm_advance_watermark},
    "state": {"write": False, "normalize": _norm_state},
    "get_receipt": {"write": False, "normalize": _norm_get_receipt},
    "get_message": {"write": False, "normalize": _norm_get_message},
}

_READ_HANDLERS = {
    "state": Ledger._read_state,
    "get_receipt": Ledger._read_receipt,
    "get_message": Ledger._read_message,
}

_WRITE_HANDLERS = {
    "declare_partition": Ledger._declare_partition,
    "register_policy": Ledger._register_policy,
    "publish": Ledger._publish,
    "advance_watermark": Ledger._advance_watermark,
}
