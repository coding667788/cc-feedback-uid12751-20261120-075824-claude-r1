"""纯数据校验：租户、主题、订阅规则、分区与偏移。

这里不碰数据库，所有判定都是本地可复核的纯函数。
"""

from __future__ import annotations

import hashlib
import json
import re

from .errors import LedgerError

TENANT_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
SEGMENT_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9_-]*\Z")  # 一个「完整段」
WILDCARD = "*"


def require_tenant(value: object) -> str:
    if not isinstance(value, str) or not TENANT_RE.match(value):
        raise LedgerError("TENANT_INVALID", f"租户标识不合法: {value!r}")
    return value


def require_subscriber(value: object) -> str:
    if not isinstance(value, str) or not TENANT_RE.match(value):
        raise LedgerError("SUBSCRIBER_INVALID", f"订阅者标识不合法: {value!r}")
    return value


def validate_topic(value: object) -> str:
    """主题用 '.' 分段，2..16 段；每段是 [A-Za-z0-9][A-Za-z0-9_-]{0,63}。

    至少两段保证「末尾整段通配」总落在某个命名空间之内，不会退化成全局 ``*``。
    """
    if not isinstance(value, str):
        raise LedgerError("TOPIC_INVALID", f"主题必须是字符串: {value!r}")
    return _validate_topic_text(value, "TOPIC_INVALID")


def _validate_topic_text(value: str, code: str) -> str:
    segments = value.split(".")
    if not 2 <= len(segments) <= 16:
        raise LedgerError(code, f"主题段数须在 2..16: {value!r}")
    for seg in segments:
        if not SEGMENT_RE.match(seg):
            raise LedgerError(code, f"主题段不合法: {value!r}（段 {seg!r}）")
    return value


def validate_pattern(value: object) -> str:
    """订阅规则：精确主题，或末尾恰好一个完整段为 '*'。"""
    if not isinstance(value, str):
        raise LedgerError("RULE_INVALID", f"规则必须是字符串: {value!r}")
    segments = value.split(".")
    if not 2 <= len(segments) <= 16:
        raise LedgerError("RULE_INVALID", f"规则段数须在 2..16: {value!r}")
    wild = [i for i, s in enumerate(segments) if s == WILDCARD]
    if wild and wild != [len(segments) - 1]:
        raise LedgerError(
            "RULE_INVALID",
            f"只允许末尾一个完整段通配: {value!r}（通配位置 {wild}）",
        )
    for i, seg in enumerate(segments):
        if seg == WILDCARD:
            continue
        if not SEGMENT_RE.match(seg):
            raise LedgerError("RULE_INVALID", f"规则段不合法: {value!r}（段 {seg!r}）")
    return value


def pattern_matches(pattern: str, topic: str) -> bool:
    """规则是否覆盖该主题。调用前两者都应已通过校验。"""
    p = pattern.split(".")
    t = topic.split(".")
    if len(p) != len(t):
        return False
    for i, seg in enumerate(p):
        if i == len(p) - 1 and seg == WILDCARD:
            return True
        if seg != t[i]:
            return False
    return True


def require_partition(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise LedgerError("PARTITION_INVALID", f"分区必须是整数: {value!r}")
    if value < 0 or value > 1_000_000:
        raise LedgerError("PARTITION_INVALID", f"分区越界: {value!r}")
    return value


def require_offset(value: object, *, field: str = "offset") -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise LedgerError("OFFSET_INVALID", f"{field} 必须是整数: {value!r}")
    if value < 0:
        raise LedgerError("OFFSET_INVALID", f"{field} 不能为负: {value!r}")
    return value


def canonical_payload(payload: object) -> str:
    """把消息体规范化为稳定 JSON 文本，供摘要使用。"""
    if not isinstance(payload, dict):
        raise LedgerError("PAYLOAD_INVALID", "消息体必须是 JSON 对象")
    try:
        return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError) as exc:  # pragma: no cover - 防御性
        raise LedgerError("PAYLOAD_INVALID", f"消息体无法规范化: {exc}") from exc


def digest_text(*parts: str) -> str:
    """以 ``\\0`` 分隔逐段哈希。

    消息摘要固定为 ``digest_text("message", canonical_json)``，
    请求指纹固定为 ``digest_text("request", canonical_json)``，域分离避免混用。
    """
    h = hashlib.sha256()
    for part in parts:
        h.update(part.encode("utf-8"))
        h.update(b"\x00")
    return h.hexdigest()
