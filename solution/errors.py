"""稳定错误码与异常类型。

错误码一经发布不再改含义，新增只追加；README 与测试均以此表为准。
"""

from __future__ import annotations


class LedgerError(Exception):
    """业务错误，携带稳定错误码与可机读细节。"""

    def __init__(self, code: str, message: str, **details: object) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details

    def to_dict(self) -> dict:
        payload: dict = {"code": self.code, "message": self.message}
        if self.details:
            payload["details"] = dict(self.details)
        return payload

    def __str__(self) -> str:  # pragma: no cover - 调试用
        return f"{self.code}: {self.message}"


ERROR_CODES: dict[str, str] = {
    # 请求结构
    "INVALID_REQUEST": "请求不是 JSON 对象，或缺少 op / tenant",
    "UNKNOWN_OP": "未知操作名",
    "REQUEST_ID_INVALID": "写操作缺少合法 request_id，或读操作的 request_id 类型不对",
    # 租户、主题与分区
    "TENANT_INVALID": "租户标识不合法（字母数字开头，仅含字母数字、'_'、'.'、'-'，1..64）",
    "TENANT_UNKNOWN": "租户尚未登记任何分区",
    "TOPIC_INVALID": "主题不合法：以 '.' 分段，每段字母数字开头，段内仅字母数字、'_'、'-'，2..16 段",
    "PARTITION_INVALID": "分区标识不合法（0..1000000 的整数）",
    "UNKNOWN_PARTITION": "分区未登记",
    "PARTITION_TOPIC_CONFLICT": "同一分区被绑定到另一个主题",
    "PARTITION_AFTER_POLICY": "策略已冻结序言，不能再新增分区",
    "PARTITION_MISSING": "租户尚未登记任何分区，无法登记策略",
    # 订阅规则
    "RULE_INVALID": "订阅规则不合法：只允许精确主题或末尾恰好一个完整段的 '*' 通配",
    "RULE_DUPLICATE": "同一策略版本内重复登记了相同规则",
    "SUBSCRIBER_INVALID": "订阅者标识不合法",
    # 策略版本
    "VERSION_INVALID": "策略版本号必须是正整数",
    "VERSION_CONFLICT": "策略版本必须全租户严格单调递增",
    "VERSION_BELOW_WATERMARK": "新策略在某分区的生效偏移不高于该分区已决的最大偏移",
    "VERSION_NOT_MONOTONIC": "同一分区连续策略版本的生效偏移必须严格递增",
    "COVERAGE_INVALID": "coverage 结构不合法：非空数组，每分区只出现一次，偏移非负",
    "COVERAGE_PARTIAL": "策略切换必须给全部分区登记生效偏移；缺项或多项整次拒绝",
    # 发布
    "PAYLOAD_INVALID": "消息体必须是 JSON 对象",
    "OFFSET_INVALID": "偏移不合法（非负整数）",
    "POLICY_VERSION_INVALID": "policy_version 必须是整数",
    "MESSAGE_CONFLICT": "同一 (租户, 分区, 偏移) 已存在不同内容的消息",
    "WATERMARK_BELOW_HOLE": "目标偏移位于安全水位之下且从未收到，已永久封存",
    "POLICY_UNCOVERED": "没有任何策略版本覆盖该偏移",
    "VERSION_NOT_APPLICABLE": "请求声明的策略版本与覆盖该偏移的实际版本不一致",
    # 水位
    "WATERMARK_REGRESS": "安全水位不可回退",
    # 回执
    "REQUEST_CONFLICT": "同一请求 ID 以不同参数重放",
    "RECEIPT_NOT_FOUND": "没有该请求的回执",
    "MESSAGE_NOT_FOUND": "没有该消息",
}

# 账本把「请求无效」与「业务拒绝」分开：
#   * 业务拒绝是对账本状态的确定性结论，会持久化为回执；
#   * 请求无效在归一化阶段被挡下，不产生回执，也不触碰数据库。
BUSINESS_REJECTION_CODES = frozenset(
    {
        "TENANT_UNKNOWN",
        "UNKNOWN_PARTITION",
        "PARTITION_TOPIC_CONFLICT",
        "PARTITION_AFTER_POLICY",
        "PARTITION_MISSING",
        "RULE_DUPLICATE",
        "VERSION_CONFLICT",
        "VERSION_BELOW_WATERMARK",
        "VERSION_NOT_MONOTONIC",
        "COVERAGE_PARTIAL",
        "MESSAGE_CONFLICT",
        "WATERMARK_BELOW_HOLE",
        "POLICY_UNCOVERED",
        "VERSION_NOT_APPLICABLE",
        "WATERMARK_REGRESS",
        "RECEIPT_NOT_FOUND",
        "MESSAGE_NOT_FOUND",
    }
)

INPUT_INVALID_CODES = frozenset(
    {
        "INVALID_REQUEST",
        "UNKNOWN_OP",
        "REQUEST_ID_INVALID",
        "TENANT_INVALID",
        "TOPIC_INVALID",
        "PARTITION_INVALID",
        "RULE_INVALID",
        "SUBSCRIBER_INVALID",
        "VERSION_INVALID",
        "COVERAGE_INVALID",
        "PAYLOAD_INVALID",
        "OFFSET_INVALID",
        "POLICY_VERSION_INVALID",
        "REQUEST_CONFLICT",
    }
)

assert BUSINESS_REJECTION_CODES | INPUT_INVALID_CODES == set(ERROR_CODES), (
    BUSINESS_REJECTION_CODES | INPUT_INVALID_CODES
) ^ set(ERROR_CODES)
assert not (BUSINESS_REJECTION_CODES & INPUT_INVALID_CODES)
