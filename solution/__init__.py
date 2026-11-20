"""本地消息授权账本。

对外只暴露 :class:`Ledger`、错误类型与错误码表。
"""

from __future__ import annotations

__all__ = ["Ledger", "LedgerError", "ERROR_CODES", "BUSINESS_REJECTION_CODES"]

from .errors import BUSINESS_REJECTION_CODES, ERROR_CODES, LedgerError
from .service import Ledger
