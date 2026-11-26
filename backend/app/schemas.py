"""通用接口契约；模块字段由领域服务持有，不再为每个模块复制一份模型类。"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

T = TypeVar("T")


class PageResult(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int = 1
    size: int = 20


class ActionResult(BaseModel):
    # 领域动作会返回 receipt_id / receipt_no / voucher / block_reason /
    # delivery_basis / reconcile 等可追查字段，统一允许透传到响应顶层。
    model_config = ConfigDict(extra="allow")

    ok: bool
    message: str = ""
    entry: dict[str, Any] | None = None
    # 以下为常用显式字段（其余键通过 extra=allow 一并输出）
    receipt: dict[str, Any] | None = None
    voucher: str | None = None
    block_reason: str | None = None
    duplicated: bool | None = None
    data: dict[str, Any] | None = None


class EntryPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    remark: str | None = None


class ActionPayload(BaseModel):
    action: str
    remark: str | None = None
