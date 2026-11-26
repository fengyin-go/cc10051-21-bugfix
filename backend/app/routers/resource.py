"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services import service_for

router = APIRouter(prefix="/api", tags=["业务资源"])

_RESERVED_QUERY = {"keyword", "status", "page", "size"}


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


def _filters_from_request(request: Request) -> dict[str, str]:
    # 标准字段与领域扩展字段都允许过滤，领域服务会判断字段是否真实存在。
    return {
        key: value.strip()
        for key, value in request.query_params.items()
        if key not in _RESERVED_QUERY and value.strip()
    }


def _action_result(result: dict[str, Any]) -> ActionResult:
    # 领域结果中的 receipt_id / delivery_basis / reconcile 等键全部透传到响应顶层，
    # ActionResult 配置了 extra=allow；同时保留 data 聚合以兼容旧调用。
    payload = dict(result)
    payload.setdefault("message", "")
    extra = {
        k: v for k, v in payload.items()
        if k not in {"ok", "message", "entry", "receipt", "voucher",
                     "block_reason", "reason", "duplicated", "data"}
    }
    if extra:
        payload["data"] = extra
    return ActionResult(**payload)


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=500),
) -> PageResult[dict]:
    service = require_service(module)
    filters = _filters_from_request(request)
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = _filters_from_request(request)
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("﻿")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    result = service.run_action_detailed(entry_id, payload.action.strip(), payload)
    return _action_result(result)


# ---------------------------------------------------------------------------
# 语义化快捷端点：与 /actions 里的中文动作等价，方便不同调用约定对接。
# 注意：固定模块路径放在通用 /{module}/{entry_id} 之后不影响 POST 匹配，
# GET 的多一段路径也不会与两段式路由冲突。
# ---------------------------------------------------------------------------

@router.post("/dist_task/{entry_id}/dispatch", response_model=ActionResult)
def dispatch_task(entry_id: int) -> ActionResult:
    service = require_service("dist_task")
    return _action_result(service.run_action_detailed(entry_id, "开始分发"))


@router.post("/dist_task/{entry_id}/claim", response_model=ActionResult)
def claim_task(entry_id: int) -> ActionResult:
    service = require_service("dist_task")
    return _action_result(service.run_action_detailed(entry_id, "开始分发"))


@router.post("/dist_task/{entry_id}/confirm", response_model=ActionResult)
def confirm_task(entry_id: int) -> ActionResult:
    service = require_service("dist_task")
    return _action_result(service.run_action_detailed(entry_id, "确认送达"))


@router.post("/dist_task/{entry_id}/confirm_delivery", response_model=ActionResult)
def confirm_task_delivery(entry_id: int) -> ActionResult:
    service = require_service("dist_task")
    return _action_result(service.run_action_detailed(entry_id, "确认送达"))


@router.post("/dist_task/{entry_id}/retry", response_model=ActionResult)
def retry_task(entry_id: int) -> ActionResult:
    service = require_service("dist_task")
    return _action_result(service.run_action_detailed(entry_id, "重试分发"))


@router.get("/dist_task/{entry_id}/eligibility")
def task_eligibility(entry_id: int) -> dict[str, Any]:
    from app.services.flow import check_claim_eligibility

    service = require_service("dist_task")
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"分发批次 {entry_id} 不存在")
    result = check_claim_eligibility(entry)
    return {
        "ok": result["ok"],
        "eligible": result["ok"],
        "code": result.get("code"),
        "reason": result.get("reason", ""),
        "block_reason": result.get("reason", ""),
        "version": result.get("version", ""),
    }


@router.post("/version_rollback/{entry_id}/execute", response_model=ActionResult)
def execute_rollback(entry_id: int) -> ActionResult:
    service = require_service("version_rollback")
    return _action_result(service.run_action_detailed(entry_id, "执行回滚"))


@router.post("/dist_receipt/{entry_id}/verify", response_model=ActionResult)
def verify_receipt(entry_id: int) -> ActionResult:
    service = require_service("dist_receipt")
    return _action_result(service.run_action_detailed(entry_id, "核对回执"))


@router.post("/dist_receipt/{entry_id}/open", response_model=ActionResult)
def open_receipt(entry_id: int) -> ActionResult:
    service = require_service("dist_receipt")
    return _action_result(service.run_action_detailed(entry_id, "打开回执"))
