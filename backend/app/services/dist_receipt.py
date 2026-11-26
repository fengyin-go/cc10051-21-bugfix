"""分发回执中心领域服务：管理订阅方分发回执的回收核对、差异标记与重发请求。

数据链路位置：分发层对账端，负责消费分发批次的送达结果，
把回执差异回写给分发任务与订阅方。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services import flow
from app.store import store


class DistReceiptService(BaseModuleService):
    MODULE = "dist_receipt"
    ENTITY = "回执记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["回执编号", "分发批次", "订阅方", "回执状态", "差异说明"]
    REQUIRED_FIELDS = ["回执编号", "分发批次", "订阅方"]
    STATUS_ORDER = ["已回收", "部分回收", "未回收"]
    ACTION_RULES = {"重发回执请求": "部分回收", "标记差异": "未回收"}
    NEGATIVE_ACTIONS = []

    def run_action_detailed(
        self, entry_id: int, action: str, payload: Any = None
    ) -> dict[str, Any]:
        name = (action or "").strip()
        if name in ("核对回执", "核对", "verify", "verify_receipt"):
            result = self.verify_receipt(entry_id)
            result.setdefault("entry", store.find(self.MODULE, entry_id))
            return result
        if name in ("打开", "打开回执", "查看送达依据", "open"):
            result = self.open_delivery(entry_id)
            result.setdefault("entry", store.find(self.MODULE, entry_id))
            return result
        if name == "重发回执请求":
            result = self.resend_request(entry_id)
            result.setdefault("entry", store.find(self.MODULE, entry_id))
            return result
        if name == "标记差异":
            diff = ""
            if payload is not None:
                diff = str(getattr(payload, "remark", None)
                           or (payload.get("remark") if isinstance(payload, dict) else "")
                           or "")
            message = self.mark_diff(entry_id, diff or "人工标记差异")
            entry = store.find(self.MODULE, entry_id)
            return {"ok": True, "message": message, "entry": entry}
        return super().run_action_detailed(entry_id, action, payload)

    # ---- 业务扩展：回执核对与差异处置 ----

    def verify_receipt(self, entry_id: int) -> dict[str, Any]:
        """核对回执：按回执类型与分发任务真实身份校验一致性。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "回执记录不存在"}
        batch = entry.get("分发批次", "")
        tid = entry.get("dist_task_id", entry.get("分发任务ID"))
        task = None
        try:
            if tid not in (None, ""):
                task = store.find("dist_task", int(tid))
        except (TypeError, ValueError):
            task = None
        if task is None and batch:
            task = next((t for t in store.rows("dist_task")
                         if t.get("分发批次") == batch), None)
        rtype = entry.get("回执类型", flow.DELIVERY_RECEIPT)

        if task is None:
            entry["status"] = "未回收"
            entry["差异说明"] = "分发批次不存在或缺少分发任务真实身份"
            return {"ok": False, "message": f"分发批次 {batch} 不存在，回执不可追查"}

        if rtype == flow.DELIVERY_RECEIPT:
            confirmed = (
                task.get("status") == "已送达"
                and (task.get("delivery_receipt_id") == entry.get("id")
                     or task.get("delivery_receipt_no") == entry.get("回执编号")
                     or entry.get("送达依据"))
            )
            if confirmed:
                entry["status"] = "已回收"
                entry["pending"] = False
                return {"ok": True, "message": "送达回执与真实送达确认一致",
                        "receipt": entry, "entry": entry}
            entry["status"] = "部分回收"
            entry["差异说明"] = f"缺少真实送达确认（分发状态为 {task.get('status')}）"
            return {"ok": False, "message": "回执缺少对应的真实送达确认，不能作为送达依据",
                    "entry": entry}

        if rtype == flow.WITHDRAW_RECEIPT:
            consistent = (
                task.get("凭证状态") == "已撤回"
                or (task.get("status") == "分发失败"
                    and task.get("withdraw_receipt_id") == entry.get("id"))
            )
            if consistent:
                entry["status"] = "已回收"
                entry["pending"] = False
                return {"ok": True, "message": "撤回处置与领取凭证作废记录一致",
                        "receipt": entry, "entry": entry}
            return {"ok": False, "message": "撤回回执与任务处置状态不一致", "entry": entry}

        if rtype == flow.CORRECTION_RECEIPT:
            related_id = entry.get("关联回执ID")
            original = store.find("dist_receipt", int(related_id)) if related_id else None
            if original is not None and task.get("status") == "已送达":
                entry["status"] = "已回收"
                entry["pending"] = False
                return {"ok": True, "message": "更正回执关联到真实送达依据，可追查",
                        "receipt": entry, "entry": entry}
            return {"ok": False, "message": "更正回执缺少可关联的原始送达回执", "entry": entry}

        if task.get("status") == "已送达":
            entry["status"] = "已回收"
            entry["pending"] = False
            return {"ok": True, "message": "回执与分发结果一致"}
        entry["status"] = "部分回收"
        entry["差异说明"] = f"分发状态为 {task.get('status')}"
        return {"ok": False, "message": "回执与分发结果不一致"}

    def open_delivery(self, entry_id: int) -> dict[str, Any]:
        """回执中心只允许把真实确认过的送达记录作为「送达依据」打开。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "回执记录不存在"}
        checked = self.verify_receipt(entry_id)
        if entry.get("回执类型", flow.DELIVERY_RECEIPT) != flow.DELIVERY_RECEIPT:
            return {"ok": False, "code": "not_delivery_receipt",
                    "message": "该记录是撤回/更正回执，不是送达确认记录，不能作为送达依据打开",
                    "entry": entry}
        if not checked.get("ok"):
            return {"ok": False, "code": "unverified_delivery",
                    "message": checked.get("message", "回执未经真实送达确认"),
                    "entry": entry}
        return {"ok": True, "message": "真实送达记录", "receipt": entry,
                "delivery_basis": entry.get("送达依据") or entry.get("回执编号"),
                "entry": entry}

    def mark_diff(self, entry_id: int, diff: str) -> str:
        """标记回执差异并把分发任务置为失败重试。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return "回执记录不存在"
        entry["status"] = "未回收"
        entry["差异说明"] = diff
        entry["abnormal"] = True
        batch = entry.get("分发批次", "")
        for task in store.rows("dist_task"):
            if task.get("分发批次") == batch:
                task["status"] = "分发失败"
                task["abnormal"] = True
        return f"差异已标记: {diff}"

    def resend_request(self, entry_id: int) -> dict[str, Any]:
        """重发回执请求并等待订阅方回传。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "回执记录不存在"}
        entry["status"] = "部分回收"
        entry["pending"] = True
        entry["重发时间"] = datetime.now(timezone.utc).isoformat()
        return {"ok": True, "message": "回执请求已重发"}

    def consume_dist_results(self, entry_id: int) -> list[dict[str, Any]]:
        """读取分发模块的送达批次，作为核对输入。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return []
        subscriber = entry.get("订阅方", "")
        return [t for t in store.rows("dist_task")
                if t.get("订阅方") == subscriber and t.get("status") in ("已送达", "分发失败")]

    def stats_by_subscriber(self) -> dict[str, dict[str, int]]:
        """按订阅方统计回执回收情况，供对账看板消费。"""
        stats: dict[str, dict[str, int]] = {}
        for row in store.rows(self.MODULE):
            subscriber = row.get("订阅方", "未知")
            bucket = stats.setdefault(subscriber, {"已回收": 0, "部分回收": 0, "未回收": 0})
            status = row.get("status", "未回收")
            if status in bucket:
                bucket[status] += 1
        return stats

    def notify_receipt_gap(self, entry_id: int) -> str:
        """把回执缺口通知订阅方重新上传。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return "回执记录不存在"
        subscriber = entry.get("订阅方", "")
        for sub in store.rows("subscriber_mgr"):
            if sub.get("订阅方标识") == subscriber:
                sub["pending"] = True
        return f"已通知 {subscriber} 补传回执"
