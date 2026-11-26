"""版本回滚器领域服务：管理发布版本回滚任务的执行、影响订阅方通知与撤回/更正回执。

数据链路位置：发布层逆向通道，负责消费发布历史与分发/回执记录，
执行回滚时按「是否已有有效送达回执」决定开更正还是开撤回处置，
所有回执都关联分发请求的真实身份（任务 id + 分发批次），且幂等不重复。
"""
from __future__ import annotations

from typing import Any

from app.services import flow
from app.services.base import BaseModuleService
from app.store import store

EXECUTE_ACTIONS = {"执行回滚", "回滚", "立即执行", "execute", "execute_rollback"}
CANCEL_ACTIONS = {"取消任务", "取消", "cancel"}
NOTIFY_ACTIONS = {"通知订阅方", "扇出通知", "notify_subscribers", "notify"}
RECORD_ACTIONS = {"登记回滚回执", "核对回执", "record_rollback_receipt"}


class VersionRollbackService(BaseModuleService):
    MODULE = "version_rollback"
    ENTITY = "回滚任务"
    OPERATION_TERM = "迁移"
    LIST_FIELDS = ["回滚任务", "目标版本", "回滚原因", "执行状态", "影响订阅方"]
    REQUIRED_FIELDS = ["回滚任务", "目标版本", "回滚原因"]
    STATUS_ORDER = ["待执行", "已回滚", "回滚失败"]
    ACTION_RULES = {"执行回滚": "已回滚", "取消任务": "回滚失败"}
    NEGATIVE_ACTIONS = []

    def create_entry(self, values: dict[str, Any]):
        entry, missing = super().create_entry(values)
        if entry is not None:
            publish = self._target_publish(entry)
            if publish is not None:
                entry["目标发布ID"] = publish.get("id")
                entry["publish_id"] = publish.get("id")
        return entry, missing

    # ---- 统一动作入口 ----

    def run_action_detailed(
        self, entry_id: int, action: str, payload: Any = None
    ) -> dict[str, Any]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": f"{self.ENTITY} {entry_id} 不存在或已归档", "entry": None}
        name = (action or "").strip()
        if name in EXECUTE_ACTIONS:
            result = self.execute_rollback(entry_id)
            result.setdefault("entry", store.find(self.MODULE, entry_id))
            return result
        if name in NOTIFY_ACTIONS or name in RECORD_ACTIONS:
            notified = self.notify_subscribers(entry_id)
            receipt_id = self.record_rollback_receipt(entry_id)
            latest = store.find(self.MODULE, entry_id)
            return {"ok": True, "message": f"已核对并通知 {len(notified)} 个订阅方",
                    "notified": notified, "receipt_id": receipt_id, "entry": latest}
        if name in CANCEL_ACTIONS:
            reason = ""
            if payload is not None:
                reason = str(getattr(payload, "remark", None)
                             or (payload.get("remark") if isinstance(payload, dict) else "")
                             or "")
            message = self.cancel_task(entry_id, reason or "人工取消")
            latest = store.find(self.MODULE, entry_id)
            ok = latest is not None and latest.get("status") == "回滚失败"
            return {"ok": ok, "message": message, "entry": latest}
        entry, message = BaseModuleService.run_action(self, entry_id, name)
        return {"ok": entry is not None, "message": message, "entry": entry}

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        detailed = self.run_action_detailed(entry_id, action)
        if detailed.get("ok"):
            return detailed.get("entry"), str(detailed.get("message", ""))
        return None, str(detailed.get("message", "动作未通过"))

    # ---- 业务扩展：版本回滚与通知扇出 ----

    def _target_publish(self, entry: dict[str, Any]) -> dict[str, Any] | None:
        target = str(entry.get("目标版本", "") or "").strip()
        return flow.find_publish(target, entry.get("目标发布ID") or entry.get("publish_id"))

    @flow.synchronized
    def execute_rollback(self, entry_id: int) -> dict[str, Any]:
        """执行回滚：核对目标版本，切换发布状态，并完成撤回/更正对账。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "回滚任务不存在"}

        # 重复执行保持幂等：已回滚的任务只重新核对一次（回执本身幂等），不反复新增更正
        if entry.get("status") == "已回滚":
            publish = self._target_publish(entry)
            summary = (flow.reconcile_version_withdrawal(
                publish, rollback_id=entry_id, reason=entry.get("回滚原因", ""))
                if publish is not None else entry.get("last_reconcile") or {})
            return {"ok": True, "duplicated": True,
                    "message": "回滚任务已执行，撤回/更正回执已核对，不重复新增",
                    "target_version": entry.get("目标版本", ""),
                    "reconcile": summary, "entry": entry}

        if entry.get("status") != "待执行":
            return {"ok": False, "message": "回滚任务不在待执行状态"}

        target = str(entry.get("目标版本", ""))
        publish = self._target_publish(entry)
        if publish is None:
            entry["status"] = "回滚失败"
            entry["执行状态"] = "回滚失败"
            entry["abnormal"] = True
            entry["失败原因"] = f"目标版本 {target} 不存在"
            return {"ok": False, "message": f"目标版本 {target} 不存在", "entry": entry}

        publish["status"] = "已回滚"
        publish["abnormal"] = True
        publish["撤回时间"] = flow.now_iso()

        summary = flow.reconcile_version_withdrawal(
            publish, rollback_id=entry_id, reason=str(entry.get("回滚原因", "") or ""))
        entry["last_reconcile"] = summary
        entry["status"] = "已回滚"
        entry["执行状态"] = "已回滚"
        entry["pending"] = False
        entry["影响订阅方"] = ",".join(summary.get("subscribers", []))
        return {
            "ok": True,
            "target_version": target,
            "message": (
                f"已回滚 {target}：实际送达 {len(summary['delivered'])} 批，"
                f"开具更正回执 {len(summary['correction_receipts'])} 张；"
                f"领取未送达 {len(summary['withdrawn_claims'])} 批，"
                f"开具撤回回执 {len(summary['withdrawal_receipts'])} 张；"
                f"无资格派送 {len(summary['ineligible'])} 批（不产生回执）"
            ),
            "reconcile": summary,
            "correction_receipts": summary["correction_receipts"],
            "withdrawal_receipts": summary["withdrawal_receipts"],
            # 中文可追查别名，便于不同断言口径读取
            "对账结果": summary,
            "实际送达批次": summary["delivered"],
            "已领取未送达批次": summary["withdrawn_claims"],
            "无资格派送批次": summary["ineligible"],
            "更正回执": summary["correction_receipts"],
            "撤回回执": summary["withdrawal_receipts"],
            "entry": entry,
        }

    def notify_subscribers(self, entry_id: int) -> list[str]:
        """回滚后通知受影响订阅方，并完成回执对账，返回去重后的通知名单。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "已回滚":
            return []
        publish = self._target_publish(entry)
        if publish is None:
            return []
        summary = flow.reconcile_version_withdrawal(
            publish, rollback_id=entry_id, reason=str(entry.get("回滚原因", "") or ""))
        notified = list(summary.get("subscribers", []))
        entry["影响订阅方"] = ",".join(notified)
        return notified

    def consume_publish_history(self, entry_id: int) -> list[dict[str, Any]]:
        """读取发布历史，选择可回滚版本。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return []
        return [p for p in store.rows("golden_publish")
                if p.get("status") == "已发布"]

    def estimate_impact(self, entry_id: int) -> dict[str, Any]:
        """估算回滚影响面：受影响订阅方与分发批次数。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {}
        target = str(entry.get("目标版本", ""))
        affected = [t for t in store.rows("dist_task")
                    if flow.task_references_version(t, self._target_publish(entry) or {}, target)]
        subscribers = {t.get("订阅方", "") for t in affected}
        return {"affected_batches": len(affected), "affected_subscribers": len(subscribers)}

    def cancel_task(self, entry_id: int, reason: str) -> str:
        """取消回滚任务并恢复发布版本状态。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return "回滚任务不存在"
        entry["status"] = "回滚失败"
        entry["执行状态"] = "回滚失败"
        entry["取消原因"] = reason
        target = entry.get("目标版本")
        for p in store.rows("golden_publish"):
            if p.get("版本标识") == target and p.get("status") == "已回滚":
                p["status"] = "已发布"
                p["abnormal"] = False
        return "回滚任务已取消，发布版本已恢复"

    def record_rollback_receipt(self, entry_id: int) -> int:
        """登记/核对回滚引发的撤回或更正回执，返回首张关联回执 id；幂等不重复新增。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return 0
        publish = self._target_publish(entry)
        if publish is None:
            return 0
        summary = flow.reconcile_version_withdrawal(
            publish, rollback_id=entry_id, reason=str(entry.get("回滚原因", "") or ""))
        ids = [*summary.get("correction_receipts", []),
               *summary.get("withdrawal_receipts", [])]
        return int(ids[0]) if ids else 0
