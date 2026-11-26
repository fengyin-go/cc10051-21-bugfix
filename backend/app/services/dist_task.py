"""分发任务领域服务：管理把发布版本分发给订阅方的批次调度、领取资格、送达确认与失败重试。

数据链路位置：主数据分发层执行端，负责消费发布版本与订阅过滤，
只有资格核对通过才写领取凭证，只有真实送达确认才把回执写入回执中心。
"""
from __future__ import annotations

from typing import Any

from app.services import flow
from app.services.base import BaseModuleService
from app.store import store

CLAIM_ACTIONS = {"开始分发", "重试分发", "重新分发", "重新办理", "领取", "领取版本", "dispatch", "claim", "retry"}
CONFIRM_ACTIONS = {"确认送达", "送达确认", "确认收货", "confirm", "confirm_delivery", "deliver"}


class DistTaskService(BaseModuleService):
    MODULE = "dist_task"
    ENTITY = "分发批次"
    OPERATION_TERM = "导出"
    LIST_FIELDS = ["分发批次", "订阅方", "记录范围", "分发状态", "回执状态"]
    REQUIRED_FIELDS = ["分发批次", "订阅方", "记录范围"]
    STATUS_ORDER = ["排队中", "分发中", "已送达", "分发失败"]
    ACTION_RULES = {"开始分发": "分发中", "重试分发": "分发中"}
    NEGATIVE_ACTIONS = []

    def create_entry(self, values: dict[str, Any]):
        entry, missing = super().create_entry(values)
        if entry is not None:
            # 登记时就把记录范围解析到具体发布行并钉住身份，
            # 避免同名版本（多个 v12）造成领取/回滚错配。
            publish = flow.resolve_task_version(entry)
            if publish is not None:
                entry["发布ID"] = publish.get("id")
                entry["publish_id"] = publish.get("id")
                if not str(entry.get("版本标识", "") or "").strip():
                    entry["版本标识"] = publish.get("版本标识", "")
        return entry, missing

    def run_action_detailed(
        self, entry_id: int, action: str, payload: Any = None
    ) -> dict[str, Any]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": f"{self.ENTITY} {entry_id} 不存在或已归档", "entry": None}
        name = (action or "").strip()
        if name in CLAIM_ACTIONS:
            result = self.dispatch_batch(entry_id)
            result.setdefault("entry", store.find(self.MODULE, entry_id))
            return result
        if name in CONFIRM_ACTIONS:
            result = self.confirm_delivery(entry_id)
            result.setdefault("entry", store.find(self.MODULE, entry_id))
            return result
        entry, message = BaseModuleService.run_action(self, entry_id, name)
        return {"ok": entry is not None, "message": message, "entry": entry}

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """保留基类二元组契约，内部改走资格核对，避免通用动作绕过领域规则。"""
        detailed = self.run_action_detailed(entry_id, action)
        if detailed.get("ok"):
            return detailed.get("entry"), str(detailed.get("message", ""))
        return None, str(detailed.get("message", "动作未通过"))

    # ---- 业务扩展：版本分发与回执回收 ----

    def dispatch_batch(self, entry_id: int) -> dict[str, Any]:
        """开始分发/重试：先核对待发布、已撤回、不可领与跨实体版本，通过后才写领取凭证。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "分发批次不存在"}
        result = flow.claim_for_task(entry)
        result["subscriber"] = entry.get("订阅方", "")
        result["scope"] = entry.get("记录范围")
        return result

    def confirm_delivery(self, entry_id: int) -> dict[str, Any]:
        """确认送达并向回执中心登记送达回执；同一领取重复确认只登记一次。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "分发批次不存在"}
        return flow.confirm_delivery_for_task(entry)

    def fail_and_requeue(self, entry_id: int, reason: str) -> dict[str, Any]:
        """标记分发失败，保留断点供重试。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "分发批次不存在"}
        entry["status"] = "分发失败"
        entry["分发状态"] = "分发失败"
        entry["abnormal"] = True
        entry["失败原因"] = reason
        return {"ok": False, "message": f"分发失败: {reason}", "retryable": True}

    def consume_publish_scope(self, entry_id: int) -> dict[str, Any] | None:
        """读取发布模块的版本范围，校验分发覆盖。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        return flow.resolve_task_version(entry)

    def resolve_backlog(self, entry_id: int) -> int:
        """按发布批次聚合积压分发任务数。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return 0
        scope = str(entry.get("记录范围", "")).replace("版本 ", "")
        return sum(1 for t in store.rows(self.MODULE)
                   if str(t.get("记录范围", "")).endswith(scope)
                   and t.get("status") in ("排队中", "分发中"))

    def notify_subscriber_rollback(self, entry_id: int, new_version: str) -> str:
        """回滚后通知订阅方新版本口径。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return "分发批次不存在"
        subscriber = entry.get("订阅方", "")
        for sub in store.rows("subscriber_mgr"):
            if sub.get("订阅方标识") == subscriber:
                sub["pending"] = True
        entry["记录范围"] = f"版本 {new_version}"
        entry["版本标识"] = new_version
        entry["status"] = "排队中"
        entry["分发状态"] = "排队中"
        entry["pending"] = True
        return f"已通知 {subscriber} 切换到 {new_version}"
