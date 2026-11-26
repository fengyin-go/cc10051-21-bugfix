"""金记录发布领域服务：管理金记录版本的发布批次、版本标识、扇出分发与回滚衔接，完成入库动作。

数据链路位置：主数据发布层，负责消费合并产物，把发布版本
扇出给订阅方并衔接版本回滚任务。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services import flow
from app.store import store


class GoldenPublishService(BaseModuleService):
    MODULE = "golden_publish"
    ENTITY = "发布批次"
    OPERATION_TERM = "入库"
    LIST_FIELDS = ["发布批次", "实体编码", "金记录数", "发布时间", "版本标识"]
    REQUIRED_FIELDS = ["发布批次", "实体编码", "金记录数"]
    STATUS_ORDER = ["待发布", "已发布", "已回滚"]
    ACTION_RULES = {"发布批次": "已发布", "回滚批次": "已回滚"}
    NEGATIVE_ACTIONS = ["回滚批次"]

    def run_action_detailed(
        self, entry_id: int, action: str, payload: Any = None
    ) -> dict[str, Any]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": f"{self.ENTITY} {entry_id} 不存在或已归档", "entry": None}
        name = (action or "").strip()
        if name in ("发布批次", "发布", "release"):
            result = self.release_version(entry_id)
            result.setdefault("entry", store.find(self.MODULE, entry_id))
            return result
        if name in ("回滚批次", "撤回版本", "回滚", "rollback"):
            reason = "分发版本撤回"
            if payload is not None:
                remark = getattr(payload, "remark", None)
                if remark is None and isinstance(payload, dict):
                    remark = payload.get("remark") or payload.get("reason")
                reason = str(remark or reason)
            result = self.rollback_version(entry_id, reason)
            result.setdefault("entry", store.find(self.MODULE, entry_id))
            return result
        entry, message = BaseModuleService.run_action(self, entry_id, name)
        return {"ok": entry is not None, "message": message, "entry": entry}

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        detailed = self.run_action_detailed(entry_id, action)
        if detailed.get("ok"):
            return detailed.get("entry"), str(detailed.get("message", ""))
        return None, str(detailed.get("message", "动作未通过"))

    # ---- 业务扩展：金记录发布与分发衔接 ----

    def release_version(self, entry_id: int) -> dict[str, Any]:
        """发布金记录版本，写发布时间与版本标识；已显式给出的版本号（如 v12）保留。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "待发布":
            return {"ok": False, "message": "发布批次不在待发布状态"}
        now = datetime.now(timezone.utc).isoformat()
        version = str(entry.get("版本标识", "") or "").strip()
        if not version:
            version = f"pub-v{entry_id}-{now[:10]}"
        entry["发布时间"] = now
        entry["版本标识"] = version
        entry["status"] = "已发布"
        entry["pending"] = False
        entry["阻断原因"] = ""
        return {"ok": True, "version": version, "released_at": now, "entry": entry}

    def fanout_to_subscribers(self, entry_id: int) -> list[int]:
        """把已发布版本扇出给全部生效订阅方，生成分发批次。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "已发布":
            return []
        tasks = store.rows("dist_task")
        created: list[int] = []
        for sub in store.rows("subscriber_mgr"):
            if sub.get("status") != "已生效":
                continue
            task_id = max((int(r.get("id", 0)) for r in tasks), default=0) + 1
            tasks.append({
                "id": task_id,
                "分发批次": f"dist-{entry_id}-{task_id}",
                "订阅方": sub.get("订阅方标识", ""),
                "记录范围": f"版本 {entry.get('版本标识', '')}",
                "分发状态": "排队中",
                "回执状态": "未回收",
                "status": "排队中",
                "pending": True,
                "abnormal": False,
            })
            created.append(task_id)
        return created

    def rollback_version(self, entry_id: int, reason: str) -> dict[str, Any]:
        """回滚发布版本并触发版本回滚任务，同时先做一次幂等的撤回/更正对账。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "已发布":
            return {"ok": False, "message": "批次未发布，无法回滚"}
        entry["status"] = "已回滚"
        entry["abnormal"] = True
        rollbacks = store.rows("version_rollback")
        rb_id = max((int(r.get("id", 0)) for r in rollbacks), default=0) + 1
        rollbacks.append({
            "id": rb_id,
            "回滚任务": f"rb-{entry_id}-{rb_id}",
            "目标版本": entry.get("版本标识", ""),
            "目标发布ID": entry_id,
            "回滚原因": reason,
            "执行状态": "待执行",
            "影响订阅方": "",
            "status": "待执行",
            "pending": True,
            "abnormal": True,
        })
        # 先对账一次：即便调用方不再执行回滚任务，撤回/更正回执也已可追查；
        # 之后执行回滚时凭幂等键不会重复新增。
        summary = flow.reconcile_version_withdrawal(
            entry, rollback_id=rb_id, reason=reason)
        rb = store.find("version_rollback", rb_id)
        if rb is not None:
            rb["影响订阅方"] = ",".join(summary.get("subscribers", []))
            rb["last_reconcile"] = summary
        return {"ok": True, "rollback_id": rb_id,
                "message": "回滚任务已创建，撤回/更正对账已完成",
                "reconcile": summary, "entry": entry}

    def consume_merge_output(self, entry_id: int) -> list[dict[str, Any]]:
        """读取合并模块产出的金记录，作为发布输入。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return []
        return [m for m in store.rows("golden_merge")
                if m.get("实体编码") == entry.get("实体编码") and m.get("status") == "已合并"]

    def emit_stats_snapshot(self, entry_id: int) -> int:
        """发布完成后向治理看板输黄金记录数快照。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return 0
        stats = store.rows("stats_overview")
        stat_id = max((int(r.get("id", 0)) for r in stats), default=0) + 1
        stats.append({
            "id": stat_id,
            "统计时间": datetime.now(timezone.utc).isoformat(),
            "实体编码": entry.get("实体编码", ""),
            "金记录数": str(entry.get("金记录数", "0")),
            "重复率": "0%",
            "分发延迟": "0",
            "status": "正常",
            "pending": False,
            "abnormal": False,
        })
        return stat_id

    def register_publish_lineage(self, entry_id: int) -> str | None:
        """登记发布批次到分发模块的血缘边。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        edges = store.rows("lineage_graph")
        edge_id = max((int(r.get("id", 0)) for r in edges), default=0) + 1
        edges.append({
            "id": edge_id,
            "血缘边标识": f"edge-pub-{edge_id}",
            "上游节点": entry.get("发布批次", ""),
            "下游节点": "dist_task",
            "变换规则": "版本分发",
            "登记时间": datetime.now(timezone.utc).isoformat(),
            "status": "已登记",
            "pending": False,
            "abnormal": False,
        })
        return f"edge-pub-{edge_id}"
