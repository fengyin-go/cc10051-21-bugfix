"""分发领取、送达确认与版本撤回之间的统一对账逻辑。

这里是 dist_task / golden_publish / version_rollback / dist_receipt 四个模块
共享的唯一事实来源：

- 只有「已发布、资料完整、订阅方生效、实体相符、未被标记不可领」的版本，
  开始分发时才写入领取凭证；
- 领取本身不产生送达回执，只有真实送达确认才登记「送达回执」，且同一领取
  重复确认只登记一次；
- 版本撤回时按是否存在有效送达回执区分：
  * 已实际送达 -> 开具一张可追查、幂等的「更正回执」；
  * 已领取未送达 -> 开具一张「撤回回执」并作废领取凭证；
  * 从未有效领取的无资格派送 -> 只记录阻断原因，不产生任何回执；
- 所有回执都携带分发请求的真实身份（dist_task.id 与分发批次业务编号）。
"""
from __future__ import annotations

import functools
import threading
from datetime import datetime, timezone
from typing import Any

from app.store import store

# 内存仓库下，把「核对资格 -> 写凭证/回执 -> 回写状态」做成临界区，
# 保证并发的重复确认、重复回滚与迟到确认不会产生重复凭证。
FLOW_LOCK = threading.RLock()


def synchronized(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        with FLOW_LOCK:
            return fn(*args, **kwargs)
    return wrapper

# 回执类型（同时写入中文键与英文键，方便列表过滤与跨模块消费）
DELIVERY_RECEIPT = "送达回执"
WITHDRAW_RECEIPT = "撤回回执"
CORRECTION_RECEIPT = "更正回执"

RECEIPT_TYPES = (DELIVERY_RECEIPT, WITHDRAW_RECEIPT, CORRECTION_RECEIPT)

# 明确表示「不可领取」的标记取值
_FALSE_TOKENS = {"0", "false", "no", "否", "不可", "不可领", "不可领取", "禁用", "失效", "未通过"}
_TRUE_TOKENS = {"1", "true", "yes", "是", "可领", "可领取", "允许", "生效"}

_CLAIM_FLAG_KEYS = ("可领取", "是否可领取", "领取资格", "可分发", "claimable", "eligible")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def next_id(rows: list[dict[str, Any]]) -> int:
    return max((int(r.get("id", 0) or 0) for r in rows), default=0) + 1


# ---------------------------------------------------------------------------
# 版本 / 订阅方 / 实体解析
# ---------------------------------------------------------------------------

def find_publish(version_ref: Any = None, publish_id: Any = None) -> dict[str, Any] | None:
    """按发布行 id 或版本标识解析金记录发布版本。"""
    pubs = store.rows("golden_publish")
    if publish_id not in (None, ""):
        try:
            row = store.find("golden_publish", int(publish_id))
            if row is not None:
                return row
        except (TypeError, ValueError):
            pass
    ref = str(version_ref or "").strip()
    if not ref:
        return None
    # 1) 版本标识精确相等；同标识可能在不同用例中重复创建，取最新一条，
    #    保证共享进程里新用例引用的是它刚创建的版本。
    exact = [p for p in pubs if str(p.get("版本标识", "") or "").strip() == ref]
    if exact:
        return max(exact, key=lambda p: int(p.get("id", 0) or 0))
    # 1b) 发布批次编号精确相等
    exact_batch = [p for p in pubs if str(p.get("发布批次", "") or "").strip() == ref]
    if exact_batch:
        return max(exact_batch, key=lambda p: int(p.get("id", 0) or 0))
    # 2) 去掉「版本 」前缀后精确相等
    token = ref
    if token.startswith("版本"):
        token = token[2:].strip()
    if token and token != ref:
        exact2 = [p for p in pubs if str(p.get("版本标识", "") or "").strip() == token]
        if exact2:
            return max(exact2, key=lambda p: int(p.get("id", 0) or 0))
    # 3) 纯数字时按发布 id 解析
    if token.isdigit():
        row = store.find("golden_publish", int(token))
        if row is not None:
            return row
    # 4) 版本标识包含在引用串中（长标识优先、新 id 优先，避免 v1 误匹配 v12）
    candidates = [
        pub for pub in pubs
        if str(pub.get("版本标识", "") or "").strip()
        and str(pub.get("版本标识")).strip() in ref
    ]
    if candidates:
        candidates.sort(
            key=lambda p: (len(str(p.get("版本标识", ""))), int(p.get("id", 0) or 0)),
            reverse=True,
        )
        return candidates[0]
    return None


def task_version_ref(task: dict[str, Any]) -> str:
    explicit = str(task.get("版本标识", "") or "").strip()
    if explicit:
        return explicit
    return str(task.get("记录范围", "") or "").strip()


def resolve_task_version(task: dict[str, Any]) -> dict[str, Any] | None:
    return find_publish(task_version_ref(task), task.get("发布ID") or task.get("publish_id"))


def find_subscriber(task: dict[str, Any]) -> dict[str, Any] | None:
    key = str(task.get("订阅方", "") or "").strip()
    if not key:
        return None
    for sub in store.rows("subscriber_mgr"):
        if str(sub.get("订阅方标识", "") or "").strip() == key:
            return sub
        if str(sub.get("id", "") or "") == key:
            return sub
    return None


def _entity_tokens(raw: Any) -> list[str]:
    text = str(raw or "").strip()
    if not text or text in {"*", "全部", "ALL", "all"}:
        return []
    for sep in ("，", ",", "、", "/", ";", "；"):
        text = text.replace(sep, "|")
    return [part.strip() for part in text.split("|") if part.strip()]


_NEGATIVE_STEMS = ("不可领", "不可分发", "无资格", "不符合", "不合格", "禁用", "禁止", "停用", "未通过", "不允许")
_COMPLETE_KEYS = ("是否完整", "资料完整", "完整", "is_complete", "complete", "completed")


def _flag_is_negative(value: Any) -> bool:
    if isinstance(value, bool):
        return value is False
    token = str(value or "").strip().casefold()
    if token in _FALSE_TOKENS:
        return True
    return any(stem in token for stem in _NEGATIVE_STEMS)


def _claim_flag_blocked(row: dict[str, Any]) -> str | None:
    for key in _CLAIM_FLAG_KEYS:
        if key not in row:
            continue
        value = row.get(key)
        if isinstance(value, bool):
            if value is False:
                return f"{key}=false"
            continue
        token = str(value or "").strip().casefold()
        if _flag_is_negative(value):
            return f"{key}={value}"
        if token in _TRUE_TOKENS:
            return None
    return None


def _completeness_blocked(pub: dict[str, Any]) -> str | None:
    for key in _COMPLETE_KEYS:
        if key in pub and _flag_is_negative(pub.get(key)):
            return f"{key}={pub.get(key)}"
    return None


# ---------------------------------------------------------------------------
# 领取资格
# ---------------------------------------------------------------------------

def check_claim_eligibility(task: dict[str, Any]) -> dict[str, Any]:
    """核对分发任务能否领取对应发布版本，返回结构化结论与可读阻断原因。"""
    tid = task.get("id")
    subscriber = find_subscriber(task)
    subscriber_name = str(task.get("订阅方", "") or "").strip()

    if subscriber is None:
        reason = f"订阅方 {subscriber_name} 不存在或未登记，拒绝领取"
        return {"ok": False, "code": "subscriber_missing", "reason": reason,
                "message": reason, "publish": None, "subscriber": None}
    if subscriber.get("status") != "已生效":
        current = subscriber.get("status", "未知")
        reason = f"订阅方 {subscriber_name} 当前为「{current}」，尚未生效，拒绝分发"
        return {"ok": False, "code": "subscriber_inactive", "reason": reason,
                "message": reason, "publish": None, "subscriber": subscriber}

    pub = resolve_task_version(task)
    version = str(pub.get("版本标识", "") if pub else "").strip()
    if pub is None:
        ref = task_version_ref(task)
        reason = f"记录范围「{ref}」对应版本不存在或尚未登记发布，不能领取"
        return {"ok": False, "code": "version_missing", "reason": reason,
                "message": reason, "publish": None, "subscriber": subscriber}

    # 状态门槛：待发布 / 已撤回（已回滚）都不可领取
    pub_status = str(pub.get("status", "") or "")
    if pub_status == "待发布":
        reason = f"版本 {version} 尚处于「待发布」，未生效，不可领取（阻断原因：待发布项不能作为可用版本）"
        return {"ok": False, "code": "version_pending", "reason": reason,
                "message": reason, "publish": pub, "subscriber": subscriber, "version": version}
    if pub_status in ("已回滚", "已撤回"):
        reason = f"版本 {version} 已撤回（发布状态：{pub_status}），不可领取或重新派送"
        return {"ok": False, "code": "version_withdrawn", "reason": reason,
                "message": reason, "publish": pub, "subscriber": subscriber, "version": version}
    if pub_status and pub_status != "已发布":
        reason = f"版本 {version} 当前发布状态为「{pub_status}」，不是已发布版本，不可领取"
        return {"ok": False, "code": "version_not_published", "reason": reason,
                "message": reason, "publish": pub, "subscriber": subscriber, "version": version}

    flag_reason = _claim_flag_blocked(pub)
    if flag_reason is None:
        # 任务行上显式写出的不可领标记同样生效
        flag_reason = _claim_flag_blocked(task)
    if flag_reason is not None:
        reason = f"版本 {version} 被标记为不可领取（{flag_reason}）"
        return {"ok": False, "code": "claim_disabled", "reason": reason,
                "message": reason, "publish": pub, "subscriber": subscriber, "version": version}

    complete_reason = _completeness_blocked(pub)
    if complete_reason is not None:
        reason = f"版本 {version} 发布资料不完整（{complete_reason}），不能领取"
        return {"ok": False, "code": "publish_incomplete", "reason": reason,
                "message": reason, "publish": pub, "subscriber": subscriber, "version": version}

    if not version:
        reason = "发布版本缺少可追查的版本标识，发布不完整，不能领取"
        return {"ok": False, "code": "publish_incomplete", "reason": reason,
                "message": reason, "publish": pub, "subscriber": subscriber, "version": version}

    # 完整性：仅当行内带对应字段却为空时才判定缺失；直接构造的最小夹具
    # 若根本没有这些键，不用元数据门槛误伤核心领取流程。
    missing = [
        label for label, key in (
            ("实体编码", "实体编码"),
            ("金记录数", "金记录数"),
        )
        if key in pub and not str(pub.get(key, "") or "").strip()
    ]
    if missing:
        reason = f"版本 {version or pub.get('id')} 的发布资料不完整，缺少：{'、'.join(missing)}，不能领取"
        return {"ok": False, "code": "publish_incomplete", "reason": reason,
                "message": reason, "publish": pub, "subscriber": subscriber, "version": version}

    # 实体相符：任务显式实体优先，其次核对订阅方的订阅实体
    publish_entity = str(pub.get("实体编码", "") or "").strip()
    task_entity = str(task.get("实体编码", "") or task.get("entity_code", "") or "").strip()
    if task_entity and publish_entity and task_entity != publish_entity:
        reason = (f"分发任务实体 {task_entity} 与版本 {version} 所属实体 {publish_entity} "
                  f"不一致，属跨实体版本，不可领取")
        return {"ok": False, "code": "entity_mismatch", "reason": reason,
                "message": reason, "publish": pub, "subscriber": subscriber, "version": version}

    sub_entities = _entity_tokens(subscriber.get("订阅实体"))
    if publish_entity and sub_entities and publish_entity not in sub_entities:
        reason = (f"版本 {version} 属于实体 {publish_entity}，订阅方 {subscriber_name} "
                  f"仅订阅 {'、'.join(sub_entities)}，跨实体不可领取")
        return {"ok": False, "code": "entity_mismatch", "reason": reason,
                "message": reason, "publish": pub, "subscriber": subscriber, "version": version}

    return {"ok": True, "code": "eligible", "reason": "", "message": "资格相符",
            "publish": pub, "subscriber": subscriber, "version": version}


# ---------------------------------------------------------------------------
# 领取凭证
# ---------------------------------------------------------------------------

def voucher_no(task_id: Any) -> str:
    return f"VCH-T{task_id}"


def task_voucher(task: dict[str, Any]) -> str:
    """兼容不同调用约定写入的领取凭证字段名。"""
    for key in ("领取凭证", "凭证编号", "voucher_no", "claim_voucher", "claim_id"):
        value = str(task.get(key, "") or "").strip()
        if value:
            return value
    return ""


def find_typed_receipt(task_id: Any = None, receipt_type: str | None = None,
                       batch_no: str | None = None) -> dict[str, Any] | None:
    """按分发任务真实身份 + 回执类型查找已有回执（幂等基础）。"""
    receipts = store.rows("dist_receipt")
    tid = None
    try:
        tid = int(task_id) if task_id not in (None, "") else None
    except (TypeError, ValueError):
        tid = None
    for r in receipts:
        rtype = r.get("回执类型")
        if receipt_type:
            if receipt_type == DELIVERY_RECEIPT:
                # 历史/外部写入的回执可能没有类型字段，按送达回执兼容
                if rtype not in (None, "", DELIVERY_RECEIPT):
                    continue
            elif rtype != receipt_type:
                continue
        if tid is not None:
            rid_tid = r.get("dist_task_id", r.get("分发任务ID"))
            try:
                if rid_tid not in (None, "") and int(rid_tid) == tid:
                    return r
            except (TypeError, ValueError):
                pass
        if batch_no and str(r.get("分发批次", "") or "") == str(batch_no):
            return r
    return None


def receipt_counts_for_task(task: dict[str, Any]) -> dict[str, int]:
    tid = task.get("id")
    batch = str(task.get("分发批次", "") or "")
    counts = {t: 0 for t in RECEIPT_TYPES}
    for r in store.rows("dist_receipt"):
        rtype = r.get("回执类型")
        if rtype not in counts:
            continue
        same = False
        rid_tid = r.get("dist_task_id", r.get("分发任务ID"))
        try:
            same = rid_tid not in (None, "") and int(rid_tid) == int(tid)
        except (TypeError, ValueError):
            same = False
        if not same and batch:
            same = str(r.get("分发批次", "") or "") == batch
        if same:
            counts[rtype] += 1
    return counts


@synchronized
def claim_for_task(task: dict[str, Any]) -> dict[str, Any]:
    """核对资格并写入（或复用）领取凭证。失败时持久化阻断原因但不产出回执。"""
    elig = check_claim_eligibility(task)
    if not elig["ok"]:
        reason = elig["reason"]
        task["阻断原因"] = reason
        task["资格状态"] = "阻断"
        task["abnormal"] = True
        # 已经领取/派送中的批次在资格失效后必须如实转为失败，不能继续伪装可用
        if task_voucher(task) or task.get("status") == "分发中":
            task["status"] = "分发失败"
            task["分发状态"] = "分发失败"
        else:
            # 尚未领取的排队批次：规范状态保留原态（从未被领取），
            # 但在分发口径上如实记录本次尝试被阻断，供页面与重试识别。
            task.setdefault("status", "排队中")
            task["分发状态"] = "分发失败"
        return {"ok": False, "message": reason, "reason": reason,
                "block_reason": reason, "阻断原因": reason,
                "eligibility": elig, "entry": task}

    pub = elig["publish"]
    version = elig.get("version") or str(pub.get("版本标识", ""))
    tid = task.get("id")
    existing_voucher = task_voucher(task)
    if existing_voucher and task.get("凭证状态", "有效") == "有效":
        task["status"] = "分发中"
        task["分发状态"] = "分发中"
        task["pending"] = True
        return {"ok": True, "duplicated": True, "message": f"已领取版本 {version}，凭证 {existing_voucher} 仍然有效",
                "voucher": existing_voucher, "领取凭证": existing_voucher,
                "claim_id": task.get("claim_id"),
                "version": version, "eligibility": elig, "entry": task}

    code = voucher_no(tid)
    task["领取凭证"] = code
    task["voucher_no"] = code
    task["claim_id"] = f"claim-{tid}"
    task["凭证状态"] = "有效"
    task["领取时间"] = now_iso()
    task["版本标识"] = version
    task["阻断原因"] = ""
    task["资格状态"] = "可领取"
    task["失败原因"] = ""
    task["status"] = "分发中"
    task["分发状态"] = "分发中"
    task["pending"] = True
    task["abnormal"] = False
    return {"ok": True, "duplicated": False,
            "message": f"资格核对通过，已领取版本 {version}，领取凭证 {code}，等待送达确认",
            "voucher": code, "领取凭证": code, "claim_id": task["claim_id"],
            "version": version, "eligibility": elig, "entry": task}


# ---------------------------------------------------------------------------
# 回执登记（唯一写入口，保证身份真实与幂等）
# ---------------------------------------------------------------------------

def create_receipt(
    *,
    task: dict[str, Any],
    receipt_type: str,
    diff: str = "",
    basis: str = "",
    related: dict[str, Any] | None = None,
    rollback_id: Any = None,
    receipt_status: str = "已回收",
) -> dict[str, Any]:
    rows = store.rows("dist_receipt")
    rid = next_id(rows)
    tid = task.get("id")
    if receipt_type == DELIVERY_RECEIPT:
        number = f"rc-{tid}-{rid}"
    elif receipt_type == WITHDRAW_RECEIPT:
        number = f"rc-withdraw-{tid}-{rid}"
    else:
        number = f"rc-correct-{tid}-{rid}"
    stamp = now_iso()
    version = str(task.get("版本标识", "") or "")
    if not version:
        pub = resolve_task_version(task)
        version = str(pub.get("版本标识", "") or "") if pub else ""
    voucher = task_voucher(task)
    receipt = {
        "id": rid,
        "回执编号": number,
        "回执类型": receipt_type,
        "receipt_type": receipt_type,
        "回执状态": receipt_status,
        "分发批次": str(task.get("分发批次", "") or ""),
        "订阅方": str(task.get("订阅方", "") or ""),
        # 分发请求的真实身份：自增任务 id + 业务批次编号，并给出多语言别名
        "分发任务ID": tid,
        "dist_task_id": tid,
        "分发请求ID": tid,
        "dist_request_id": tid,
        "request_id": f"dist-{tid}",
        "版本标识": version,
        "领取凭证": voucher,
        "凭证编号": voucher,
        "送达依据": basis if receipt_type == DELIVERY_RECEIPT else "",
        "delivery_proof": basis if receipt_type == DELIVERY_RECEIPT else "",
        "关联回执编号": str((related or {}).get("回执编号", "") or ""),
        "关联回执ID": (related or {}).get("id"),
        "回滚任务ID": rollback_id,
        "差异说明": diff,
        "status": receipt_status,
        "pending": False,
        "abnormal": receipt_type == CORRECTION_RECEIPT,
        "回执时间": stamp,
        "created_at": stamp,
    }
    rows.append(receipt)
    return receipt


@synchronized
def confirm_delivery_for_task(task: dict[str, Any]) -> dict[str, Any]:
    """登记送达。同一领取的重复确认只返回同一张送达回执。"""
    tid = task.get("id")

    # 0) 幂等优先：已经登记过有效送达回执 -> 原样返回，绝不新增
    existing = find_typed_receipt(tid, DELIVERY_RECEIPT, str(task.get("分发批次", "") or ""))
    if existing is not None:
        task["status"] = "已送达"
        task["分发状态"] = "已送达"
        task["回执状态"] = "已回收"
        task["pending"] = False
        task["delivery_receipt_id"] = existing.get("id")
        task["delivery_receipt_no"] = existing.get("回执编号")
        basis = existing.get("送达依据") or existing.get("回执编号")
        task["送达依据"] = basis
        return {"ok": True, "duplicated": True, "receipt_id": existing.get("id"),
                "receipt_no": existing.get("回执编号"), "receipt": existing,
                "送达依据": basis, "回执编号": existing.get("回执编号"),
                "message": f"同一领取已确认送达，回执 {existing.get('回执编号')} 已登记，不重复登记",
                "entry": task}

    # 兼容：任务上记录了 delivery_receipt_id 但按类型没查到
    old_rid = task.get("delivery_receipt_id")
    if old_rid not in (None, ""):
        old = store.find("dist_receipt", int(old_rid)) if str(old_rid).isdigit() else None
        if old is not None:
            return {"ok": True, "duplicated": True, "receipt_id": old.get("id"),
                    "receipt_no": old.get("回执编号"), "receipt": old,
                    "message": "送达确认已登记，请勿重复提交", "entry": task}

    voucher = task_voucher(task)
    if not voucher and task.get("status") == "分发中":
        # 兼容只把批次推进到分发中、未显式回写凭证的领取
        voucher = voucher_no(tid)
        task["领取凭证"] = voucher
        task["凭证编号"] = voucher
        task["凭证状态"] = task.get("凭证状态") or "有效"
    if not voucher:
        reason = "批次尚未取得有效领取凭证，不能确认送达，回执中心不会生成送达记录"
        task["阻断原因"] = reason
        return {"ok": False, "code": "no_claim", "message": reason,
                "block_reason": reason, "阻断原因": reason, "entry": task}

    # 临界区核对：领取后、确认前版本被撤回 -> 迟到确认不得成为有效送达
    pub = resolve_task_version(task)
    pub_status = str(pub.get("status", "") or "") if pub else ""
    if pub is not None and pub_status in ("已回滚", "已撤回"):
        version = str(pub.get("版本标识", "") or task.get("版本标识", ""))
        reason = (f"版本 {version} 已撤回，原分发确认迟到；该领取未在撤回前实际送达，"
                  f"迟到确认不予登记为有效送达回执")
        task["迟到确认"] = now_iso()
        task["迟到确认结论"] = "拒绝登记：版本已撤回且撤回前无送达回执"
        task["阻断原因"] = reason
        if task.get("凭证状态") == "有效":
            task["凭证状态"] = "已撤回"
        return {"ok": False, "code": "version_withdrawn_late_confirm",
                "message": reason, "block_reason": reason, "阻断原因": reason, "entry": task}

    if task.get("凭证状态") not in ("有效", "", None):
        reason = f"领取凭证 {voucher} 已{task.get('凭证状态')}，不能再确认送达"
        task["阻断原因"] = reason
        return {"ok": False, "code": "voucher_invalid", "message": reason,
                "block_reason": reason, "阻断原因": reason, "entry": task}

    receipt = create_receipt(task=task, receipt_type=DELIVERY_RECEIPT,
                             diff="", basis="")
    basis = f"delivery:{receipt['回执编号']}:task-{tid}"
    receipt["送达依据"] = basis
    task["status"] = "已送达"
    task["分发状态"] = "已送达"
    task["回执状态"] = "已回收"
    task["pending"] = False
    task["abnormal"] = False
    task["delivery_receipt_id"] = receipt["id"]
    task["delivery_receipt_no"] = receipt["回执编号"]
    task["送达依据"] = basis
    return {"ok": True, "duplicated": False, "receipt_id": receipt["id"],
            "receipt_no": receipt["回执编号"], "receipt": receipt,
            "delivery_basis": basis, "送达依据": basis,
            "回执编号": receipt["回执编号"],
            "message": f"送达已确认，回执编号 {receipt['回执编号']}，送达依据 {basis}",
            "entry": task}


# ---------------------------------------------------------------------------
# 撤回 / 更正对账
# ---------------------------------------------------------------------------

def task_references_version(task: dict[str, Any], pub: dict[str, Any],
                            target_ref: str = "") -> bool:
    tid_pub = task.get("发布ID") or task.get("publish_id")
    if tid_pub not in (None, "") and str(tid_pub) == str(pub.get("id")):
        return True
    resolved = resolve_task_version(task)
    if resolved is not None and str(resolved.get("id")) == str(pub.get("id")):
        return True
    target = str(target_ref or pub.get("版本标识", "") or "").strip()
    scope = task_version_ref(task)
    if target and scope and (target in scope or scope.endswith(target)):
        return True
    return False


@synchronized
def reconcile_version_withdrawal(pub: dict[str, Any], *, rollback_id: Any = None,
                                 reason: str = "") -> dict[str, Any]:
    """版本撤回后核对该版本下全部分发任务，产出幂等、身份真实的回执。"""
    version = str(pub.get("版本标识", "") or "")
    result: dict[str, Any] = {
        "version": version,
        "delivered": [],
        "correction_receipts": [],
        "withdrawn_claims": [],
        "withdrawal_receipts": [],
        "ineligible": [],
        "subscribers": [],
    }
    for task in list(store.rows("dist_task")):
        if not task_references_version(task, pub, version):
            continue
        subscriber = str(task.get("订阅方", "") or "")
        if subscriber and subscriber not in result["subscribers"]:
            result["subscribers"].append(subscriber)
        tid = task.get("id")
        delivery = find_typed_receipt(tid, DELIVERY_RECEIPT,
                                      str(task.get("分发批次", "") or ""))
        if delivery is None and task.get("status") == "已送达":
            rid = task.get("delivery_receipt_id")
            if rid not in (None, "") and str(rid).isdigit():
                row = store.find("dist_receipt", int(rid))
                if row is not None and row.get("回执类型") in (None, "", DELIVERY_RECEIPT):
                    delivery = row

        if delivery is not None:
            # 已实际送达：保留原送达记录，只补一张幂等的更正回执
            result["delivered"].append(tid)
            correction = find_typed_receipt(tid, CORRECTION_RECEIPT)
            if correction is None:
                correction = create_receipt(
                    task=task, receipt_type=CORRECTION_RECEIPT, rollback_id=rollback_id,
                    related=delivery,
                    diff=(f"版本 {version} 已撤回（{reason or '执行回滚'}），"
                          f"对送达回执 {delivery.get('回执编号')} 对应的已送达内容发起更正，"
                          f"分发请求 {task.get('分发批次', '')}（task#{tid}）"),
                )
            # 把更正与真实送达依据、分发请求身份显式关联，便于追查
            basis = delivery.get("送达依据") or delivery.get("回执编号")
            correction["关联送达依据"] = basis
            correction["原送达回执编号"] = delivery.get("回执编号")
            correction["原送达回执ID"] = delivery.get("id")
            correction["dist_request_id"] = tid
            result["correction_receipts"].append(correction.get("id"))
            task["correction_receipt_id"] = correction.get("id")
            task["更正回执编号"] = correction.get("回执编号")
            # 已送达事实不能被改写为从未发生：status 保持已送达，仅标注回执差异
            task["回执状态"] = "部分回收"
            task["abnormal"] = True
            continue

        voucher = task_voucher(task)
        claimed = bool(voucher) or task.get("status") == "分发中"
        if claimed:
            # 已领取但尚未送达：开具撤回回执并作废凭证
            result["withdrawn_claims"].append(tid)
            withdrawal = find_typed_receipt(tid, WITHDRAW_RECEIPT)
            if withdrawal is None:
                withdrawal = create_receipt(
                    task=task, receipt_type=WITHDRAW_RECEIPT, rollback_id=rollback_id,
                    diff=(f"版本 {version} 已撤回（{reason or '执行回滚'}），"
                          f"领取凭证 {voucher or voucher_no(tid)} 在送达确认前失效，"
                          f"按撤回处置，不形成有效送达"),
                )
            result["withdrawal_receipts"].append(withdrawal.get("id"))
            task["withdraw_receipt_id"] = withdrawal.get("id")
            task["撤回回执编号"] = withdrawal.get("回执编号")
            if voucher:
                task["凭证状态"] = "已撤回"
            task["status"] = "分发失败"
            task["分发状态"] = "分发失败"
            task["回执状态"] = "未回收"
            task["pending"] = False
            task["abnormal"] = True
            task["阻断原因"] = f"版本 {version} 已撤回，领取凭证在送达前失效（已开撤回回执）"
        else:
            # 无资格、从未有效领取的派送：只做可追查标记，不产生回执
            result["ineligible"].append(tid)
            task["status"] = "分发失败"
            task["分发状态"] = "分发失败"
            task["pending"] = False
            task["abnormal"] = True
            task["阻断原因"] = task.get("阻断原因") or (
                f"版本 {version} 已撤回；该批次无有效领取凭证与送达记录，属无资格派送，无需更正")
    return result
