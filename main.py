"""好友门卫 —— 自动处理 QQ 好友申请与群邀请。

AstrBot 只把 OneBot 的 request 事件当一条空消息丢进管道，没人处理的话
申请就一直躺在 QQ 里。这个插件把它变成一个确定的动作：按黑/白名单、
暗号、频率上限决定同意还是拒绝，并把结果告诉你。

除了好友申请，同一套规则也管两种群请求：
  · 群邀请（sub_type=invite）：有人把机器人拉进群
  · 加群申请（sub_type=add）：有人申请加入机器人管理的群
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
import weakref
from datetime import datetime
from pathlib import Path
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.message_components import Plain
from astrbot.api.star import Context, Star, StarTools
from astrbot.core.message.message_event_result import MessageChain

try:  # 让最后一个参数吃掉剩余文本，验证语里带空格也能测
    from astrbot.core.star.filter.command import GreedyStr
except Exception:  # pragma: no cover

    class GreedyStr(str):  # type: ignore[no-redef]
        pass

PLUGIN_NAME = "astrbot_plugin_friend_gate"

# 同一个 flag 可能同时从协议端钩子和消息管道进来，10 分钟内只处理一次。
DEDUP_TTL = 600
# 记录里最多留多少条
RECENT_LIMIT = 50

# SnowLuma 的群请求 flag 不是标准 OneBot 的 add:<群号>:<uid>，
# 而是 slreq:1:<sequence>:<群号>:<eventType>:<filtered>，
# 事件里没带 group_id 时可以从这里抠出来。
GROUP_FLAG_PREFIX = "slreq:"

# client 对象的代际号。不能存在 client 自己身上：CQHttp 有属性代理，
# 随手挂一个属性读回来可能变成 functools.partial。
_GENERATIONS: dict[int, int] = {}

# 当前唯一有效的实例。aiocqhttp 的订阅没法撤销，重载插件后旧实例的 handler
# 还挂着；AstrBot 重载会换一份新的模块对象，模块级变量各算各的，所以旧实例
# 拿自己的那份全局一看"我还是当前的"，照样处理 —— 同一条申请被处理两次。
# 把标记挂在 sys 上（进程级，重载不会换），新旧实例读的是同一个地方。
_ACTIVE_ATTR = "_astrbot_friend_gate_active"


def _set_active(obj: Any) -> None:
    setattr(sys, _ACTIVE_ATTR, weakref.ref(obj))


def _is_active(obj: Any) -> bool:
    ref = getattr(sys, _ACTIVE_ATTR, None)
    try:
        return ref is not None and ref() is obj
    except Exception:
        return False


def _get(raw: Any, key: str, default: Any = None) -> Any:
    """兼容 aiocqhttp Event（dict 子类）与普通 dict。"""
    if raw is None:
        return default
    try:
        if hasattr(raw, "get"):
            return raw.get(key, default)
        return raw[key]
    except Exception:
        return getattr(raw, key, default)


def _now_str() -> str:
    return datetime.now().strftime("%m-%d %H:%M")


class FriendGate(Star):
    """自动同意 / 拒绝 QQ 好友申请。"""

    def __init__(self, context: Context, config: Any = None) -> None:
        super().__init__(context, config)
        # 先认领"我是当前实例"，让上一个实例残留的 handler 立刻失效。
        _set_active(self)
        self.config = config or {}
        try:
            self.base_dir = Path(StarTools.get_data_dir(PLUGIN_NAME))
        except Exception:
            self.base_dir = Path(__file__).resolve().parents[2] / "plugin_data" / PLUGIN_NAME
        self.base_dir.mkdir(parents=True, exist_ok=True)

        self.state: dict = self._load_state()
        self._seen: dict[str, float] = {}
        self._stamps: list[float] = []
        self._g_stamps: list[float] = []
        self._hooked: set[str] = set()
        self._hook_task: asyncio.Task | None = None

        # 热重载不会触发 on_astrbot_loaded，所以这里也起一个后台任务去挂钩子。
        try:
            loop = asyncio.get_running_loop()
            self._hook_task = loop.create_task(self._ensure_hooked())
        except Exception:
            pass

    # ---------------- 配置 ----------------
    def _cfg(self, key: str, default: Any = None) -> Any:
        """按点号路径取配置，例如 filter.blacklist。

        面板生成的配置是分组的（filter / reject），扁平写法也兼容，
        免得改过 schema 之后老配置读不到。
        """
        node: Any = self.config
        for part in key.split("."):
            if not hasattr(node, "get"):
                node = None
                break
            node = node.get(part, None)
            if node is None:
                break
        if node is None:
            leaf = key.split(".")[-1]
            try:
                node = self.config.get(leaf, default)
            except Exception:
                return default
        return default if node is None else node

    def _enabled(self) -> bool:
        if self.state.get("paused"):
            return False
        return bool(self._cfg("enabled", True))

    @staticmethod
    def _normalize(raw: Any) -> list[str]:
        """把配置里的 list / 逗号字符串统一成去空的字符串列表。"""
        if isinstance(raw, str):
            raw = raw.replace("，", ",").split(",")
        out: list[str] = []
        try:
            for item in raw or []:
                text = str(item).strip()
                if text:
                    out.append(text)
        except Exception:
            return []
        return out

    def _list(self, key: str) -> list[str]:
        return self._normalize(self._cfg(key, []) or [])

    # ---------------- 验证语 / 暗号（命令可改，改动立即生效） ----------------
    def _friend_keywords(self) -> list[str]:
        override = self.state.get("filter_override") or {}
        if isinstance(override, dict) and "require_keyword" in override:
            return self._normalize(override.get("require_keyword"))
        return self._list("filter.require_keyword")

    def _friend_require_comment(self) -> bool:
        override = self.state.get("filter_override") or {}
        if isinstance(override, dict) and "require_comment" in override:
            return bool(override.get("require_comment"))
        return bool(self._cfg("filter.require_comment", False))

    def _friend_reject_reason(self) -> str:
        override = self.state.get("filter_override") or {}
        if isinstance(override, dict) and "reject_reason" in override:
            return str(override.get("reject_reason") or "").strip()
        return str(self._cfg("reject.reject_reason", "") or "").strip()

    def _group_keywords(self) -> list[str]:
        override = self.state.get("group_override") or {}
        if isinstance(override, dict) and "require_keyword" in override:
            return self._normalize(override.get("require_keyword"))
        return self._list("group.require_keyword")

    @staticmethod
    def _apply_keyword_edit(current: list[str], arg: str) -> tuple[list[str], list[str]]:
        """支持：裸词覆盖 / +词 追加 / -词 删除 / 空 清空。"""
        tokens = [t.strip() for t in arg.replace("，", ",").replace("、", ",").split(",")]
        tokens = [t for t in tokens if t]
        if not tokens:
            return current, []
        if len(tokens) == 1 and tokens[0] in ("空", "清空", "clear"):
            return [], ["清空"]
        additive = any(tok[0] in "+-" for tok in tokens)
        if not additive:
            return tokens, [f"设为 {'/'.join(tokens)}"]
        new = list(current)
        changed: list[str] = []
        for tok in tokens:
            sign = tok[0] if tok[0] in "+-" else "+"
            word = tok[1:] if tok[0] in "+-" else tok
            if not word:
                continue
            if sign == "+" and word not in new:
                new.append(word)
                changed.append(f"+{word}")
            elif sign == "-" and word in new:
                new.remove(word)
                changed.append(f"-{word}")
        return new, changed

    # ---------------- 状态 ----------------
    def _state_path(self) -> Path:
        return self.base_dir / "state.json"

    def _load_state(self) -> dict:
        try:
            data = json.loads(self._state_path().read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
        except Exception:
            pass
        return {"paused": False, "stats": {}, "recent": []}

    def _save_state(self) -> None:
        try:
            self._state_path().write_text(
                json.dumps(self.state, ensure_ascii=False, indent=1),
                encoding="utf-8",
            )
        except Exception as exc:
            logger.warning(f"[friend_gate] 状态写盘失败：{exc}")

    def _bump(self, action: str) -> None:
        stats = self.state.setdefault("stats", {})
        stats[action] = int(stats.get(action, 0)) + 1

    def _push_recent(self, row: dict) -> None:
        rows = self.state.setdefault("recent", [])
        rows.append(row)
        if len(rows) > RECENT_LIMIT:
            del rows[: len(rows) - RECENT_LIMIT]

    # ---------------- 去重与限流 ----------------
    def _dup(self, flag: str) -> bool:
        now = time.time()
        for key in [k for k, v in self._seen.items() if now - v > DEDUP_TTL]:
            self._seen.pop(key, None)
        if not flag:
            return False
        if flag in self._seen:
            return True
        # 热重载会换一个新实例，内存表是空的；再查一遍落盘的记录，
        # 免得同一个申请被同意两次。
        handled = self.state.setdefault("handled", [])
        if flag in handled:
            self._seen[flag] = now
            return True
        self._seen[flag] = now
        handled.append(flag)
        if len(handled) > 80:
            del handled[: len(handled) - 80]
        return False

    def _rate_ok(self) -> bool:
        limit = 0
        try:
            limit = int(self._cfg("filter.max_per_hour", 10))
        except Exception:
            limit = 10
        if limit <= 0:
            return True
        now = time.time()
        self._stamps = [t for t in self._stamps if now - t < 3600]
        if len(self._stamps) >= limit:
            return False
        self._stamps.append(now)
        return True

    # ---------------- 群请求 ----------------
    def _group_switch_raw(self, sub_type: str) -> bool:
        """不看总开关，只看这个群请求类型自己的开关（供 /好友 群 展示）。"""
        override = self.state.get("group_override") or {}
        key = "add" if sub_type == "add" else "invite"
        if isinstance(override, dict) and key in override:
            return bool(override.get(key))
        if key == "add":
            return bool(self._cfg("group.approve_join_request", False))
        legacy = bool(self._cfg("reject.auto_approve_group_invite", False))
        return bool(self._cfg("group.enabled", legacy))

    def _group_on(self, sub_type: str) -> bool:
        """群邀请和加群申请各有一个开关。

        老版本把「自动同意入群邀请」藏在 reject 分组里，这里把它当
        group.enabled 的兜底默认值，升级后不用重新配一遍。
        """
        if not self._enabled():
            return False
        return self._group_switch_raw(sub_type)

    def _group_list(self, key: str) -> list[str]:
        """配置里的静态名单 + 命令写进 state 的运行时名单，合并去重。

        静态名单适合「装机时就想固定」的群；运行时名单方便临时加一个，
        改完立刻生效，不用去面板翻配置。
        """
        merged: list[str] = list(self._list(f"group.{key}"))
        runtime = self.state.get("group_filter", {}).get(key, [])
        if isinstance(runtime, list):
            merged.extend(str(item).strip() for item in runtime)
        seen: set[str] = set()
        out: list[str] = []
        for item in merged:
            text = str(item).strip()
            if text and text not in seen:
                seen.add(text)
                out.append(text)
        return out

    def _group_rate_ok(self) -> bool:
        """群请求单独限流，免得被拉群刷屏把额度占满。"""
        try:
            limit = int(self._cfg("group.max_per_hour", 5))
        except Exception:
            limit = 5
        if limit <= 0:
            return True
        now = time.time()
        self._g_stamps = [t for t in self._g_stamps if now - t < 3600]
        if len(self._g_stamps) >= limit:
            return False
        self._g_stamps.append(now)
        return True

    def _decide_group(self, group_id: str, sub_type: str, comment: str) -> tuple[str, str]:
        """返回 (approve|reject|skip, 原因)。"""
        if not self._enabled():
            return "skip", "总开关关闭"
        if sub_type == "add":
            if not self._group_on("add"):
                return "skip", "未开启自动同意加群申请"
        elif not self._group_on("invite"):
            return "skip", "未开启自动同意入群邀请"

        if group_id and group_id in self._group_list("blacklist"):
            return "reject", "群黑名单"
        whitelist = self._group_list("whitelist")
        if whitelist:
            if group_id and group_id in whitelist:
                return "approve", "群白名单"
            return "skip", "不在群白名单，留待人工确认"
        keywords = self._group_keywords()
        if keywords:
            text = (comment or "").lower()
            if not any(word.lower() in text for word in keywords):
                return "reject", f"邀请附言不含暗号（{'/'.join(keywords)}）"
        if not self._group_rate_ok():
            return "skip", "超过每小时上限，留待人工处理"
        return "approve", "自动通过"

    @staticmethod
    def _group_id_from_flag(flag: str) -> str:
        """标准协议端在事件里带 group_id；SnowLuma 的 flag 里也有一份。"""
        if not flag.startswith(GROUP_FLAG_PREFIX):
            return ""
        parts = flag.split(":")
        if len(parts) >= 4 and parts[3].isdigit():
            return parts[3]
        return ""

    async def _group_name(self, group_id: str, platform_id: str = "") -> str:
        if not group_id:
            return ""
        try:
            data = await asyncio.wait_for(
                self._call_action(
                    "get_group_info", platform_id=platform_id, group_id=int(group_id)
                ),
                timeout=4,
            )
        except Exception:
            return ""
        if isinstance(data, dict):
            return str(data.get("group_name") or "")
        return ""

    # ---------------- 判定 ----------------
    def _decide(self, user_id: str, comment: str) -> tuple[str, str]:
        """返回 (approve|reject|skip, 原因)。"""
        if not self._enabled():
            return "skip", "总开关关闭"
        blacklist = self._list("filter.blacklist")
        if user_id in blacklist:
            return "reject", "黑名单"
        whitelist = self._list("filter.whitelist")
        if user_id in whitelist:
            return "approve", "白名单"
        keywords = self._friend_keywords()
        if keywords:
            text = (comment or "").lower()
            if not any(word.lower() in text for word in keywords):
                return "reject", f"验证语不含暗号（{'/'.join(keywords)}）"
        if not self._rate_ok():
            return "skip", "超过每小时上限，留待人工处理"
        return "approve", "自动通过"

    def _remark(self, user_id: str, nickname: str, comment: str) -> str:
        template = str(self._cfg("friend_remark", "") or "").strip()
        if not template:
            return ""
        try:
            return template.format(
                nickname=nickname or user_id,
                user_id=user_id,
                comment=comment or "",
            )[:60]
        except Exception:
            return ""

    # ---------------- OneBot 调用 ----------------
    async def _call_action(self, action: str, platform_id: str = "", **params: Any) -> Any:
        try:
            platforms = list(self.context.platform_manager.platform_insts)
        except Exception:
            platforms = []
        for platform in platforms:
            try:
                if platform_id and str(platform.meta().id) != platform_id:
                    continue
                client = platform.get_client() if hasattr(platform, "get_client") else None
                api = getattr(client, "api", None)
                if api is None or not hasattr(api, "call_action"):
                    continue
                return await api.call_action(action, **params)
            except Exception as exc:
                logger.error(f"[friend_gate] 调用 {action} 失败：{exc}")
                return None
        return None

    # ---------------- 核心处理 ----------------
    async def _on_request(self, raw: Any, platform_id: str = "") -> None:
        if _get(raw, "post_type") != "request":
            return
        request_type = str(_get(raw, "request_type") or "").lower()
        sub_type = str(_get(raw, "sub_type") or "").lower()
        flag = str(_get(raw, "flag") or "")
        user_id = str(_get(raw, "user_id") or "")
        comment = str(_get(raw, "comment") or "")
        nickname = str(_get(raw, "nickname") or "")
        group_id = str(_get(raw, "group_id") or "")

        if request_type == "friend":
            if self._dup(flag):
                return
            if not (comment or "").strip() and self._friend_require_comment():
                action, reason = "reject", "要求填写验证语，但对方没填"
            else:
                action, reason = self._decide(user_id, comment)
            if action == "skip":
                logger.info(f"[friend_gate] 跳过 {user_id}（{reason}）")
                self._bump("skipped")
                self._push_recent(
                    {"ts": _now_str(), "user_id": user_id, "nickname": nickname,
                     "comment": comment, "action": "跳过", "reason": reason}
                )
                self._save_state()
                if reason != "总开关关闭":
                    await self._notify(
                        f"好友申请 · 未处理\nQQ：{user_id}\n昵称：{nickname or '未知'}\n"
                        f"验证语：{comment or '（无）'}\n原因：{reason}",
                        platform_id,
                    )
                return

            approve = action == "approve"
            remark = self._remark(user_id, nickname, comment) if approve else ""
            reason_text = "" if approve else self._friend_reject_reason()

            params: dict[str, Any] = {"flag": flag, "approve": approve}
            if remark:
                params["remark"] = remark
            if reason_text:
                params["reason"] = reason_text

            result = await self._call_action(
                "set_friend_add_request", platform_id=platform_id, **params
            )
            degraded = ""
            if result is None and (remark or reason_text):
                # 有的协议端（SnowLuma 就是）只认 flag + approve，
                # 多带字段会被参数校验拒掉，这里退回最简参数重试一次。
                result = await self._call_action(
                    "set_friend_add_request",
                    platform_id=platform_id,
                    flag=flag,
                    approve=approve,
                )
                if result is not None:
                    degraded = "（协议端不支持备注/理由，已按最简参数重试）"
                    remark = ""

            ok = result is not None
            if not nickname and approve:
                nickname = await self._fetch_nickname(user_id, platform_id)
            self._bump("approved" if approve else "rejected")
            welcomed = ""
            if approve and ok:
                welcomed = await self._welcome(user_id, platform_id)
            self._push_recent(
                {"ts": _now_str(), "user_id": user_id, "nickname": nickname,
                 "comment": comment, "action": "同意" if approve else "拒绝",
                 "reason": reason + welcomed, "ok": ok}
            )
            self._save_state()
            logger.info(
                f"[friend_gate] {'已同意' if approve else '已拒绝'} {user_id}"
                f"（{nickname or '无昵称'}）原因：{reason}，接口返回：{ok}"
            )
            if self._cfg("notify_admin", True):
                title = "好友申请 · 已同意" if approve else "好友申请 · 已拒绝"
                extra = f"\n备注：{remark}" if remark else ""
                await self._notify(
                    f"{title}\nQQ：{user_id}\n昵称：{nickname or '未知'}\n"
                    f"验证语：{comment or '（无）'}\n依据：{reason}{extra}{degraded}"
                    + ("" if ok else "\n⚠️ 协议端没有回应，可能在 QQ 侧仍待处理"),
                    platform_id,
                )
            return

        if request_type == "group":
            await self._handle_group_request(
                flag=flag,
                sub_type=sub_type or "invite",
                group_id=group_id,
                user_id=user_id,
                nickname=nickname,
                comment=comment,
                platform_id=platform_id,
            )

    async def _handle_group_request(
        self,
        flag: str,
        sub_type: str,
        group_id: str,
        user_id: str,
        nickname: str,
        comment: str,
        platform_id: str = "",
    ) -> None:
        """群邀请（invite）与加群申请（add）共用一套判定。"""
        if not flag:
            return
        # 事件里偶尔不带 group_id（SnowLuma 的邀请卡片就是这样），
        # flag 里还有一份，先补上再判定，否则黑名单会失效。
        if not group_id:
            group_id = self._group_id_from_flag(flag)
        is_join = sub_type == "add"
        label = "加群申请" if is_join else "入群邀请"
        if self._dup(flag):
            return

        action, reason = self._decide_group(group_id, sub_type, comment)
        if action == "skip":
            logger.info(f"[friend_gate] 跳过{label} 群 {group_id}（{reason}）")
            self._bump("group_skipped")
            self._push_recent(
                {"ts": _now_str(), "user_id": user_id, "nickname": nickname,
                 "comment": comment, "action": f"{label}跳过({group_id})",
                 "reason": reason}
            )
            self._save_state()
            # 开关是主动关的，就别每来一条都戳一下管理员了。
            if "未开启" not in reason and reason != "总开关关闭" and self._cfg("group.notify", True):
                await self._notify(
                    f"{label} · 未处理\n群号：{group_id}\n邀请人：{user_id}"
                    f"（{nickname or '未知'}）\n附言：{comment or '（无）'}\n原因：{reason}",
                    platform_id,
                )
            return

        approve = action == "approve"
        params: dict[str, Any] = {"flag": flag, "approve": approve}
        reason_text = ""
        if not approve:
            reason_text = str(self._cfg("group.reject_reason", "") or "").strip()
            if reason_text:
                params["reason"] = reason_text

        result = await self._call_action(
            "set_group_add_request", platform_id=platform_id, **params
        )
        degraded = ""
        if result is None and reason_text:
            # 和好友申请一样，多带字段可能被协议端拒掉，退回最简参数重试。
            result = await self._call_action(
                "set_group_add_request",
                platform_id=platform_id,
                flag=flag,
                approve=approve,
            )
            if result is not None:
                degraded = "（协议端不支持拒绝理由，已按最简参数重试）"
        ok = result is not None

        self._bump("group_approved" if approve else "group_rejected")
        self._push_recent(
            {"ts": _now_str(), "user_id": user_id, "nickname": nickname,
             "comment": comment,
             "action": f"{'同意' if approve else '拒绝'}{label}({group_id})",
             "reason": reason, "ok": ok}
        )
        self._save_state()
        logger.info(
            f"[friend_gate] {'已同意' if approve else '已拒绝'}{label} 群 {group_id}"
            f"（邀请人 {user_id or '未知'}）原因：{reason}，接口返回：{ok}"
        )
        if self._cfg("group.notify", True):
            title = f"{label} · {'已同意' if approve else '已拒绝'}"
            name = await self._group_name(group_id, platform_id) if group_id else ""
            await self._notify(
                f"{title}\n群号：{group_id}"
                + (f"\n群名：{name}" if name else "")
                + f"\n邀请人：{user_id or '未知'}（{nickname or '未知'}）"
                f"\n附言：{comment or '（无）'}\n依据：{reason}{degraded}"
                + ("" if ok else "\n⚠️ 协议端没有回应，可能仍待人工处理"),
                platform_id,
            )

    async def _welcome(self, user_id: str, platform_id: str = "") -> str:
        """同意后给对方发一句欢迎语（默认关闭，避免打扰）。"""
        text = str(self._cfg("approve_welcome", "") or "").strip()
        if not text:
            return ""
        target_platform = platform_id
        if not target_platform:
            try:
                target_platform = str(self.context.platform_manager.platform_insts[0].meta().id)
            except Exception:
                return ""
        umo = f"{target_platform}:FriendMessage:{user_id}"
        try:
            await asyncio.wait_for(
                self.context.send_message(umo, MessageChain([Plain(text[:200])])),
                timeout=8,
            )
            return "，已发送欢迎语"
        except Exception as exc:
            logger.debug(f"[friend_gate] 欢迎语发送失败（{user_id}）：{exc}")
            return ""

    async def _fetch_nickname(self, user_id: str, platform_id: str = "") -> str:
        """好友申请事件里通常不带昵称，单独查一次；失败就用 QQ 号顶上。"""
        try:
            data = await asyncio.wait_for(
                self._call_action(
                    "get_stranger_info", platform_id=platform_id, user_id=int(user_id)
                ),
                timeout=3,
            )
        except Exception:
            return ""
        if isinstance(data, dict):
            return str(data.get("nickname") or data.get("card") or "")
        return ""

    async def _notify(self, text: str, platform_id: str = "") -> None:
        chain = MessageChain([Plain(text)])

        # 指定了完整会话就直接发，省得猜平台 ID。
        explicit = str(self._cfg("notify_session", "") or "").strip()
        if explicit:
            try:
                await self.context.send_message(explicit, chain)
                return
            except Exception as exc:
                logger.warning(f"[friend_gate] 通知 {explicit} 失败：{exc}")
                return

        targets: list[str] = []
        try:
            for admin in list(self.context.get_config().get("admins_id", []) or []):
                tag = str(admin).strip()
                # admins_id 里常混着 webchat 用户名（比如 astrbot），
                # 那不是 QQ 号，拼出来的会话发不出去，直接跳过。
                if tag.isdigit():
                    targets.append(tag)
        except Exception:
            targets = []
        if not targets:
            return
        target_platform = platform_id
        if not target_platform:
            try:
                target_platform = str(self.context.platform_manager.platform_insts[0].meta().id)
            except Exception:
                return
        for admin_id in targets:
            umo = f"{target_platform}:FriendMessage:{admin_id}"
            try:
                await self.context.send_message(umo, chain)
            except Exception as exc:
                logger.debug(f"[friend_gate] 通知管理员 {admin_id} 失败：{exc}")

    # ---------------- 钩子安装 ----------------
    async def _install_hooks(self) -> None:
        """在协议端客户端上挂 request 钩子。

        AstrBot 只是把 request 事件当空消息丢进管道，没有动作能力；
        直接订阅 aiocqhttp 的 request 事件最可靠，也不受唤醒前缀影响。
        """
        try:
            platforms = list(self.context.platform_manager.platform_insts)
        except Exception:
            platforms = []
        for platform in platforms:
            try:
                client = platform.get_client() if hasattr(platform, "get_client") else None
                if client is None or not hasattr(client, "on_request"):
                    continue
                platform_id = str(platform.meta().id)
                if platform_id in self._hooked:
                    continue

                # aiocqhttp 的订阅没法撤销，重载后会留下旧 handler。
                # 用一个代际号让旧实例的 handler 直接失效。
                client_key = id(client)
                generation = _GENERATIONS.get(client_key, 0) + 1
                _GENERATIONS[client_key] = generation

                def _make(pid: str, gen: int, key: int, owner: Any):
                    async def handler(event: Any) -> None:
                        try:
                            # 重载后旧实例可能还挂着钩子，直接让位给新实例。
                            if not _is_active(owner):
                                return
                            if _GENERATIONS.get(key, 0) != gen:
                                return  # 已被新实例接管
                            await owner._on_request(event, platform_id=pid)
                        except Exception as exc:
                            logger.error(f"[friend_gate] 处理申请出错：{exc}")

                    return handler

                client.on_request()(_make(platform_id, generation, client_key, self))
                self._hooked.add(platform_id)
                logger.info(f"[friend_gate] 已接管 {platform_id} 的好友申请事件")
            except Exception as exc:
                import traceback

                logger.warning(
                    f"[friend_gate] 挂载事件钩子失败：{exc}\n{traceback.format_exc()}"
                )

    async def _ensure_hooked(self, tries: int = 20, delay: float = 3.0) -> None:
        """启动早期平台可能还没起来，轮询到挂上为止；之后定期复查新平台。"""
        for _ in range(max(1, tries)):
            if self._hooked:
                break
            try:
                await self._install_hooks()
            except Exception:
                pass
            if self._hooked:
                break
            await asyncio.sleep(delay)
        # 平台可能在运行中被重载（换号、加新适配器），定期补挂一次。
        while True:
            await asyncio.sleep(300)
            try:
                await self._install_hooks()
            except Exception:
                pass

    async def terminate(self) -> None:
        task = getattr(self, "_hook_task", None)
        if task is not None and not task.done():
            task.cancel()
        # 卸任时把自己的标记摘掉，别留个指向已停用实例的弱引用。
        ref = getattr(sys, _ACTIVE_ATTR, None)
        try:
            if ref is not None and ref() is self:
                delattr(sys, _ACTIVE_ATTR)
        except Exception:
            pass

    async def on_astrbot_loaded(self) -> None:
        await self._install_hooks()

    # ---------------- 命令 ----------------
    @filter.command("好友")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def friend_cmd(self, event: AstrMessageEvent, raw_args: GreedyStr):
        """好友门卫：/好友、/好友 开|关、/好友 记录、/好友 暗号、/好友 群、/好友 群测试 <群号>"""
        parts = str(raw_args or "").split()
        sub = parts[0] if parts else ""
        arg = " ".join(parts[1:]).strip()
        logger.info(f"[friend_gate] 收到命令：/好友 {sub} {arg}".rstrip())

        # 顺手自检：热重载后钩子可能丢了，这里补挂一次。
        await self._install_hooks()

        if sub in ("开", "on", "启用"):
            self.state["paused"] = False
            self._save_state()
            yield event.plain_result("好友门卫已开启：符合条件的好友申请会自动同意。")
            return
        if sub in ("关", "off", "停用"):
            self.state["paused"] = True
            self._save_state()
            yield event.plain_result("好友门卫已暂停：新的好友申请不再自动处理，等你手动确认。")
            return
        if sub in ("路径", "path", "文件"):
            path = self._state_path()
            exists = "已生成" if path.is_file() else "还没生成"
            yield event.plain_result(
                f"好友门卫数据文件\n{path}\n状态：{exists}\n"
                f"（在实例目录下 core/data/plugin_data/{PLUGIN_NAME}/）"
            )
            return
        if sub in ("记录", "日志", "list"):
            rows = list(self.state.get("recent", []))[-12:]
            if not rows:
                yield event.plain_result("还没有处理过任何申请。")
                return
            lines = ["最近的好友申请："]
            for row in reversed(rows):
                lines.append(
                    f"· {row.get('ts', '')} {row.get('action', '')} "
                    f"{row.get('user_id', '')}（{row.get('nickname') or '无昵称'}）"
                    f" — {row.get('reason', '')}"
                )
            yield event.plain_result("\n".join(lines))
            return
        if sub in ("测试", "试跑", "check"):
            if not arg:
                yield event.plain_result("用法：/好友 测试 <QQ号> [验证语]")
                return
            parts = arg.split(maxsplit=1)
            user_id = parts[0]
            comment = parts[1] if len(parts) > 1 else ""
            action, reason = self._decide(user_id, comment)
            label = {"approve": "会同意", "reject": "会拒绝", "skip": "不处理"}.get(action, action)
            yield event.plain_result(
                f"试跑结果：{label}\nQQ：{user_id}\n验证语：{comment or '（无）'}\n依据：{reason}"
            )
            return

        # ---------------- 验证语 / 暗号 ----------------
        if sub in ("暗号", "验证语", "关键词"):
            current = self._friend_keywords()
            if not arg:
                yield event.plain_result(
                    f"当前验证语暗号：{'/'.join(current) if current else '不校验（谁加都同意）'}\n\n"
                    "改法：\n"
                    "/好友 暗号 老王            → 设为「老王」\n"
                    "/好友 暗号 老王,朋友        → 设为多个（命中任一即可）\n"
                    "/好友 暗号 +同学            → 追加一个\n"
                    "/好友 暗号 -老王            → 删掉一个\n"
                    "/好友 暗号 空               → 清空，恢复不校验\n\n"
                    "注意：暗号是「对方加你时填的那句话」里要包含的词，不是让他看到提示。\n"
                    "对方没填验证语 = 暗号不命中 = 会被拒绝。多数人不填，建议配「必填」先观察。"
                )
                return
            new, changed = self._apply_keyword_edit(current, arg)
            self.state.setdefault("filter_override", {})["require_keyword"] = new
            self._save_state()
            yield event.plain_result(
                f"验证语暗号已更新：{'、'.join(changed) if changed else '没有变化'}\n"
                f"现在：{'/'.join(new) if new else '不校验'}"
            )
            return

        if sub in ("必填", "要求填写", "必须填"):
            override = self.state.setdefault("filter_override", {})
            if not arg:
                now = self._friend_require_comment()
                yield event.plain_result(
                    f"必须填写验证语：{'开' if now else '关'}\n"
                    "开启后，验证语为空的好友申请直接拒绝。\n"
                    "用法：/好友 必填 开 | /好友 必填 关"
                )
                return
            if arg in ("开", "on", "是", "启用"):
                override["require_comment"] = True
                self._save_state()
                yield event.plain_result("已开启：验证语为空的好友申请会被拒绝。")
                return
            if arg in ("关", "off", "否", "停用"):
                override["require_comment"] = False
                self._save_state()
                yield event.plain_result("已关闭：验证语为空也照常按暗号等规则判定。")
                return
            yield event.plain_result("用法：/好友 必填 开 或 /好友 必填 关")
            return

        if sub in ("拒绝理由", "理由"):
            override = self.state.setdefault("filter_override", {})
            if not arg:
                now = self._friend_reject_reason()
                yield event.plain_result(
                    f"拒绝好友申请时的回复：{now or '（空，不回复）'}\n\n"
                    "改法：/好友 拒绝理由 不认识的人不加\n"
                    "       /好友 拒绝理由 空   → 清空\n\n"
                    "提示：SnowLuma 的 set_friend_add_request 只接受 flag + approve，\n"
                    "不接受任何回复内容，这条配置在它上面不会生效（会自动降级，不影响拒绝）。\n"
                    "换 NapCat / Lagrange 之类支持 reason 的协议端才有用。"
                )
                return
            text = arg.strip()
            if text in ("空", "清空", "clear"):
                override["reject_reason"] = ""
                self._save_state()
                yield event.plain_result("已清空拒绝回复。")
                return
            override["reject_reason"] = text[:60]
            self._save_state()
            yield event.plain_result(
                f"拒绝回复已设为：{text[:60]}\n"
                "（协议端支持 reason 参数时才会真的发出去，SnowLuma 不支持，只会降级成静默拒绝）"
            )
            return

        # ---------------- 群请求相关命令 ----------------
        if sub in ("群开", "群关", "邀请开", "邀请关", "加群开", "加群关"):
            target = "add" if sub.startswith("加群") else "invite"
            on = sub.endswith("开")
            override = self.state.setdefault("group_override", {})
            override[target] = on
            self._save_state()
            what = "加群申请" if target == "add" else "入群邀请"
            yield event.plain_result(
                f"已{'开启' if on else '关闭'}自动同意{what}。"
                + ("" if on else "（之后收到的会原样留在 QQ 里）")
                + "\n用 /好友 群 查看完整状态。"
            )
            return

        if sub in ("群暗号", "邀请暗号"):
            current = self._group_keywords()
            if not arg:
                yield event.plain_result(
                    f"当前邀请附言暗号：{'/'.join(current) if current else '不校验'}\n"
                    "用法：/好友 群暗号 老王 / 好友 群暗号 +同学 / 好友 群暗号 空"
                )
                return
            new, changed = self._apply_keyword_edit(current, arg)
            self.state.setdefault("group_override", {})["require_keyword"] = new
            self._save_state()
            yield event.plain_result(
                f"邀请附言暗号已更新：{'、'.join(changed) if changed else '没有变化'}\n"
                f"现在：{'/'.join(new) if new else '不校验'}"
            )
            return

        if sub in ("群白", "群黑"):
            key = "whitelist" if sub == "群白" else "blacklist"
            name = "白名单" if key == "whitelist" else "黑名单"
            current = self._group_list(key)
            if not arg:
                yield event.plain_result(
                    f"群{name}（{len(current)} 个）：\n"
                    + ("\n".join(f"· {gid}" for gid in current) if current else "（空）")
                    + f"\n\n添加：/好友 {sub} 群号\n删除：/好友 {sub} -群号\n清空：/好友 {sub} 空"
                )
                return
            bucket = self.state.setdefault("group_filter", {})
            store = bucket.get(key)
            if not isinstance(store, list):
                store = bucket[key] = []
            if arg.strip() in ("空", "清空", "clear"):
                store.clear()
                self._save_state()
                yield event.plain_result(f"已清空运行时群{name}（面板里配置的静态名单不受影响）。")
                return
            changed: list[str] = []
            for token in arg.replace("，", ",").split(","):
                token = token.strip()
                if not token:
                    continue
                sign, num = (token[0], token[1:]) if token[0] in "+-" else ("+", token)
                if not num.isdigit():
                    continue
                if sign == "+" and num not in store:
                    store.append(num)
                    changed.append(f"+{num}")
                elif sign == "-" and num in store:
                    store.remove(num)
                    changed.append(f"-{num}")
            self._save_state()
            lines = [f"群{name}已更新：{'、'.join(changed) if changed else '没有变化'}"]
            lines.append("当前：" + ("、".join(self._group_list(key)) or "（空）"))
            if key == "whitelist" and self._group_list(key):
                lines.append("提示：白名单非空后，只同意白名单里的群，其他群邀请会留给你人工处理。")
            yield event.plain_result("\n".join(lines))
            return

        if sub in ("群测试", "群试跑"):
            if not arg:
                yield event.plain_result("用法：/好友 群测试 <群号> [邀请附言]")
                return
            pieces = arg.split(maxsplit=1)
            group_id = pieces[0]
            comment = pieces[1] if len(pieces) > 1 else ""
            action, reason = self._decide_group(group_id, "invite", comment)
            label = {"approve": "会同意", "reject": "会拒绝", "skip": "不处理"}.get(action, action)
            yield event.plain_result(
                f"试跑结果（入群邀请）：{label}\n群号：{group_id}\n"
                f"附言：{comment or '（无）'}\n依据：{reason}"
            )
            return

        if sub in ("群", "群邀请", "邀请", "群状态"):
            if arg:
                yield event.plain_result("用法：/好友 群")
                return
            stats = self.state.get("stats", {})
            wl = self._group_list("whitelist")
            bl = self._group_list("blacklist")
            kw = self._group_keywords()
            master = self._enabled()
            inv = self._group_switch_raw("invite")
            join = self._group_switch_raw("add")
            lines = [
                "群请求门卫",
                f"入群邀请（被拉进群）：{'开' if inv else '关'}"
                f"{'（总开关关闭，实际不生效）' if inv and not master else ''}",
                f"加群申请（有人申请进你的群）：{'开' if join else '关'}"
                f"{'（总开关关闭，实际不生效）' if join and not master else ''}",
                f"群白名单 {len(wl)} 个" + (f"：{'、'.join(wl[:8])}{'…' if len(wl) > 8 else ''}" if wl else "（空，不限群）"),
                f"群黑名单 {len(bl)} 个" + (f"：{'、'.join(bl[:8])}{'…' if len(bl) > 8 else ''}" if bl else ""),
                f"邀请附言暗号：{'/'.join(kw) if kw else '不校验'}",
                f"每小时上限：{self._cfg('group.max_per_hour', 5)}",
                f"累计：同意 {stats.get('group_approved', 0) + stats.get('group_invited', 0)}，"
                f"拒绝 {stats.get('group_rejected', 0)}，跳过 {stats.get('group_skipped', 0)}",
                "",
                "开关：/好友 群开 | 群关 | 加群开 | 加群关",
                "名单：/好友 群白 群号 | 群黑 群号（-群号 删除，空 清空）",
                "暗号：/好友 群暗号 词 | 群暗号 空",
            ]
            yield event.plain_result("\n".join(lines))
            return

        keywords = self._friend_keywords()
        stats = self.state.get("stats", {})
        last = ""
        recent = self.state.get("recent") or []
        if recent:
            last = f"　最近：{recent[-1].get('ts', '')} {recent[-1].get('action', '')}"
        reject_reason = self._friend_reject_reason()
        lines = [
            "好友门卫",
            f"自动同意：{'开' if self._enabled() else '关'}"
            f"（面板开关 {'开' if self._cfg('enabled', True) else '关'}"
            f"{'，命令暂停中' if self.state.get('paused') else ''}）",
            f"验证语暗号：{'/'.join(keywords) if keywords else '不校验'}"
            f"{'（且必须填写验证语）' if self._friend_require_comment() else ''}",
            f"黑名单 {len(self._list('filter.blacklist'))} 个 / "
            f"白名单 {len(self._list('filter.whitelist'))} 个",
            f"每小时上限：{self._cfg('filter.max_per_hour', 10)}",
            f"群请求：邀请 {'开' if self._group_on('invite') else '关'} / "
            f"加群 {'开' if self._group_on('add') else '关'}（详情 /好友 群）",
            f"累计：同意 {stats.get('approved', 0)}，拒绝 {stats.get('rejected', 0)}，"
            f"跳过 {stats.get('skipped', 0)}",
            f"钩子：{'已接管 ' + '/'.join(sorted(self._hooked)) if self._hooked else '未挂上（重载插件试试）'}"
            f"{last}",
            "验证语：/好友 暗号 词 | 必填 开/关 | 拒绝理由 文本",
            "记录文件：/好友 路径",
        ]
        if reject_reason:
            lines.insert(6, f"拒绝时回复：{reject_reason}")
        yield event.plain_result("\n".join(lines))
