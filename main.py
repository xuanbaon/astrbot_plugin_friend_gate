"""好友门卫 —— 自动处理 QQ 好友申请。

AstrBot 只把 OneBot 的 request 事件当一条空消息丢进管道，没人处理的话
申请就一直躺在 QQ 里。这个插件把它变成一个确定的动作：按黑/白名单、
暗号、频率上限决定同意还是拒绝，并把结果告诉你。
"""

from __future__ import annotations

import asyncio
import json
import time
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

# client 对象的代际号。不能存在 client 自己身上：CQHttp 有属性代理，
# 随手挂一个属性读回来可能变成 functools.partial。
_GENERATIONS: dict[int, int] = {}


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
        self.config = config or {}
        try:
            self.base_dir = Path(StarTools.get_data_dir(PLUGIN_NAME))
        except Exception:
            self.base_dir = Path(__file__).resolve().parents[2] / "plugin_data" / PLUGIN_NAME
        self.base_dir.mkdir(parents=True, exist_ok=True)

        self.state: dict = self._load_state()
        self._seen: dict[str, float] = {}
        self._stamps: list[float] = []
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

    def _list(self, key: str) -> list[str]:
        raw = self._cfg(key, []) or []
        if isinstance(raw, str):
            raw = [item for item in raw.replace("，", ",").split(",")]
        out: list[str] = []
        for item in raw:
            text = str(item).strip()
            if text:
                out.append(text)
        return out

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
        keywords = self._list("filter.require_keyword")
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
            if not (comment or "").strip() and self._cfg("filter.require_comment", False):
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
            reason_text = "" if approve else str(self._cfg("reject.reject_reason", "") or "").strip()

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

        if request_type == "group" and sub_type == "invite":
            if not self._cfg("reject.auto_approve_group_invite", False):
                return
            if self._dup(flag):
                return
            if not self._enabled():
                return
            result = await self._call_action(
                "set_group_add_request",
                platform_id=platform_id,
                flag=flag,
                sub_type=sub_type,
                approve=True,
            )
            logger.info(f"[friend_gate] 已同意入群邀请 群 {group_id}，返回：{result is not None}")
            self._bump("group_invited")
            self._push_recent(
                {"ts": _now_str(), "user_id": user_id, "nickname": nickname,
                 "comment": comment, "action": f"入群邀请({group_id})", "reason": "自动同意"}
            )
            self._save_state()
            if self._cfg("notify_admin", True):
                await self._notify(
                    f"入群邀请 · 已同意\n群号：{group_id}\n邀请人：{user_id}\n"
                    f"附言：{comment or '（无）'}",
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

                def _make(pid: str, gen: int, key: int):
                    async def handler(event: Any) -> None:
                        try:
                            if _GENERATIONS.get(key, 0) != gen:
                                return  # 已被新实例接管
                            await self._on_request(event, platform_id=pid)
                        except Exception as exc:
                            logger.error(f"[friend_gate] 处理申请出错：{exc}")

                    return handler

                client.on_request()(_make(platform_id, generation, client_key))
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

    async def on_astrbot_loaded(self) -> None:
        await self._install_hooks()

    # ---------------- 命令 ----------------
    @filter.command("好友")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def friend_cmd(self, event: AstrMessageEvent, raw_args: GreedyStr):
        """好友门卫：/好友、/好友 开|关、/好友 记录、/好友 测试 <QQ>"""
        parts = str(raw_args or "").split()
        sub = parts[0] if parts else ""
        arg = " ".join(parts[1:]).strip()

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

        keywords = self._list("filter.require_keyword")
        stats = self.state.get("stats", {})
        last = ""
        recent = self.state.get("recent") or []
        if recent:
            last = f"　最近：{recent[-1].get('ts', '')} {recent[-1].get('action', '')}"
        lines = [
            "好友门卫",
            f"自动同意：{'开' if self._enabled() else '关'}"
            f"（面板开关 {'开' if self._cfg('enabled', True) else '关'}"
            f"{'，命令暂停中' if self.state.get('paused') else ''}）",
            f"暗号：{'/'.join(keywords) if keywords else '不校验'}"
            f"{'（且必须填写验证语）' if self._cfg('filter.require_comment', False) else ''}",
            f"黑名单 {len(self._list('filter.blacklist'))} 个 / "
            f"白名单 {len(self._list('filter.whitelist'))} 个",
            f"每小时上限：{self._cfg('filter.max_per_hour', 10)}",
            f"累计：同意 {stats.get('approved', 0)}，拒绝 {stats.get('rejected', 0)}，"
            f"跳过 {stats.get('skipped', 0)}",
            f"钩子：{'已接管 ' + '/'.join(sorted(self._hooked)) if self._hooked else '未挂上（重载插件试试）'}"
            f"{last}",
            "记录文件：/好友 路径",
        ]
        yield event.plain_result("\n".join(lines))
