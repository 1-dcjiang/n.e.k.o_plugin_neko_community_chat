"""neko_community_chat 插件：本地消息服务 + 聊天面板 + YUI 唤醒桥。

结构：
- server.py        本地 HTTP 服务（127.0.0.1:7870）：消息 API / 社区动态代理 / 聊天面板页
- static/chat.html QQ 式聊天面板（客户端面板 / 油猴 iframe 共用）
- userscript/      油猴注入脚本（社区网页右下角聊天窗）
- docs/guide.md    教程

消息流：
  面板发消息 → server 写 messages.jsonl + inbox/
  → 本插件后台 tick（每 5s）扫 inbox → push_message 唤醒 YUI（注入消息内容）
  → YUI 用 community_chat_send 回复 → 写 outbox/
  → tick 扫 outbox → 归入 messages.jsonl → 面板轮询 /api/messages 显示
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time

from plugin.sdk.plugin import NekoPluginBase, lifecycle, llm_tool, neko_plugin

from . import server as _server

logger = logging.getLogger("N.E.K.O.Plugin_neko_community_chat")

_TICK_INTERVAL = 5.0          # 后台扫描间隔（秒）
_INBOX_COOLDOWN = 3.0         # 同一来源消息最短唤醒间隔（防刷屏）
_MAX_CONSECUTIVE_FAIL = 5     # inbox 消费连续失败上限（到达后暂停该文件避免死循环）
_SEND_NOTE = (
    "【喵宇宙社区聊天】用户在聊天面板对你说：{text}"
    "。请以 YUI 的身份自然回复这条消息，用 community_chat_send 把你的回复发回聊天面板"
    "（不回复时也可不发）。本条是聊天消息，正常对话即可，无需开口播报。"
)


@neko_plugin
class NekoCommunityChatPlugin(NekoPluginBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._server: object = None
        self._server_started = False
        self._tick_thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._last_inject_ts = 0.0
        self._fail_map: dict[str, int] = {}  # inbox 文件 → 连续失败次数
        self._notified_outbox: set[str] = set()  # 已归入消息流的 outbox 文件

    # ══════════ 生命周期 ══════════

    @lifecycle(id="startup")
    async def on_startup(self, **_):
        # 1) 本地消息服务（127.0.0.1:7870，纯标准库）；端口被占（已有实例）则复用不报错
        try:
            if not _port_in_use(server_port()):
                self._server = _server.start_server()
            self._server_started = True
        except Exception as exc:  # noqa: BLE001
            logger.warning("community chat server start failed: %s", exc)
        # 2) 后台 tick：inbox → 唤醒 YUI；outbox → 归入消息流
        self._start_tick()
        return {"status": "ready", "plugin": "neko_community_chat", "panel": "http://127.0.0.1:7870/chat"}

    @lifecycle(id="shutdown")
    async def on_shutdown(self, **_):
        self._stop.set()
        try:
            srv = getattr(self, "_server", None)
            if srv is not None:
                await asyncio.to_thread(srv.shutdown)
        except Exception:  # noqa: BLE001
            pass
        return {"status": "shutdown"}

    # ══════════ 后台 tick ══════════

    def _start_tick(self) -> None:
        if self._tick_thread and self._tick_thread.is_alive():
            return
        self._tick_thread = threading.Thread(
            target=self._tick_loop, daemon=True, name="neko-community-chat-tick"
        )
        self._tick_thread.start()

    def _tick_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._consume_inbox()
            except Exception:  # noqa: BLE001
                logger.debug("inbox consume failed", exc_info=True)
            try:
                self._consume_outbox()
            except Exception:  # noqa: BLE001
                logger.debug("outbox consume failed", exc_info=True)
            self._stop.wait(_TICK_INTERVAL)

    def _consume_inbox(self) -> None:
        """扫 inbox：新用户消息 → push_message 唤醒 YUI（节流防刷屏）。"""
        inbox_dir = os.path.join(_server.data_dir(), "inbox")
        try:
            names = sorted(os.listdir(inbox_dir))
        except OSError:
            return
        for name in names:
            if not name.endswith(".json"):
                continue
            path = os.path.join(inbox_dir, name)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except (OSError, ValueError):
                continue
            text = str(data.get("text") or "").strip()
            if not text:
                self._safe_remove(path)
                continue
            if self._fail_map.get(name, 0) >= _MAX_CONSECUTIVE_FAIL:
                continue  # 连续失败过多：停止尝试，保留文件等人处理
            if time.time() - self._last_inject_ts < _INBOX_COOLDOWN:
                continue
            ok = self._inject_to_yui(text)
            if ok:
                self._last_inject_ts = time.time()
                self._safe_remove(path)
                self._fail_map.pop(name, None)
            else:
                self._fail_map[name] = self._fail_map.get(name, 0) + 1

    def _inject_to_yui(self, text: str) -> bool:
        """push_message 注入聊天消息触发 YUI 行动。失败静默返回 False。"""
        try:
            self.ctx.push_message(
                visibility=[],
                ai_behavior="respond",
                message=_SEND_NOTE.format(text=text[:300]),
                parts=[{"type": "text", "text": _SEND_NOTE.format(text=text[:300])}],
                source=self.plugin_id,
                coalesce_key="community_chat_msg",
                priority=0,
                metadata={"community_chat": True},
            )
            return True
        except Exception:  # noqa: BLE001
            logger.debug("push_message failed", exc_info=True)
            return False

    def _consume_outbox(self) -> None:
        """扫 outbox：YUI 的回复 → 归入 messages.jsonl（面板轮询可见）。"""
        outbox_dir = os.path.join(_server.data_dir(), "outbox")
        try:
            names = sorted(os.listdir(outbox_dir))
        except OSError:
            return
        for name in names:
            if not name.endswith(".json"):
                continue
            path = os.path.join(outbox_dir, name)
            if name in self._notified_outbox:
                self._safe_remove(path)
                continue
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except (OSError, ValueError):
                continue
            text = str(data.get("text") or "").strip()
            if not text:
                self._notified_outbox.add(name)
                self._safe_remove(path)
                continue
            try:
                _server.append_message({"sender": "YUI", "text": text, "role": "yui"})
            except OSError:
                continue  # 写入失败保留文件重试
            self._notified_outbox.add(name)
            self._safe_remove(path)

    @staticmethod
    def _safe_remove(path: str) -> None:
        try:
            os.remove(path)
        except OSError:
            pass

    # ══════════ LLM 工具（YUI 侧） ══════════

    @llm_tool(
        name="community_chat_send",
        description=(
            "把你在喵宇宙社区聊天面板上的回复发出去（发回本地聊天记录，聊天面板会显示）。"
            "何时用：用户通过聊天面板（社区网页右下角聊天窗 / 本地聊天页）给你发了消息，"
            "你要回复时——只发内容本身，不要发指令句。参数 text：你要回复的完整内容（最长 2000 字）。"
            "注意：这是回复用户聊天消息的唯一通道；无需回复时不要调用。"
        ),
        parameters={
            "type": "object",
            "properties": {"text": {"type": "string", "description": "要发送的回复内容"}},
            "required": ["text"],
        },
    )
    async def community_chat_send(self, text: str) -> dict:
        text = (text or "").strip()
        if not text:
            return {"ok": False, "error": "empty message"}
        if len(text) > 2000:
            return {"ok": False, "error": "message too long (2000)"}
        outbox_dir = os.path.join(_server.data_dir(), "outbox")
        os.makedirs(outbox_dir, exist_ok=True)
        rid = f"{int(time.time() * 1000)}-{os.getpid()}"
        path = os.path.join(outbox_dir, f"{rid}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"sender": "YUI", "text": text, "role": "yui", "rid": rid}, f, ensure_ascii=False)
        return {"ok": True, "sent": True, "note": "已写入聊天记录，面板稍后显示"}

    @llm_tool(
        name="community_chat_read",
        description=(
            "读取喵宇宙社区聊天面板的最新聊天记录（含你之前回复的历史）。"
            "何时用：需要回顾你和用户在聊天面板说过什么、或用户消息被唤醒后想看看上下文时。"
            "参数 limit：最多读几条（默认 10）。返回按时间正序的最近记录。"
        ),
        parameters={
            "type": "object",
            "properties": {"limit": {"type": "integer", "description": "最多返回条数，默认 10"}},
        },
    )
    async def community_chat_read(self, limit: int = 10) -> dict:
        try:
            limit = max(1, min(int(limit), 100))
        except (TypeError, ValueError):
            limit = 10
        all_msgs = _server.read_all_messages()
        return {"ok": True, "messages": all_msgs[-limit:]}

    @llm_tool(
        name="community_chat_status",
        description=(
            "查看喵宇宙社区聊天服务的运行状态（本地服务是否在线、聊天记录条数、社区动态代理是否可用）。"
            "何时用：需要确认聊天面板/本地服务是否正常时。"
        ),
        parameters={"type": "object", "properties": {}},
    )
    async def community_chat_status(self) -> dict:
        return {
            "ok": True,
            "service": "http://127.0.0.1:7870/chat",
            "server_started": bool(self._server_started),
            "messages": len(_server.read_all_messages()),
            "feed": "https://community.project-neko.cn/api/feed",
        }


def server_port() -> int:
    return int(os.environ.get("NEKO_COMMUNITY_CHAT_PORT") or _server.DEFAULT_PORT)


def _port_in_use(port: int) -> bool:
    import socket

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", port))
        return False
    except OSError:
        return True
    finally:
        s.close()
