"""neko_community_chat 本地消息服务（127.0.0.1:7870）。

纯标准库实现（threading + http.server + json），零第三方依赖：
- POST /api/messages           面板发消息（写入 inbox 供 YUI 唤醒消费）
- GET  /api/messages?after_id= 面板轮询消息（含 YUI 回复，id 递增）
- GET  /api/feed              喵宇宙社区动态流代理（community.project-neko.cn/api/feed）
- GET  /api/history           全量历史（面板首屏）
- GET  /chat                  聊天面板页面（客户端面板）
- GET  /api/health            健康检查

消息目录：%LOCALAPPDATA%/N.E.K.O/neko_community_chat/
  messages.jsonl  全量消息流（面板读写）
  inbox/*.json    待 YUI 消费的新用户消息（插件 tick 读）
  outbox/*.json   YUI 的回复（community_chat_send 写入，插件 tick 读后归入消息流）
"""

from __future__ import annotations

import json
import os
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DEFAULT_PORT = 7870
SOCIAL_FEED_URL = "https://community.project-neko.cn/api/feed"
FEED_TIMEOUT = 12.0


def data_dir() -> str:
    base = os.environ.get("LOCALAPPDATA") or os.path.expanduser("~")
    d = os.path.join(base, "N.E.K.O", "neko_community_chat")
    os.makedirs(os.path.join(d, "inbox"), exist_ok=True)
    os.makedirs(os.path.join(d, "outbox"), exist_ok=True)
    return d


def messages_path() -> str:
    return os.path.join(data_dir(), "messages.jsonl")


def next_id() -> int:
    """消息 ID = 文件行数 + 1（追加写，线程锁保证单调）。"""
    p = messages_path()
    try:
        with open(p, "r", encoding="utf-8") as f:
            return sum(1 for _ in f) + 1
    except OSError:
        return 1


_ID_LOCK = threading.Lock()


def append_message(msg: dict) -> dict:
    """追加一条消息到消息流，返回带 id/ts 的完整记录。"""
    with _ID_LOCK:
        msg["id"] = next_id()
        msg["ts"] = time.strftime("%H:%M:%S")
        p = messages_path()
        with open(p, "a", encoding="utf-8") as f:
            f.write(json.dumps(msg, ensure_ascii=False) + "\n")
        return msg


def write_inbox(msg: dict) -> None:
    """写 inbox：待 YUI 消费的新用户消息（插件后台 tick 扫描）。"""
    rid = f"{int(time.time() * 1000)}"
    path = os.path.join(data_dir(), "inbox", f"{rid}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(msg, f, ensure_ascii=False)
    msg["inbox_file"] = path


def read_messages(after_id: int = 0) -> list[dict]:
    """读消息流中 id > after_id 的消息（轮询增量）。"""
    out: list[dict] = []
    try:
        with open(messages_path(), "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    m = json.loads(line)
                except ValueError:
                    continue
                if int(m.get("id") or 0) > after_id:
                    out.append(m)
    except OSError:
        pass
    return out


def read_all_messages() -> list[dict]:
    try:
        with open(messages_path(), "r", encoding="utf-8") as f:
            return [json.loads(line) for line in f if line.strip()]
    except (OSError, ValueError):
        return []


def fetch_social_feed() -> dict:
    """代理喵宇宙社区动态流（真实接口）。失败如实返回错误，不编造。"""
    req = urllib.request.Request(SOCIAL_FEED_URL, headers={"User-Agent": "Mozilla/5.0 neko-community-chat/0.1"})
    try:
        with urllib.request.urlopen(req, timeout=FEED_TIMEOUT) as resp:
            raw = resp.read(2 * 1024 * 1024).decode("utf-8", errors="replace")
        return {"ok": True, "data": raw}
    except urllib.error.HTTPError as exc:
        return {"ok": False, "error": f"HTTP {exc.code}", "status": exc.code}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": str(exc)[:200]}


class _Handler(BaseHTTPRequestHandler):
    server_version = "neko_community_chat/0.1"

    def _cors(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _json(self, obj: dict, status: int = 200) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def _html(self, body: bytes) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        if path == "/api/health":
            return self._json({"ok": True, "port": DEFAULT_PORT, "service": "neko_community_chat"})
        if path == "/api/history":
            return self._json({"ok": True, "messages": read_all_messages()})
        if path == "/api/messages":
            after = 0
            try:
                after = int((self.path.split("?", 1)[1] or "").split("after_id=")[1])
            except (IndexError, ValueError):
                pass
            return self._json({"ok": True, "messages": read_messages(after)})
        if path == "/api/feed":
            return self._json(fetch_social_feed())
        if path == "/chat" or path == "/":
            try:
                with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "chat.html"), "rb") as f:
                    return self._html(f.read())
            except OSError:
                return self._json({"ok": False, "error": "chat.html missing"}, 500)
        return self._json({"ok": False, "error": "not found"}, 404)

    def do_POST(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        if path != "/api/messages":
            return self._json({"ok": False, "error": "not found"}, 404)
        length = int(self.headers.get("Content-Length") or 0)
        try:
            data = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
        except (ValueError, UnicodeDecodeError):
            return self._json({"ok": False, "error": "bad json"}, 400)
        text = str(data.get("text") or "").strip()
        sender = str(data.get("sender") or "user").strip()[:40]
        if not text:
            return self._json({"ok": False, "error": "empty message"}, 400)
        if len(text) > 2000:
            return self._json({"ok": False, "error": "message too long"}, 400)
        msg = {"sender": sender, "text": text, "role": "user"}
        recorded = append_message(msg)
        try:
            write_inbox(recorded)  # 交给 YUI 唤醒消费
        except OSError:
            pass  # inbox 写失败不阻断回显，唤醒可能失败由插件侧重试
        return self._json({"ok": True, "message": recorded}, 201)


def start_server(port: int = DEFAULT_PORT, host: str = "127.0.0.1") -> ThreadingHTTPServer:
    """启动本地消息服务（独立线程）。只绑本机回环，不暴露局域网。"""
    server = ThreadingHTTPServer((host, port), _Handler)
    t = threading.Thread(target=server.serve_forever, daemon=True, name="neko-community-chat-http")
    t.start()
    return server
