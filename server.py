"""neko_community_chat 本地消息服务（127.0.0.1:7870）+ 节点互联（v0.2.0）。

纯标准库实现（threading + http.server + json + node.py），零第三方依赖：
- POST /api/messages           面板发消息（写入 inbox 供 YUI 唤醒消费，并广播给 peer）
- GET  /api/messages?after_id= 面板轮询消息（含 YUI 回复，id 递增）
- POST /api/relay/inbox        远端 peer LAN 直推入口（校验来源 + 限速）
- GET  /api/peers              在线节点列表（LAN + 中继可见 peer）
- POST /api/posts              社区墙发帖（msg_type=post，广播给所有 peer）
- GET  /api/feed               社区动态（官方 feed + 自建墙帖子合并）
- GET  /api/settings POST /api/settings   节点昵称/公网中继 URL
- GET  /api/history            全量历史（面板首屏）
- GET  /chat                   聊天面板页面（客户端面板）
- GET  /api/health             健康检查

消息目录：%LOCALAPPDATA%/N.E.K.O/neko_community_chat/
  messages.jsonl  全量消息流（面板读写）
  inbox/*.json    待 YUI 消费的新用户消息（插件 tick 读）
  outbox/*.json   YUI 的回复（community_chat_send 写入，插件 tick 读后归入消息流）
  peer.json       节点身份（peer_id / name）
  settings.json   面板设置（昵称、中继 URL）
"""

from __future__ import annotations

import json
import os
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import node as node_mod

DEFAULT_PORT = 7870
LAN_HTTP_PORT = 7871  # 局域网互推专用口（绑 0.0.0.0，只开放 /api/relay/inbox）
SOCIAL_FEED_URL = "https://community.project-neko.cn/api/feed"
FEED_TIMEOUT = 12.0

# 模块级节点（server 启动时由插件注入）
_NODE: node_mod.Node | None = None
_NODE_LOCK = threading.Lock()


def set_node(n: node_mod.Node | None) -> None:
    global _NODE
    with _NODE_LOCK:
        _NODE = n


def get_node() -> node_mod.Node | None:
    with _NODE_LOCK:
        return _NODE


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
    req = urllib.request.Request(SOCIAL_FEED_URL, headers={"User-Agent": "Mozilla/5.0 neko-community-chat/0.2"})
    try:
        with urllib.request.urlopen(req, timeout=FEED_TIMEOUT) as resp:
            raw = resp.read(2 * 1024 * 1024).decode("utf-8", errors="replace")
        return {"ok": True, "data": raw}
    except urllib.error.HTTPError as exc:
        return {"ok": False, "error": f"HTTP {exc.code}", "status": exc.code}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": str(exc)[:200]}


def read_posts() -> list[dict]:
    """自建社区墙帖子（本地消息流中的 msg_type=post）。"""
    return [m for m in read_all_messages() if (m.get("msg_type") or "chat") == "post"]


def local_posts_feed() -> dict:
    return {"ok": True, "posts": read_posts()}


def incoming_remote(msg: dict) -> None:
    """远端 peer 消息落地：chat → 消息流 + inbox（唤醒本机 YUI）；post → 仅进消息流。"""
    m = dict(msg)
    m.setdefault("origin", "lan")
    msg_type = m.get("msg_type") or "chat"
    if msg_type == "post":
        append_message(m)
    else:
        recorded = append_message(m)
        try:
            write_inbox(recorded)
        except OSError:
            pass


def _is_private_ip(ip: str) -> bool:
    if ip == "127.0.0.1" or ip == "::1":
        return True
    parts = ip.split(".")
    if len(parts) != 4:
        return False
    a = int(parts[0])
    if a == 10:
        return True
    if a == 172 and 16 <= int(parts[1]) <= 31:
        return True
    if a == 192 and parts[1] == "168":
        return True
    return False


def save_settings(d: dict) -> dict:
    """保存面板设置（昵称/中继 URL），返回合并后的完整设置。"""
    p = os.path.join(data_dir(), "settings.json")
    old = {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            old = json.load(f)
    except (OSError, ValueError):
        pass
    merged = dict(old)
    for k in ("name", "relay_url"):
        if d.get(k) is not None:
            merged[k] = str(d[k]).strip()[:200]
    try:
        with open(p, "w", encoding="utf-8") as f:
            json.dump(merged, f, ensure_ascii=False, indent=2)
    except OSError:
        pass
    n = get_node()
    if n is not None:
        n.refresh()
    return merged


def load_settings() -> dict:
    p = os.path.join(data_dir(), "settings.json")
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


class _Handler(BaseHTTPRequestHandler):
    server_version = "neko_community_chat/0.2"

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
            # 聊天轮询只返回 chat 消息（社区墙帖子走 /api/feed）
            return self._json({"ok": True, "messages": [m for m in read_messages(after) if (m.get("msg_type") or "chat") == "chat"]})
        if path == "/api/feed":
            official = fetch_social_feed()
            return self._json(
                {"ok": True, "official": official, "posts": read_posts()}
            )
        if path == "/api/peers":
            n = get_node()
            peers = n.peers_snapshot() if n is not None else []
            self_info = {}
            if n is not None:
                self_info = {"peer_id": n.peer_id, "name": n.name, "self": True}
            return self._json({"ok": True, "self": self_info, "peers": peers})
        if path == "/api/settings":
            s = load_settings()
            n = get_node()
            if n is not None:
                s.setdefault("name", n.name)
            return self._json({"ok": True, "settings": s})
        if path == "/chat" or path == "/":
            try:
                with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "chat.html"), "rb") as f:
                    return self._html(f.read())
            except OSError:
                return self._json({"ok": False, "error": "chat.html missing"}, 500)
        return self._json({"ok": False, "error": "not found"}, 404)

    def do_POST(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        if path == "/api/relay/inbox":
            return self._post_relay_inbox()
        if path == "/api/posts":
            return self._post_posts()
        if path == "/api/settings":
            return self._post_settings()
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
        target_peer = str(data.get("target_peer") or "").strip()[:64]
        msg = {"sender": sender, "text": text, "role": "user", "msg_type": "chat", "origin": "local",
               "target_peer": target_peer or None}
        recorded = append_message(msg)
        n = get_node()
        if target_peer:
            # 面板定向回复远端用户：不唤醒本机 YUI，直接投递目标节点
            if n is not None:
                n.send_to(target_peer, recorded)
            return self._json({"ok": True, "message": recorded}, 201)
        try:
            write_inbox(recorded)  # 交给 YUI 唤醒消费
        except OSError:
            pass  # inbox 写失败不阻断回显，唤醒可能失败由插件侧重试
        if n is not None:
            n.dispatch(recorded)  # 广播给所有 peer（本机用户消息）
        return self._json({"ok": True, "message": recorded}, 201)

    def _read_json_body(self) -> dict | None:
        length = int(self.headers.get("Content-Length") or 0)
        try:
            return json.loads(self.rfile.read(length).decode("utf-8") or "{}")
        except (ValueError, UnicodeDecodeError):
            return None

    def _post_relay_inbox(self):
        """LAN peer 直推入口：来源 IP 须为私有网段 + 限速；chat→唤醒，post→仅记录。"""
        peer_id = str(self.headers.get("X-Peer-Id") or "").strip()[:64]
        client_ip = self.client_address[0] if self.client_address else ""
        if not peer_id or not _is_private_ip(client_ip):
            return self._json({"ok": False, "error": "forbidden"}, 403)
        n = get_node()
        if n is not None and n.peer_id == peer_id:
            return self._json({"ok": True, "dropped": "self"})  # 自己的消息回环丢弃
        if n is not None and n.rate_limited(peer_id):
            return self._json({"ok": False, "error": "rate limited"}, 429)
        data = self._read_json_body()
        if data is None:
            return self._json({"ok": False, "error": "bad json"}, 400)
        text = str(data.get("text") or "").strip()
        if not text or len(text) > 2000:
            return self._json({"ok": False, "error": "bad message"}, 400)
        msg = {
            "sender": str(data.get("sender") or peer_id[:12])[:40],
            "text": text,
            "role": "user",
            "msg_type": data.get("msg_type") or "chat",
            "origin": "lan",
            "peer_id": peer_id,
            "peer_name": str(data.get("peer_name") or peer_id[:12])[:40],
        }
        incoming_remote(msg)
        return self._json({"ok": True, "received": True}, 200)

    def _post_posts(self):
        """社区墙发帖：进消息流 + 广播给所有 peer。"""
        data = self._read_json_body()
        if data is None:
            return self._json({"ok": False, "error": "bad json"}, 400)
        text = str(data.get("text") or "").strip()
        if not text or len(text) > 2000:
            return self._json({"ok": False, "error": "bad post"}, 400)
        n = get_node()
        name = n.name if n is not None else "社区用户"
        msg = {"sender": name, "text": text, "role": "post", "msg_type": "post", "origin": "local"}
        recorded = append_message(msg)
        if n is not None:
            n.dispatch(recorded)
        return self._json({"ok": True, "post": recorded}, 201)

    def _post_settings(self):
        data = self._read_json_body()
        if data is None:
            return self._json({"ok": False, "error": "bad json"}, 400)
        merged = save_settings(data)
        return self._json({"ok": True, "settings": merged})


_SERVER_PORT: int = DEFAULT_PORT
_LAN_SERVER: object | None = None


def server_port() -> int:
    """当前绑定的 HTTP 端口（start_server 实际绑定端口；未启动时为默认 7870）。"""
    return _SERVER_PORT


def lan_port() -> int:
    """局域网互推端口（start_lan_server 实际绑定；失败时仍返回默认 7871 供广播参考）。"""
    return LAN_HTTP_PORT


def start_server(port: int = DEFAULT_PORT, host: str = "127.0.0.1") -> ThreadingHTTPServer:
    """启动本地消息服务（独立线程）。只绑本机回环，不暴露局域网。"""
    server = ThreadingHTTPServer((host, port), _Handler)
    global _SERVER_PORT
    _SERVER_PORT = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True, name="neko-community-chat-http")
    t.start()
    return server


class _LanHandler(_Handler):
    """局域网互推口：只接受 /api/relay/inbox，其余一律拒绝（防 LAN 内读取聊天历史）。"""

    server_version = "neko_community_chat/0.2-lan"

    def do_GET(self) -> None:  # noqa: N802
        return self._json({"ok": False, "error": "lan endpoint only"}, 404)

    def do_POST(self) -> None:  # noqa: N802
        if self.path.split("?", 1)[0] != "/api/relay/inbox":
            return self._json({"ok": False, "error": "lan endpoint only"}, 404)
        return self._post_relay_inbox()

    def log_message(self, fmt: str, *args) -> None:  # noqa: N802
        pass  # 局域网口静默日志，避免刷屏


def start_lan_server(port: int = LAN_HTTP_PORT, host: str = "0.0.0.0") -> ThreadingHTTPServer | None:
    """启动局域网互推口（0.0.0.0）。端口被占（多实例/防火墙）返回 None，不阻断主服务。"""
    global _LAN_SERVER
    try:
        server = ThreadingHTTPServer((host, port), _LanHandler)
    except OSError:
        return None
    t = threading.Thread(target=server.serve_forever, daemon=True, name="neko-community-chat-lan")
    t.start()
    _LAN_SERVER = server
    return server
