"""neko_community_chat 节点互联层（v0.2.0）。

双通道互联：
- LAN：UDP 广播（7872）发现同网段 peer，HTTP 直推 7870/api/relay/inbox
- 公网：HTTP 轮询中继（Cloudflare Worker，relay_url 可配），register/send/poll

纯标准库实现（socket/threading/urllib），零第三方依赖。
"""

from __future__ import annotations

import json
import logging
import os
import random
import socket
import threading
import time
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)

LAN_PORT = 7872
LAN_HELLO_PREFIX = "neko-cc-v1"
PEER_TTL = 30.0          # 2 轮广播未到即移除
LAN_PUSH_RETRY = 2
RELAY_POLL_INTERVAL = 3.0
RELAY_URL_ENV = "NEKO_COMMUNITY_RELAY"
PORT_ENV = "NEKO_COMMUNITY_CHAT_PORT"
DEFAULT_PORT_HTTP = 7870
MAX_TEXT_LEN = 2000
RATE_LIMIT_PER_PEER = (10.0, 60)  # 10 秒窗口最多 60 条


def _parse_hello(payload: str) -> dict | None:
    """解析 UDP hello 报文。"""
    parts = payload.split("|")
    if len(parts) < 4 or parts[0] != LAN_HELLO_PREFIX:
        return None
    try:
        return {
            "peer_id": parts[1],
            "name": parts[2][:40],
            "port": int(parts[3]),
            "ts": parts[4] if len(parts) > 4 else "",
        }
    except (ValueError, IndexError):
        return None


class Node:
    """节点：peer 身份 + LAN 发现/互推 + 公网中继轮询。

    on_incoming: Callable[[dict], None]  —— 收到远端消息时回调（server 侧负责
    落库 + inbox 唤醒），返回后不再转发（防环）。
    on_settings: 面板设置写回（昵称/中继URL）后由外部调用 node.refresh()。
    """

    def __init__(self, data_dir: str, on_incoming, http_port: int | None = None) -> None:
        self.data_dir = data_dir
        self.on_incoming = on_incoming
        self.peer_file = os.path.join(data_dir, "peer.json")
        self.settings_file = os.path.join(data_dir, "settings.json")
        try:
            self.http_port = int(http_port or os.environ.get(PORT_ENV) or DEFAULT_PORT_HTTP)
        except (TypeError, ValueError):
            self.http_port = DEFAULT_PORT_HTTP

        ident = self._load_identity()
        self.peer_id: str = ident["peer_id"]
        self.name: str = ident["name"]
        self.relay_url: str = self._load_relay_url()

        self.peers: dict[str, dict] = {}  # peer_id -> {ip, port, name, last_seen}
        self._peer_lock = threading.Lock()
        self._rate: dict[str, list[float]] = {}
        self._relay_since = 0
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    # ── 身份与配置 ──────────────────────────────────────────────

    def _load_identity(self) -> dict:
        try:
            with open(self.peer_file, "r", encoding="utf-8") as f:
                d = json.load(f)
            if d.get("peer_id") and d.get("name"):
                return d
        except (OSError, ValueError):
            pass
        host = socket.gethostname() or "pc"
        ident = {
            "peer_id": f"node-{host[:16]}-{random.randrange(16 ** 8):08x}",
            "name": host[:40],
        }
        try:
            with open(self.peer_file, "w", encoding="utf-8") as f:
                json.dump(ident, f, ensure_ascii=False)
        except OSError:
            pass
        return ident

    def _load_relay_url(self) -> str:
        url = os.environ.get(RELAY_URL_ENV, "").strip()
        if url:
            return url.rstrip("/")
        try:
            with open(self.settings_file, "r", encoding="utf-8") as f:
                return str(json.load(f).get("relay_url") or "").strip().rstrip("/")
        except (OSError, ValueError):
            return ""

    def refresh(self) -> None:
        """设置变更后重载：昵称/中继 URL 生效（线程安全：只改下次使用）。"""
        try:
            with open(self.settings_file, "r", encoding="utf-8") as f:
                d = json.load(f)
        except (OSError, ValueError):
            d = {}
        name = str(d.get("name") or "").strip()[:40]
        if name:
            self.name = name
            ident = {"peer_id": self.peer_id, "name": name}
            try:
                with open(self.peer_file, "w", encoding="utf-8") as f:
                    json.dump(ident, f, ensure_ascii=False)
            except OSError:
                pass
        self.relay_url = str(d.get("relay_url") or "").strip().rstrip("/")

    # ── 生命周期 ────────────────────────────────────────────────

    def start(self) -> None:
        if self._threads:
            return
        lan = threading.Thread(target=self._lan_loop, daemon=True, name="neko-cc-lan")
        relay = threading.Thread(target=self._relay_loop, daemon=True, name="neko-cc-relay")
        self._threads = [lan, relay]
        lan.start()
        relay.start()
        logger.info("node %s (%s) started, relay=%s", self.peer_id, self.name, self.relay_url or "-")

    def stop(self) -> None:
        self._stop.set()

    # ── 对外消息分发（本机产生的消息 → 全网络）────────────────────

    def dispatch(self, msg: dict) -> None:
        """本机产生的消息广播到所有 peer（LAN 直推 + 中继广播）。"""
        m = dict(msg)
        m.setdefault("peer_id", self.peer_id)
        m.setdefault("peer_name", self.name)
        peers = self._snapshot_peers()
        for peer in peers:
            self._lan_push(peer, m)
        if self.relay_url:
            self._relay_send(m, target=None)

    def send_to(self, target_peer: str, msg: dict) -> bool:
        """定向投递给指定 peer（YUI 回复远端用户用）。"""
        m = dict(msg)
        m.setdefault("peer_id", self.peer_id)
        m.setdefault("peer_name", self.name)
        m["target_peer"] = target_peer
        peer = self._get_peer(target_peer)
        if peer is not None:
            return self._lan_push(peer, m)
        if self.relay_url:
            return self._relay_send(m, target=target_peer)
        return False

    # ── LAN 通道 ────────────────────────────────────────────────

    def _lan_loop(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.bind(("0.0.0.0", LAN_PORT))
        except OSError as exc:
            logger.warning("LAN bind failed: %s", exc)
            return
        sock.settimeout(1.0)
        last_announce = 0.0
        while not self._stop.is_set():
            try:
                data, addr = sock.recvfrom(512)
                hello = _parse_hello(data.decode("utf-8", errors="replace"))
                if hello and hello["peer_id"] != self.peer_id:
                    self._upsert_peer(hello, ip=addr[0])
            except socket.timeout:
                pass
            except OSError:
                break
            now = time.time()
            if now - last_announce >= 5.0:
                last_announce = now
                try:
                    hello = f"{LAN_HELLO_PREFIX}|{self.peer_id}|{self.name}|{self.http_port}|{int(now)}"
                    sock.sendto(hello.encode("utf-8"), ("255.255.255.255", LAN_PORT))
                except OSError:
                    pass
            self._sweep_peers(now)
        sock.close()

    def _upsert_peer(self, hello: dict, ip: str) -> None:
        with self._peer_lock:
            self.peers[hello["peer_id"]] = {
                "ip": ip,
                "port": hello["port"],
                "name": hello["name"],
                "last_seen": time.time(),
            }

    def _sweep_peers(self, now: float) -> None:
        stale = [pid for pid, p in self.peers.items() if now - p["last_seen"] > PEER_TTL]
        if stale:
            with self._peer_lock:
                for pid in stale:
                    self.peers.pop(pid, None)

    def _snapshot_peers(self) -> list[dict]:
        with self._peer_lock:
            return [dict(p) for p in self.peers.values()]

    def _get_peer(self, peer_id: str) -> dict | None:
        with self._peer_lock:
            p = self.peers.get(peer_id)
            return dict(p) if p else None

    def _lan_push(self, peer: dict, msg: dict) -> bool:
        m = dict(msg)
        m["origin"] = "lan"  # 强制来源标签，远端落地后按局域网来源显示
        url = f"http://{peer['ip']}:{peer['port']}/api/relay/inbox"
        payload = json.dumps(m, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            url, data=payload, method="POST",
            headers={"Content-Type": "application/json", "X-Peer-Id": self.peer_id},
        )
        for attempt in range(LAN_PUSH_RETRY + 1):
            try:
                with urllib.request.urlopen(req, timeout=4) as resp:
                    if resp.status == 200:
                        return True
            except (urllib.error.URLError, OSError):
                if attempt >= LAN_PUSH_RETRY:
                    return False
                time.sleep(0.5)
        return False

    # ── 公网中继通道（HTTP 轮询，兼容 Cloudflare Worker）──────────

    def _relay_loop(self) -> None:
        while not self._stop.is_set():
            if not self.relay_url:
                self._stop.wait(RELAY_POLL_INTERVAL)
                continue
            try:
                self._relay_poll()
            except Exception as exc:  # noqa: BLE001
                logger.debug("relay poll failed: %s", exc)
            self._stop.wait(RELAY_POLL_INTERVAL)

    def _relay_poll(self) -> None:
        url = f"{self.relay_url}/relay/poll?peer_id={self.peer_id}&since={self._relay_since}"
        req = urllib.request.Request(url, headers={"User-Agent": "neko-community-chat/0.2"})
        try:
            with urllib.request.urlopen(req, timeout=6) as resp:
                data = json.loads(resp.read(2 * 1024 * 1024).decode("utf-8") or "{}")
        except (urllib.error.HTTPError, urllib.error.URLError, OSError, ValueError):
            return
        msgs = data.get("messages") or []
        since = int(data.get("since") or 0)
        if since > self._relay_since:
            self._relay_since = since
        for m in msgs:
            if m.get("peer_id") == self.peer_id:
                continue  # 自己的消息回环丢弃
            if m.get("target_peer") and m["target_peer"] != self.peer_id:
                continue  # 不是给我的定向消息
            try:
                self.on_incoming(m)
            except Exception:  # noqa: BLE001
                logger.debug("relay incoming failed", exc_info=True)

    def _relay_send(self, msg: dict, target: str | None) -> bool:
        m = dict(msg)
        m["origin"] = "relay"  # 中继来源标签
        payload = {
            "peer_id": self.peer_id,
            "peer_name": self.name,
            "target": target,
            "message": m,
        }
        req = urllib.request.Request(
            f"{self.relay_url}/relay/send",
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            method="POST",
            headers={"Content-Type": "application/json", "User-Agent": "neko-community-chat/0.2"},
        )
        try:
            with urllib.request.urlopen(req, timeout=6) as resp:
                return resp.status == 200
        except (urllib.error.HTTPError, urllib.error.URLError, OSError):
            return False

    # ── 供 server.py 使用的 peer 查询 ────────────────────────────

    def peers_snapshot(self) -> list[dict]:
        with self._peer_lock:
            items = [(pid, dict(p)) for pid, p in self.peers.items()]
        out = [{"peer_id": pid, "name": p["name"], "ip": p["ip"], "port": p["port"]} for pid, p in items]
        out.sort(key=lambda x: x["name"])
        return out

    def rate_limited(self, peer_id: str) -> bool:
        """防刷：同一 peer 每 10 秒最多 60 条。"""
        now = time.time()
        window, limit = RATE_LIMIT_PER_PEER
        with self._peer_lock:
            hits = [t for t in self._rate.get(peer_id, []) if now - t < window]
            if len(hits) >= limit:
                return True
            hits.append(now)
            self._rate[peer_id] = hits
            return False
