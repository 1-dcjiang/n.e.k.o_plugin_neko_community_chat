"""mqtt.py 协议帧与去重逻辑单元测试（纯本地，不依赖外网）。"""

import json
import socket
import struct
import threading

from plugin.plugins.neko_community_chat import mqtt as mqtt_mod
from plugin.plugins.neko_community_chat.node import Node


def test_varint_roundtrip():
    for n in (0, 1, 127, 128, 16383, 16384, 2 ** 24 - 1):
        b = mqtt_mod._varint(n)
        # 用 socket 对 _read_varint 做字节流验证
        rsock, wsock = socket.socketpair()
        wsock.sendall(b)
        got = mqtt_mod._read_varint(rsock)
        assert got == n
        rsock.close()
        wsock.close()


def test_encode_str():
    b = mqtt_mod._encode_str("MQTT")
    assert b == b"\x00\x04MQTT"


def test_build_packet():
    pkt = mqtt_mod._build_packet(0x10, b"\x00")
    assert pkt == b"\x10\x01\x00"


def test_handle_publish_dispatch():
    got = {}
    link = mqtt_mod.MQTTLink(
        host="localhost", port=1883, channel="t", peer_id="node-A",
        peer_name="A", on_message=lambda d: got.update(d),
    )
    body = mqtt_mod._encode_str("neko_cc/t/broadcast") + json.dumps(
        {"peer_id": "node-B", "peer_name": "B", "message": {"text": "hi", "origin": "x"}}
    ).encode("utf-8")
    link._handle_publish(0x30, body)  # QoS0
    assert got["peer_id"] == "node-B"
    assert got["message"]["text"] == "hi"


def test_handle_publish_qos1_acks():
    sent = []
    link = mqtt_mod.MQTTLink(
        host="localhost", port=1883, channel="t", peer_id="node-A",
        peer_name="A", on_message=lambda d: None,
    )
    body = (mqtt_mod._encode_str("neko_cc/t/broadcast")
            + struct.pack(">H", 7)
            + json.dumps({"peer_id": "node-B", "message": {"text": "q1"}}).encode("utf-8"))

    class FakeSock:
        def sendall(self, data):
            sent.append(data)

    link._handle_publish(0x33, body, FakeSock())  # QoS1 → 应回 PUBACK(0x40, id=7)
    assert any(b[:1] == b"\x40" and b"\x00\x07" in b for b in sent)


def test_handle_publish_self_loop():
    got = []
    link = mqtt_mod.MQTTLink(
        host="localhost", port=1883, channel="t", peer_id="node-A",
        peer_name="A", on_message=got.append,
    )
    body = mqtt_mod._encode_str("neko_cc/t/broadcast") + json.dumps(
        {"peer_id": "node-A", "message": {"text": "loop"}}
    ).encode("utf-8")
    link._handle_publish(0x30, body)
    assert got == []


def test_handle_publish_bad_json():
    got = []
    link = mqtt_mod.MQTTLink(
        host="localhost", port=1883, channel="t", peer_id="node-A",
        peer_name="A", on_message=got.append,
    )
    body = mqtt_mod._encode_str("neko_cc/t/broadcast") + b"{not-json"
    link._handle_publish(0x30, body)
    assert got == []


def test_publish_topic_routing():
    """广播/定向主题正确性（通过伪造 socket 的 _send_publish）。"""
    link = mqtt_mod.MQTTLink(
        host="localhost", port=1883, channel="myroom", peer_id="node-A",
        peer_name="A", on_message=lambda d: None,
    )
    sent = []

    class FakeSock:
        def sendall(self, data):
            sent.append(data)

    link._send_publish(FakeSock(), {"message": {"text": "hi"}}, target=None)
    link._send_publish(FakeSock(), {"message": {"text": "dm"}}, target="node-B")
    assert b"neko_cc/myroom/broadcast" in sent[0]
    assert b"neko_cc/myroom/peer/node-B" in sent[1]


def test_dedup_seen():
    n = object.__new__(Node)
    n._mqtt_dedup = []
    n._mqtt_lock = threading.Lock()
    m1 = {"peer_id": "node-B", "message": {"text": "hello", "ts": 100}}
    assert n._mqtt_dedup_seen(m1) is True
    assert n._mqtt_dedup_seen(m1) is False  # 重复丢弃
    m2 = {"peer_id": "node-B", "message": {"text": "hello", "ts": 101}}
    assert n._mqtt_dedup_seen(m2) is True  # 时间戳不同 → 放行
