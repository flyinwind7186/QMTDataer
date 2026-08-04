"""
QMTD 真实 MiniQMT 与 Redis 会话集成测试。

Responsibilities:
    - 验证真实 xtdata 空白会话能够进入 READY 并人工停止。
    - 验证随机 Redis 控制通道上的 subscribe/status/unsubscribe 完整链路。
    - 验证协议版本 2 ACK 和固定健康 payload 可被下游读取。

Data Contract:
    - 仅在 `RUN_QMTD_REALTIME_LIVE=1` 时执行。
    - 使用随机 control、ACK、topic、Registry 和健康键，不占用生产默认通道。

Internal Dependencies:
    - core.control_plane.ControlPlane
    - core.health.HealthReporter
    - core.realtime_service.RealtimeSubscriptionService

External Systems:
    - 本机已启动并登录的 MiniQMT。
    - `REDIS_URL` 指向的 Redis，默认 `redis://127.0.0.1:6379/0`。
"""
from __future__ import annotations

import json
import os
import threading
import time
import unittest
from typing import Any, Dict, Optional

import redis

from core.control_plane import ControlPlane
from core.health import HealthReporter
from core.metrics import Metrics
from core.pubsub_publisher import PubSubPublisher
from core.realtime_service import RealtimeConfig, RealtimeSubscriptionService, SERVICE_READY
from tests._helpers import random_suffix, redis_params_from_env


@unittest.skipUnless(
    os.environ.get("RUN_QMTD_REALTIME_LIVE") == "1",
    "仅在显式启用真实 MiniQMT 集成测试时运行",
)
class TestRealtimeSessionLive(unittest.TestCase):
    """
    真实 MiniQMT 与随机 Redis 通道集成测试集。
    """

    def _await_ack(self, pubsub: Any, timeout: float = 8.0) -> Optional[Dict[str, Any]]:
        """
        等待并解析一条 ACK。

        Args:
            pubsub (Any): 已订阅 ACK 通道的 Redis PubSub。
            timeout (float): 最大等待秒数。

        Returns:
            Optional[Dict[str, Any]]: ACK 字典，超时返回 `None`。
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            message = pubsub.get_message(ignore_subscribe_messages=True, timeout=0.2)
            if message and isinstance(message.get("data"), str):
                return json.loads(message["data"])
        return None

    def test_real_blank_session_control_and_health(self) -> None:
        """验证真实空白会话、控制命令和健康键的完整链路。"""
        params = redis_params_from_env()
        suffix = random_suffix()
        control_channel = f"xt:ctrl:sub:live:{suffix}"
        ack_prefix = f"xt:ctrl:ack:live:{suffix}"
        registry_prefix = f"xt:bridge:live:{suffix}"
        topic = f"xt:topic:bar:live:{suffix}"
        health_key = f"xt:bridge:health:live:{suffix}"
        strategy_id = f"live-{suffix}"
        ack_channel = f"{ack_prefix}:{strategy_id}"

        client = redis.Redis.from_url(params["url"], decode_responses=True)
        publisher = PubSubPublisher(
            host=params["host"],
            port=params["port"],
            password=params["password"],
            db=params["db"],
            topic=topic,
        )
        service = RealtimeSubscriptionService(
            RealtimeConfig(codes=[], periods=["1m"], preload_days=0),
            publisher,
        )
        control = ControlPlane(
            host=params["host"],
            port=params["port"],
            password=params["password"],
            db=params["db"],
            channel=control_channel,
            ack_prefix=ack_prefix,
            registry_prefix=registry_prefix,
            svc=service,
        )
        health = HealthReporter(
            host=params["host"],
            port=params["port"],
            password=params["password"],
            db=params["db"],
            key_prefix="xt:bridge:health",
            current_key=health_key,
            metrics=Metrics(),
            interval_sec=1,
            ttl_sec=5,
            state_provider=service.health_snapshot,
        )
        ack_pubsub = client.pubsub()
        ack_pubsub.subscribe(ack_channel)
        while ack_pubsub.get_message(timeout=0.01):
            pass

        service_errors = []

        def run_service() -> None:
            try:
                service.run_forever()
            except Exception as exc:
                service_errors.append(exc)

        service_thread = threading.Thread(target=run_service, name="QMTDLiveSessionTest")
        try:
            self.assertTrue(publisher.ping())
            self.assertTrue(control.ping())
            control.clear_registry()
            control.start()
            self.assertTrue(control.wait_until_ready(5.0))
            health.start()
            service_thread.start()

            deadline = time.time() + 10.0
            while time.time() < deadline and service.status()["service_state"] != SERVICE_READY:
                time.sleep(0.1)
            self.assertEqual(service.status()["service_state"], SERVICE_READY)

            client.publish(
                control_channel,
                json.dumps(
                    {
                        "action": "subscribe",
                        "strategy_id": strategy_id,
                        "codes": ["510050.SH"],
                        "periods": ["1m"],
                        "preload_days": 0,
                    },
                    ensure_ascii=False,
                ),
            )
            subscribe_ack = self._await_ack(ack_pubsub)
            self.assertIsNotNone(subscribe_ack)
            self.assertTrue(subscribe_ack["ok"])
            self.assertEqual(subscribe_ack["protocol_version"], 2)
            sub_id = subscribe_ack["sub_id"]

            client.publish(
                control_channel,
                json.dumps({"action": "status", "strategy_id": strategy_id}),
            )
            status_ack = self._await_ack(ack_pubsub)
            self.assertIsNotNone(status_ack)
            self.assertEqual(status_ack["status"]["service_state"], SERVICE_READY)
            self.assertEqual(
                status_ack["status"]["desired_streams"],
                status_ack["status"]["active_streams"],
            )

            health_deadline = time.time() + 3.0
            health_payload: Dict[str, Any] = {}
            while time.time() < health_deadline:
                raw_health = client.get(health_key)
                health_payload = json.loads(raw_health) if raw_health else {}
                if health_payload.get("service_state") == SERVICE_READY:
                    break
                time.sleep(0.1)
            self.assertEqual(health_payload["service"], "qmtdataer-realtime")
            self.assertEqual(health_payload["service_state"], SERVICE_READY)

            client.publish(
                control_channel,
                json.dumps(
                    {
                        "action": "unsubscribe",
                        "strategy_id": strategy_id,
                        "sub_id": sub_id,
                    }
                ),
            )
            unsubscribe_ack = self._await_ack(ack_pubsub)
            self.assertTrue(unsubscribe_ack["ok"])
        finally:
            service.stop()
            if service_thread.ident is not None:
                service_thread.join(timeout=8.0)
            control.stop()
            if control.ident is not None:
                control.join(timeout=2.0)
            health.stop()
            if health.ident is not None:
                health.join(timeout=2.0)
            control.clear_registry()
            client.delete(health_key)
            ack_pubsub.close()

        self.assertFalse(service_thread.is_alive())
        self.assertEqual(service_errors, [])


if __name__ == "__main__":
    unittest.main()
