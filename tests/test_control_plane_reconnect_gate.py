"""
Redis 控制面重连门禁单元测试。

Responsibilities:
    - 验证 RECONNECTING 状态下 subscribe/unsubscribe 立即失败。
    - 验证拒绝发生在 Registry 和实时订阅结构修改之前。
    - 验证 ACK 顶层携带实时进程身份字段。

Internal Dependencies:
    - core.control_plane.ControlPlane

External Systems:
    - None，测试不创建真实 Redis 连接。
"""
from __future__ import annotations

import unittest
import json
from typing import Any, Dict, List

from core.control_plane import ControlPlane


class _FailOnUseRegistry:
    """
    任意访问都会使测试失败的 Registry 替身。
    """

    def __getattr__(self, name: str) -> Any:
        """拒绝所有 Registry 操作。"""
        raise AssertionError(f"重连门禁后不应访问 Registry：{name}")


class _ReconnectingService:
    """
    固定返回重连状态的实时服务替身。
    """

    def __init__(self) -> None:
        self.cfg = type("Cfg", (), {"mode": "close_only", "preload_days": 0})
        self.publisher = type("Publisher", (), {"topic": "xt:topic:bar"})
        self.add_calls = 0
        self.remove_calls = 0

    @staticmethod
    def control_mutation_error() -> str:
        """返回重连门禁错误码。"""
        return "service_reconnecting"

    @staticmethod
    def protocol_identity() -> Dict[str, Any]:
        """返回固定实时进程身份。"""
        return {
            "instance_id": "realtime-test-instance",
            "instance_started_at_ms": 1,
            "session_generation": 2,
            "protocol_version": 2,
        }

    def add_subscription(self, **kwargs: Any) -> None:
        """记录不应发生的订阅调用。"""
        self.add_calls += 1

    def remove_subscription(self, **kwargs: Any) -> None:
        """记录不应发生的退订调用。"""
        self.remove_calls += 1

    @staticmethod
    def status() -> Dict[str, Any]:
        """返回重连中的只读状态。"""
        return {
            "service_state": "RECONNECTING",
            "desired_streams": [{"code": "510050.SH", "period": "1m"}],
            "active_streams": [],
        }


class TestControlPlaneReconnectGate(unittest.TestCase):
    """
    控制面重连门禁测试集。
    """

    def _build_control_plane(self) -> tuple[ControlPlane, List[Dict[str, Any]]]:
        """
        构造不连接 Redis 的 ControlPlane 实例。

        Returns:
            tuple[ControlPlane, List[Dict[str, Any]]]: 控制面和 ACK 收集列表。
        """
        service = _ReconnectingService()
        control = object.__new__(ControlPlane)
        control._svc = service
        control._registry = _FailOnUseRegistry()
        control._accept = set()
        acknowledgements: List[Dict[str, Any]] = []

        def capture_ack(strategy_id: str, payload: Dict[str, Any]) -> None:
            merged = {**payload, **service.protocol_identity(), "strategy_id": strategy_id}
            acknowledgements.append(merged)

        control._ack = capture_ack
        return control, acknowledgements

    def test_subscribe_is_rejected_before_registry_change(self) -> None:
        """验证重连期间订阅不会访问 Registry 或实时服务。"""
        control, acknowledgements = self._build_control_plane()
        control._handle_subscribe(
            {
                "strategy_id": "demo",
                "codes": ["510050.SH"],
                "periods": ["1m"],
            }
        )

        self.assertEqual(acknowledgements[0]["error"], "service_reconnecting")
        self.assertEqual(acknowledgements[0]["instance_id"], "realtime-test-instance")
        self.assertEqual(control._svc.add_calls, 0)

    def test_unsubscribe_is_rejected_before_registry_change(self) -> None:
        """验证重连期间退订不会加载或删除 Registry。"""
        control, acknowledgements = self._build_control_plane()
        control._handle_unsubscribe(
            {
                "strategy_id": "demo",
                "sub_id": "old-sub-id",
            }
        )

        self.assertEqual(acknowledgements[0]["error"], "service_reconnecting")
        self.assertEqual(control._svc.remove_calls, 0)

    def test_ack_contains_realtime_process_identity(self) -> None:
        """验证实际 ACK 序列化结果在顶层携带实时进程身份。"""
        service = _ReconnectingService()
        published: List[tuple[str, str]] = []
        control = object.__new__(ControlPlane)
        control._svc = service
        control._ack_prefix = "xt:ctrl:ack"
        control._r = type(
            "RedisPublisher",
            (),
            {"publish": lambda _self, channel, payload: published.append((channel, payload))},
        )()

        control._ack("demo", {"ok": True, "action": "status"})
        payload = json.loads(published[0][1])

        self.assertEqual(published[0][0], "xt:ctrl:ack:demo")
        self.assertEqual(payload["instance_id"], "realtime-test-instance")
        self.assertEqual(payload["session_generation"], 2)
        self.assertEqual(payload["protocol_version"], 2)

    def test_status_remains_available_while_reconnecting(self) -> None:
        """验证重连期间 status 仍返回 desired/active 差异。"""
        service = _ReconnectingService()
        control = object.__new__(ControlPlane)
        control._svc = service
        control._registry = type("Registry", (), {"list_all": lambda _self: ["sub-a"]})()
        acknowledgements: List[Dict[str, Any]] = []
        control._ack = lambda strategy_id, payload: acknowledgements.append(payload)

        control._handle_status({"strategy_id": "demo"})

        status = acknowledgements[0]["status"]
        self.assertTrue(acknowledgements[0]["ok"])
        self.assertEqual(status["service_state"], "RECONNECTING")
        self.assertEqual(status["active_streams"], [])
        self.assertEqual(acknowledgements[0]["subs"], ["sub-a"])


if __name__ == "__main__":
    unittest.main()
