"""
QMTD MiniQMT 会话有限重连测试。

Responsibilities:
    - 验证 `xtdata.run()` 非预期结束后能够重建全部期望行情流。
    - 验证会话恢复保持进程身份并递增会话代次。
    - 验证旧回调和断线前未完成 bar 不会进入 Redis 发布链路。
    - 验证软期限耗尽后服务进入 ERROR 并抛出恢复异常。

Internal Dependencies:
    - core.realtime_service.RealtimeSubscriptionService

External Systems:
    - None，测试使用 fake xtdata，不连接 MiniQMT 或 Redis。
"""
from __future__ import annotations

import unittest
from typing import Any, Callable, Dict, List
from unittest import mock

from core.realtime_service import (
    RealtimeConfig,
    RealtimeRecoveryError,
    RealtimeSubscriptionService,
    SERVICE_ERROR,
    SERVICE_STOPPING,
)


class _FakeClient:
    """
    始终返回已连接状态的 xtdata 客户端替身。
    """

    @staticmethod
    def is_connected() -> bool:
        """返回连接状态。"""
        return True


class _FakePublisher:
    """
    收集发布结果的 Redis Publisher 替身。
    """

    def __init__(self) -> None:
        self.messages: List[Dict[str, Any]] = []

    def publish(self, payload: Dict[str, Any]) -> None:
        """
        保存发布消息。

        Args:
            payload (Dict[str, Any]): 标准行情 payload。

        Returns:
            None
        """
        self.messages.append(payload)


class _RecoveringXtdata:
    """
    首轮断线、第二轮人工停止的 xtdata 故障注入替身。
    """

    def __init__(self) -> None:
        self.reconnect_calls = 0
        self.run_calls = 0
        self.next_sub_id = 100
        self.callbacks: List[Callable[[Dict[str, Any]], None]] = []
        self.unsubscribe_calls: List[int] = []
        self.disconnect_calls = 0

    def reconnect(self) -> _FakeClient:
        """记录重连并返回已连接客户端。"""
        self.reconnect_calls += 1
        return _FakeClient()

    def subscribe_quote(self, **kwargs: Any) -> int:
        """记录回调并返回递增订阅 ID。"""
        self.next_sub_id += 1
        self.callbacks.append(kwargs["callback"])
        return self.next_sub_id

    def unsubscribe_quote(self, sub_id: int) -> None:
        """记录候选订阅清理。"""
        self.unsubscribe_calls.append(sub_id)

    def disconnect(self) -> None:
        """记录会话断开。"""
        self.disconnect_calls += 1

    def run(self) -> None:
        """首轮制造残留 bar 后断线，恢复轮验证新旧回调隔离。"""
        self.run_calls += 1
        if self.run_calls == 1:
            self.callbacks[0](
                {"510050.SH": [{"time": "20260804 09:31:00", "close": 2.5}]}
            )
            raise RuntimeError("行情服务连接断开")

        # 旧会话回调即使晚到，也不能推进断线前的 09:31 bar。
        self.callbacks[0](
            {"510050.SH": [{"time": "20260804 09:32:00", "close": 2.6}]}
        )
        self.callbacks[-1](
            {
                "510050.SH": [
                    {"time": "20260804 09:33:00", "close": 2.7},
                    {"time": "20260804 09:34:00", "close": 2.8},
                ]
            }
        )
        raise KeyboardInterrupt


class _FailingXtdata:
    """
    初始连接成功、运行中恢复持续失败的 xtdata 替身。
    """

    def __init__(self) -> None:
        self.reconnect_calls = 0

    def reconnect(self) -> _FakeClient:
        """初次成功，后续恢复均失败。"""
        self.reconnect_calls += 1
        if self.reconnect_calls == 1:
            return _FakeClient()
        raise RuntimeError("MiniQMT 尚未恢复")

    @staticmethod
    def run() -> None:
        """模拟已建立会话非预期结束。"""
        raise RuntimeError("行情服务连接断开")

    @staticmethod
    def disconnect() -> None:
        """模拟断开接口。"""


class _PartialRecoveringXtdata:
    """
    首次恢复部分订阅失败、第二次恢复全部成功的 xtdata 替身。
    """

    def __init__(self) -> None:
        self.reconnect_calls = 0
        self.subscribe_calls = 0
        self.unsubscribe_calls: List[int] = []
        self.run_calls = 0

    def reconnect(self) -> _FakeClient:
        """返回可用客户端。"""
        self.reconnect_calls += 1
        return _FakeClient()

    def subscribe_quote(self, **kwargs: Any) -> Any:
        """第一次恢复的第二个流返回 None，制造部分失败。"""
        self.subscribe_calls += 1
        if self.subscribe_calls == 4:
            return None
        return 100 + self.subscribe_calls

    def unsubscribe_quote(self, sub_id: int) -> None:
        """记录失败候选订阅清理。"""
        self.unsubscribe_calls.append(sub_id)

    def run(self) -> None:
        """首轮断线，恢复后人工停止。"""
        self.run_calls += 1
        if self.run_calls == 1:
            raise RuntimeError("行情服务连接断开")
        raise KeyboardInterrupt

    @staticmethod
    def disconnect() -> None:
        """模拟断开接口。"""


class _StoppingXtdata:
    """
    在 `run()` 内触发人工停止的 xtdata 替身。
    """

    def __init__(self) -> None:
        self.service: Any = None
        self.reconnect_calls = 0

    def reconnect(self) -> _FakeClient:
        """建立初始会话。"""
        self.reconnect_calls += 1
        return _FakeClient()

    def run(self) -> None:
        """模拟人工停止导致 SDK 抛出连接断开异常。"""
        self.service.stop()
        raise RuntimeError("行情服务连接断开")

    @staticmethod
    def disconnect() -> None:
        """模拟断开接口。"""


class TestRealtimeReconnect(unittest.TestCase):
    """
    MiniQMT 会话有限恢复测试集。
    """

    def test_reconnect_keeps_instance_and_rebuilds_streams(self) -> None:
        """验证同进程恢复、会话代次递增和旧回调隔离。"""
        fake_xtdata = _RecoveringXtdata()
        publisher = _FakePublisher()
        cfg = RealtimeConfig(
            codes=["510050.SH"],
            periods=["1m"],
            preload_days=0,
            reconnect_timeout_sec=1.0,
            reconnect_backoff_sec=[0.0],
        )

        with mock.patch("core.realtime_service.xtdata", fake_xtdata):
            service = RealtimeSubscriptionService(cfg, publisher)
            original_instance_id = service.protocol_identity()["instance_id"]
            service.run_forever()

        status = service.status()
        self.assertEqual(status["instance_id"], original_instance_id)
        self.assertEqual(status["session_generation"], 2)
        self.assertEqual(status["reconnect_count"], 1)
        self.assertEqual(fake_xtdata.reconnect_calls, 2)
        self.assertEqual(len(fake_xtdata.callbacks), 2)
        self.assertEqual(status["service_state"], SERVICE_STOPPING)
        self.assertEqual(
            status["desired_streams"],
            [{"code": "510050.SH", "period": "1m"}],
        )
        self.assertEqual(status["desired_streams"], status["active_streams"])
        self.assertEqual(
            [item["bar_end_ts"] for item in publisher.messages],
            ["2026-08-04T09:33:00"],
        )

    def test_reconnect_timeout_enters_error(self) -> None:
        """验证恢复软期限耗尽后进入 ERROR 并向入口抛出异常。"""
        fake_xtdata = _FailingXtdata()
        cfg = RealtimeConfig(
            codes=[],
            periods=["1m"],
            preload_days=0,
            reconnect_timeout_sec=0.01,
            reconnect_backoff_sec=[0.002],
        )

        with mock.patch("core.realtime_service.xtdata", fake_xtdata):
            service = RealtimeSubscriptionService(cfg, _FakePublisher())
            with self.assertRaises(RealtimeRecoveryError):
                service.run_forever()

        self.assertEqual(service.status()["service_state"], SERVICE_ERROR)
        self.assertGreaterEqual(fake_xtdata.reconnect_calls, 2)

    def test_partial_candidate_is_discarded_before_retry(self) -> None:
        """验证部分恢复失败时清理候选 ID，并在下一轮重建全部流。"""
        fake_xtdata = _PartialRecoveringXtdata()
        cfg = RealtimeConfig(
            codes=["510050.SH", "518880.SH"],
            periods=["1m"],
            preload_days=0,
            reconnect_timeout_sec=1.0,
            reconnect_backoff_sec=[0.0],
        )

        with mock.patch("core.realtime_service.xtdata", fake_xtdata):
            service = RealtimeSubscriptionService(cfg, _FakePublisher())
            service.run_forever()

        status = service.status()
        self.assertEqual(status["session_generation"], 2)
        self.assertEqual(status["reconnect_count"], 1)
        self.assertEqual(status["desired_streams"], status["active_streams"])
        self.assertIn(103, fake_xtdata.unsubscribe_calls)
        self.assertEqual(fake_xtdata.reconnect_calls, 3)

    def test_manual_stop_does_not_enter_reconnecting(self) -> None:
        """验证人工停止导致的 SDK 断线异常不会触发会话恢复。"""
        fake_xtdata = _StoppingXtdata()
        cfg = RealtimeConfig(codes=[], periods=["1m"], preload_days=0)

        with mock.patch("core.realtime_service.xtdata", fake_xtdata):
            service = RealtimeSubscriptionService(cfg, _FakePublisher())
            fake_xtdata.service = service
            service.run_forever()

        self.assertEqual(fake_xtdata.reconnect_calls, 1)
        self.assertEqual(service.status()["service_state"], SERVICE_STOPPING)
        self.assertEqual(service.status()["reconnect_count"], 0)


if __name__ == "__main__":
    unittest.main()
