"""health 单元测试（M3.2）

说明：使用假 redis 客户端，验证 HealthReporter 的写入循环与停止。
"""
import sys
import types
import time
import unittest
import json

from core.metrics import Metrics


class _FakeRedis:
    def __init__(self, *a, **kw):
        self.set_calls = []
        self.values = {}
        self.delete_calls = []
    def set(self, key, val, ex=None):
        # 记录最后一次写入
        self.set_calls.append((key, val, ex))
        self.values[key] = val
        return True
    def get(self, key):
        return self.values.get(key)
    def delete(self, key):
        self.delete_calls.append(key)
        self.values.pop(key, None)


def _install_fake_redis():
    r = types.ModuleType("redis")
    r.Redis = _FakeRedis
    sys.modules["redis"] = r


def _reload_health():
    sys.modules.pop("core.health", None)
    import core.health  # noqa


class TestHealth(unittest.TestCase):
    """类说明：健康上报线程测试"""

    def test_health_reporter_runs_and_stops(self):
        """测试内容：上报循环与停止
        目的：验证在短时间内至少发生一次 set 写入，且 stop() 能终止线程
        输入：interval_sec=0.05, ttl_sec=1，metrics 有初始快照
        预期输出：_FakeRedis.set 被调用≥1 次，写入键前缀正确
        """
        _install_fake_redis()
        _reload_health()
        from core.health import HealthReporter

        m = Metrics()
        m.inc_published(2)
        hr = HealthReporter(host="127.0.0.1", port=6379, password=None,
                            key_prefix="xt:bridge:health", metrics=m,
                            interval_sec=0.05, ttl_sec=1,
                            extra_info={"instance_tag": "T"})
        hr.start()
        time.sleep(0.2)  # 等待至少 1~2 次循环
        hr.stop()
        hr.join(timeout=1.0)
        rcli = hr._cli  # type: ignore[attr-defined]
        self.assertTrue(len(rcli.set_calls) >= 1)
        key, val, ex = rcli.set_calls[-1]
        self.assertTrue(key.startswith("xt:bridge:health:"))
        self.assertIsInstance(ex, int)

    def test_fixed_health_key_uses_service_snapshot(self):
        """验证固定健康键写入实时进程状态，并在停止时按实例身份清理。"""
        _install_fake_redis()
        _reload_health()
        from core.health import HealthReporter

        snapshot = {
            "service": "qmtdataer-realtime",
            "instance_id": "realtime-instance-a",
            "service_state": "RECONNECTING",
            "session_generation": 1,
            "protocol_version": 2,
        }
        health = HealthReporter(
            host="127.0.0.1",
            port=6379,
            password=None,
            key_prefix="xt:bridge:health",
            current_key="xt:bridge:health:current",
            metrics=Metrics(),
            interval_sec=1,
            ttl_sec=20,
            state_provider=lambda: dict(snapshot),
        )
        health.start()
        time.sleep(0.05)
        written = health._cli.set_calls[-1]
        payload = json.loads(written[1])
        health.stop()
        health.join(timeout=1.0)

        self.assertEqual(written[0], "xt:bridge:health:current")
        self.assertEqual(payload["service_state"], "RECONNECTING")
        self.assertEqual(health._cli.delete_calls, ["xt:bridge:health:current"])
