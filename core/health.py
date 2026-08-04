# -*- coding = utf-8 -*-
# @Time : 2025/9/10 14:26
# @Author : EquipmentADV
# @File : health.py
# @Software : PyCharm
# -*- coding: utf-8 -*-
"""健康上报（M3）
类说明：
    - HealthReporter：后台线程，定期将 Metrics 快照与实例信息写入 Redis（带 TTL）。
功能：
    - 供运维与可视化读取当前实例的存活与吞吐指标；
上下游：
    - 上游：run_with_config（实例化并启动）；
    - 下游：Redis（写入字符串或哈希，当前实现为字符串 JSON）。
"""
from __future__ import annotations
import json
import os
import socket
import threading
import time
from typing import Callable, Dict, Optional

try:
    import redis
except Exception:
    redis = None  # type: ignore

from .metrics import Metrics


class HealthReporter(threading.Thread):
    """类说明：健康上报线程"""
    daemon = True

    def __init__(self, host: str, port: int, password: Optional[str], key_prefix: str,
                 metrics: Metrics, interval_sec: int = 5, ttl_sec: int = 20,
                 extra_info: Optional[Dict[str, object]] = None,
                 current_key: Optional[str] = None,
                 state_provider: Optional[Callable[[], Dict[str, object]]] = None,
                 db: int = 0) -> None:
        super().__init__(name="HealthReporter")
        if redis is None:
            raise RuntimeError("未安装 redis 依赖，无法启用健康上报")
        self._cli = redis.Redis(
            host=host,
            port=port,
            password=password,
            db=int(db),
            decode_responses=True,
        )
        self.key_prefix = key_prefix
        self.metrics = metrics
        self.interval = max(1, int(interval_sec))
        self.ttl = max(self.interval * 2, int(ttl_sec))
        self.extra = extra_info or {}
        self.current_key = current_key
        self.state_provider = state_provider
        # 修复：不要覆盖 Thread._stop
        self._stop_evt = threading.Event()
        self._instance_id = self._make_instance_id()

    def _make_instance_id(self) -> str:
        host = socket.gethostname()
        pid = os.getpid()
        tag = self.extra.get("instance_tag") if self.extra else None
        return f"{host}:{pid}:{tag}" if tag else f"{host}:{pid}"

    def stop(self) -> None:
        """方法说明：停止健康上报"""
        self._stop_evt.set()

    def run(self) -> None:
        """
        周期写入实时进程健康快照。

        Returns:
            None
        """
        try:
            while not self._stop_evt.is_set():
                if self.state_provider is not None:
                    payload = dict(self.state_provider())
                    key = self.current_key or f"{self.key_prefix}:current"
                else:
                    payload = {
                        "ts": int(time.time()),
                        "instance_id": self._instance_id,
                        "metrics": self.metrics.snapshot(),
                        "extra": self.extra,
                    }
                    key = f"{self.key_prefix}:{self._instance_id}"
                try:
                    self._cli.set(key, json.dumps(payload, ensure_ascii=False), ex=self.ttl)
                except Exception:
                    # 健康上报失败不应中断行情线程，由 TTL 向下游暴露失联事实。
                    pass
                self._stop_evt.wait(self.interval)
        finally:
            self._delete_owned_current_key()

    def _delete_owned_current_key(self) -> None:
        """
        停止时只删除仍由当前实时实例持有的固定健康键。

        Returns:
            None
        """
        if self.state_provider is None or not self.current_key:
            return
        try:
            current_raw = self._cli.get(self.current_key)
            current = json.loads(current_raw) if current_raw else {}
            own_instance = self.state_provider().get("instance_id")
            if current.get("instance_id") == own_instance:
                self._cli.delete(self.current_key)
        except Exception:
            pass
