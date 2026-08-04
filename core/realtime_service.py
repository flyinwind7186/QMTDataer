# -*- coding: utf-8 -*-
"""
实时订阅服务（M3 / M3.5）

文件说明（功能 / 特性 / 上下游）：
- 功能：
    * 统一封装 QMT xtdata 的实时订阅能力；
    * 接收 QMT 回调数据（callback(datas)），将其规范化为“宽表”后发布到 Redis（通过外部 Publisher）；
    * 支持启动或动态新增订阅时的历史预热（补齐），以保证首条实时推送的连续性；
    * 支持去重与两种推送模式：'close_only'（默认，仅收盘推送）、'forming_and_close'（生成中也推）；
    * 提供状态查询（当前订阅集 / 最近发布时间等）；
- 特性：
    * 回调签名完全遵循 QMT 官方：callback(datas)，datas 形如 { '600000.SH': [ {...}, ... ] }；
    * 订阅调用使用关键字参数（避免回调被编码进 BSON 的问题）；
    * 去重键采用 (code, period, bar_end_ts)，内部 LRU 控制内存增长；
    * 并发安全：回调在 xtdata 内部线程触发，本类用锁保护关键结构（订阅集 / 去重集）；
- 上下游：
    * 上游：运行入口（scripts/run_with_config.py）及 ControlPlane（动态订阅 / 退订）；
    * 下游：PubSubPublisher（将标准化“宽表”消息发布到 Redis）。

注意：
- QMT/MiniQMT 的历史数据获取通常需要先 download_history_data，再通过 get_* 读取；本服务中的“预热”步骤会在新增订阅时按配置进行补齐；
- 实时回调的数据字段（尤其时间与收盘标记）在不同品种/站点可能存在差异，此处做了通用容错（例如 isClose/isClosed/closed 多字段容错）。
"""

from __future__ import annotations
import logging
import math
import random
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, Any, List, Tuple, Optional
from collections import deque
from datetime import datetime, timedelta, timezone
from numbers import Integral, Real

import numpy as np
import pandas as pd

from core.time_utils import parse_local_naive_time_series

CN_TZ = timezone(timedelta(hours=8))
MARKET_NUMERIC_DECIMALS = 10
PROTOCOL_VERSION = 2

SERVICE_STARTING = "STARTING"
SERVICE_READY = "READY"
SERVICE_RECONNECTING = "RECONNECTING"
SERVICE_STOPPING = "STOPPING"
SERVICE_ERROR = "ERROR"


class RealtimeRecoveryError(RuntimeError):
    """
    MiniQMT 会话在有限恢复窗口内未能恢复。

    该异常应传播到进程入口，使真实行情进程以非零状态退出。
    """


class RealtimeServiceUnavailable(RuntimeError):
    """
    实时服务当前状态不允许修改底层订阅。
    """

# QMT xtdata
try:  # pragma: no cover
    from xtquant import xtdata  # type: ignore
except Exception as _e:  # pragma: no cover
    xtdata = None  # type: ignore
    _XT_IMPORT_ERR = _e
else:  # pragma: no cover
    _XT_IMPORT_ERR = None


# =========================
# 配置数据类
# =========================

@dataclass
class RealtimeConfig:
    """类说明：实时订阅配置
    功能：描述订阅行为（模式 / 周期 / 标的 / 预热等）。
    上下游：由外部加载（config_loader）并传入 RealtimeSubscriptionService。
    """
    mode: str = "close_only"                     # 'close_only' | 'forming_and_close'
    periods: List[str] = field(default_factory=lambda: ["1m"])
    codes: List[str] = field(default_factory=list)
    close_delay_ms: int = 100                    # 若做延迟收盘判定（当前版本未启用队列延迟，仅保留配置）
    preload_days: int = 3                        # 新增订阅时历史预热天数（0 表示不预热）
    reconnect_timeout_sec: float = 60.0
    reconnect_backoff_sec: List[float] = field(
        default_factory=lambda: [1.0, 2.0, 4.0, 8.0, 10.0]
    )

    # 可选：去重容量限制（LRU）
    dedup_max_size: int = 50000

    @dataclass
    class MockConfig:
        """类说明：Mock 行情配置
        功能：控制是否启用随机游走行情，以及相关行为参数。
        """
        enabled: bool = False
        base_price: float = 10.0
        volatility: float = 0.002       # 对数收益标准差
        step_seconds: float = 5.0       # 每根 bar 生成的时间间隔（秒）
        seed: Optional[int] = None
        volume_mean: float = 1_000_000
        volume_std: float = 200_000
        source: str = "mock"

    mock: MockConfig = field(default_factory=MockConfig)


# =========================
# 内部状态结构
# =========================

@dataclass
class _BarState:
    """类说明：单个标的/周期的 bar 状态缓存
    功能：保存当前 forming bar，记录最近一次已发布的 bar 结束时间；
    上下游：仅供 RealtimeSubscriptionService 内部使用。
    """
    current_payload: Optional[Dict[str, Any]] = None
    current_dt: Optional[datetime] = None
    last_published_dt: Optional[datetime] = None

# =========================
# Mock 行情生成器
# =========================

class MockBarFeeder(threading.Thread):
    """类说明：Mock 行情生成线程
    功能：根据订阅集合构建随机游走的 bar 序列，并以 datas 形式投喂 RealtimeSubscriptionService。
    """

    @dataclass
    class _State:
        last_dt: Optional[datetime] = None
        last_price: Optional[float] = None
        vol: Optional[float] = None

    @dataclass
    class _HistoryBaseline:
        base_price: float
        vol: Optional[float] = None
        latest_dt: Optional[datetime] = None

    _PERIOD_SECONDS = {
        "1m": 60,
        "1h": 3600,
        "1d": 86400,
    }

    def __init__(self, svc: "RealtimeSubscriptionService", cfg: RealtimeConfig.MockConfig,
                 logger: Optional[logging.Logger] = None) -> None:
        super().__init__(name="MockBarFeeder", daemon=True)
        self._svc = svc
        self._cfg = cfg
        self._log = logger or logging.getLogger("MockBarFeeder")
        self._stop_evt = threading.Event()
        self._states: Dict[Tuple[str, str], MockBarFeeder._State] = {}
        self._rng = random.Random(cfg.seed)
        self._code_params: Dict[str, float] = {}  # per-code fallback 波动率
        self._history_cache: Dict[Tuple[str, str], Optional[MockBarFeeder._HistoryBaseline]] = {}
        self._mock_clock_dt: Optional[datetime] = None

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    def stop(self) -> None:
        self._stop_evt.set()

    def run(self) -> None:
        self._log.info("mock feeder started (step=%.2fs base=%.2f vol=%.4f)", float(self._cfg.step_seconds or 1.0),
                       self._cfg.base_price, self._cfg.volatility)
        step = max(0.1, float(self._cfg.step_seconds or 1.0))
        while not self._stop_evt.is_set():
            try:
                self._emit_cycle()
            except Exception:  # pragma: no cover
                self._log.exception("mock feeder emit failed")
            finally:
                self._stop_evt.wait(step)
        self._log.info("mock feeder stopped")

    # ------------------------------------------------------------------
    # 内部逻辑
    # ------------------------------------------------------------------
    def _emit_cycle(self) -> None:
        subs = set(self._svc._list_subscriptions())
        if not subs:
            return
        for key in list(self._states.keys()):
            if key not in subs:
                self._states.pop(key, None)

        now = datetime.now(CN_TZ)
        one_min_subs = sorted(key for key in subs if key[1] == "1m")
        if one_min_subs:
            self._emit_cn_stock_1m_cycle(one_min_subs, now)

        other_subs = sorted(key for key in subs if key[1] != "1m")
        for code, period in other_subs:
            self._emit_legacy_period_cycle(code, period, now)

    def _emit_legacy_period_cycle(self, code: str, period: str, now: datetime) -> None:
        """按旧逻辑生成非 1m Mock 行情，避免扩大交易时钟改动范围。"""
        delta = self._period_delta(period)
        if not delta:
            self._log.debug("mock feeder skip unsupported period=%s", period)
            return
        state = self._states.setdefault((code, period), MockBarFeeder._State())
        if state.last_dt is None or state.last_price is None:
            base_dt = self._align_base(now, delta) - delta
            self._init_price_state(code, period, state)
            seed_row = self._build_row(code, period, base_dt, state.last_price, close_price=state.last_price)
            self._svc._on_datas(period, {code: [seed_row]})
            state.last_dt = base_dt

        next_dt = state.last_dt + delta
        next_price = self._next_price(state.last_price, state.vol)
        row = self._build_row(code, period, next_dt, state.last_price, close_price=next_price)
        state.last_dt = next_dt
        state.last_price = next_price
        self._svc._on_datas(period, {code: [row]})

    def _emit_cn_stock_1m_cycle(self, subs: List[Tuple[str, str]], now: datetime) -> None:
        """按 A 股交易分钟推进 1m Mock 行情，所有标的共享同一个模拟时钟。"""
        cycle_dt = self._ensure_mock_clock(subs, now)
        next_dt = self._next_cn_stock_minute(cycle_dt)

        for code, period in subs:
            state = self._states.setdefault((code, period), MockBarFeeder._State())
            if state.last_price is None:
                self._init_price_state(code, period, state)

            next_price = self._next_price(state.last_price, state.vol)
            current_row = self._build_row(code, period, cycle_dt, state.last_price, close_price=next_price)
            # close_only 状态机需要看到下一根 bar，才能确认并发布当前 bar。
            lookahead_row = self._build_row(code, period, next_dt, next_price, close_price=next_price)
            self._svc._on_datas(period, {code: [current_row, lookahead_row]})
            state.last_dt = cycle_dt
            state.last_price = next_price

        self._mock_clock_dt = next_dt

    def _init_price_state(self, code: str, period: str, state: _State) -> None:
        """初始化单标的价格和波动率；时间由全局 Mock 时钟统一控制。"""
        hist_base = self._get_history_baseline(code, period)
        if hist_base:
            base_price = hist_base.base_price
            state.vol = self._vol_for_code(code, hist_base.vol)
            self._log.info("[Mock] %s %s 使用历史基准 base=%.4f vol=%.6f", code, period, base_price, state.vol)
        else:
            base_price = self._initial_price(code)
            state.vol = self._vol_for_code(code, None)
            self._log.info("[Mock] %s %s 历史获取失败，使用默认基准 base=%.4f vol=%.6f", code, period, base_price, state.vol)
        state.last_price = base_price

    def _ensure_mock_clock(self, subs: List[Tuple[str, str]], now: datetime) -> datetime:
        """初始化或返回全局 1m Mock 模拟时钟。"""
        if self._mock_clock_dt is not None:
            return self._mock_clock_dt

        latest_candidates: List[datetime] = []
        for code, period in subs:
            hist_base = self._get_history_baseline(code, period)
            if hist_base and hist_base.latest_dt is not None:
                latest_candidates.append(hist_base.latest_dt)

        if latest_candidates:
            latest_dt = max(latest_candidates)
            self._mock_clock_dt = self._next_cn_stock_minute(latest_dt)
            self._log.info("[Mock] 全局模拟时钟使用历史最大时间初始化: latest=%s next=%s",
                           latest_dt.isoformat(), self._mock_clock_dt.isoformat())
        else:
            self._mock_clock_dt = self._ceil_cn_stock_minute(now)
            self._log.info("[Mock] 全局模拟时钟使用当前时间初始化: now=%s next=%s",
                           now.isoformat(), self._mock_clock_dt.isoformat())
        return self._mock_clock_dt

    @staticmethod
    def _period_delta(period: str) -> Optional[timedelta]:
        secs = MockBarFeeder._PERIOD_SECONDS.get(period)
        if not secs:
            return None
        return timedelta(seconds=secs)

    @staticmethod
    def _align_base(now: datetime, delta: timedelta) -> datetime:
        secs = int(delta.total_seconds())
        epoch = int(now.timestamp())
        aligned_epoch = (epoch // secs) * secs
        return datetime.fromtimestamp(aligned_epoch, CN_TZ)

    @staticmethod
    def _as_cn_aware(dt: datetime) -> datetime:
        if dt.tzinfo is None:
            return dt.replace(tzinfo=CN_TZ)
        return dt.astimezone(CN_TZ)

    @staticmethod
    def _ceil_cn_stock_minute(dt: datetime) -> datetime:
        """返回当前时间之后最近的 A 股理论交易分钟。"""
        dt = MockBarFeeder._as_cn_aware(dt)
        base = dt.replace(second=0, microsecond=0)
        candidate = base + timedelta(minutes=1)
        return MockBarFeeder._normalize_cn_stock_minute(candidate)

    @staticmethod
    def _next_cn_stock_minute(dt: datetime) -> datetime:
        """返回给定时间之后的下一根 A 股 1m bar 标签。"""
        dt = MockBarFeeder._as_cn_aware(dt)
        candidate = dt.replace(second=0, microsecond=0) + timedelta(minutes=1)
        return MockBarFeeder._normalize_cn_stock_minute(candidate)

    @staticmethod
    def _normalize_cn_stock_minute(candidate: datetime) -> datetime:
        """将候选分钟归入 A 股日内交易分钟，暂不处理周末和节假日。"""
        candidate = MockBarFeeder._as_cn_aware(candidate).replace(second=0, microsecond=0)
        day = candidate.date()
        am_start = datetime(day.year, day.month, day.day, 9, 30, tzinfo=CN_TZ)
        am_end = datetime(day.year, day.month, day.day, 11, 30, tzinfo=CN_TZ)
        pm_start = datetime(day.year, day.month, day.day, 13, 0, tzinfo=CN_TZ)
        pm_end = datetime(day.year, day.month, day.day, 15, 0, tzinfo=CN_TZ)

        if candidate < am_start:
            return am_start
        if candidate <= am_end:
            return candidate
        if candidate < pm_start:
            return pm_start
        if candidate <= pm_end:
            return candidate
        next_day = day + timedelta(days=1)
        return datetime(next_day.year, next_day.month, next_day.day, 9, 30, tzinfo=CN_TZ)

    def _initial_price(self, code: str) -> float:
        jitter = (abs(hash(code)) % 500) / 100.0
        base = max(0.01, self._cfg.base_price + jitter)
        return round(base, 4)

    def _next_price(self, prev: float, vol: Optional[float]) -> float:
        sigma = vol if vol is not None else self._cfg.volatility
        drift = self._rng.gauss(0, sigma)
        price = prev * math.exp(drift)
        return round(max(0.01, price), 4)

    def _get_history_baseline(self, code: str, period: str) -> Optional[_HistoryBaseline]:
        """读取并缓存单标的历史基准，避免同一轮重复访问 xtdata。"""
        key = (code, period)
        if key not in self._history_cache:
            self._history_cache[key] = self._history_baseline(code, period)
        return self._history_cache[key]

    def _history_baseline(self, code: str, period: str) -> Optional[_HistoryBaseline]:
        """尝试从 xtdata 读取近期行情，提取最新价、波动率和最新时间；失败则返回 None。"""
        if xtdata is None:
            return None
        # 先尝试下载近期数据以提升命中率（失败忽略）
        try:
            end_s = datetime.now(CN_TZ).strftime("%Y%m%d")
            start_s = (datetime.now(CN_TZ) - timedelta(days=60)).strftime("%Y%m%d")
            xtdata.download_history_data(
                stock_code=code,
                period=period,
                start_time=start_s,
                end_time=end_s,
                incrementally=True,
            )
        except Exception:
            pass
        data = None
        # 首选 get_market_data_ex
        try:
            data = xtdata.get_market_data_ex(
                stock_list=[code],
                period=period,
                start_time="",
                end_time="",
                count=50,
                dividend_type="none",
                fill_data=False,
                field_list=[],
            )
        except Exception:
            data = None
        # 回退 get_market_data
        if not isinstance(data, dict) or not data:
            try:
                data = xtdata.get_market_data(
                    stock_list=[code],
                    period=period,
                    start_time="",
                    end_time="",
                    count=50,
                    dividend_type="none",
                    fill_data=False,
                    field_list=[],
                )
            except Exception as e:
                self._log.debug("mock feeder history fetch failed: %s %s err=%s", code, period, e)
                data = None
        if not isinstance(data, dict) or not data:
            return None

        closes: List[float] = []
        latest_dt: Optional[datetime] = None
        # 结构 1：field->DataFrame
        if "close" in data and "time" in data:
            close_df = data.get("close")
            if hasattr(close_df, "loc") and code in close_df.index:
                series = close_df.loc[code]
                closes = [float(x) for x in series.dropna().tolist() if pd.notna(x)]
            time_df = data.get("time")
            if hasattr(time_df, "loc") and code in time_df.index:
                latest_dt = self._latest_time_from_series(time_df.loc[code])
        else:
            # 结构 2：code->DataFrame（index 通常为日期/时间，不含 code）
            for val in data.values():
                if isinstance(val, pd.DataFrame) and "close" in val.columns:
                    closes = [float(x) for x in val["close"].dropna().tolist() if pd.notna(x)]
                    if "time" in val.columns:
                        latest_dt = self._latest_time_from_series(val["time"])
                    else:
                        latest_dt = self._latest_time_from_series(pd.Series(val.index))
                    break
        if not closes:
            return None
        base_price = closes[-1]
        vol = None
        if len(closes) >= 3:
            try:
                rets = np.diff(np.log(np.array(closes)))
                if len(rets) > 0:
                    vol = float(np.std(rets))
            except Exception:
                vol = None
        return MockBarFeeder._HistoryBaseline(base_price=base_price, vol=vol, latest_dt=latest_dt)

    @staticmethod
    def _latest_time_from_series(series: Any) -> Optional[datetime]:
        """从 xtdata 返回的时间列或索引中提取最新本地时间，再转为北京时间 aware。"""
        try:
            parsed = parse_local_naive_time_series(pd.Series(series).dropna()).dropna()
        except Exception:
            return None
        if parsed.empty:
            return None
        latest = pd.Timestamp(parsed.iloc[-1]).to_pydatetime()
        return latest.replace(tzinfo=CN_TZ)

    def _vol_for_code(self, code: str, hist_vol: Optional[float]) -> float:
        """返回单标的使用的波动率：优先历史估计，否则对默认波动率做 per-code 抖动。"""
        if hist_vol is not None and hist_vol > 0:
            return hist_vol
        if code in self._code_params:
            return self._code_params[code]
        factor = 0.5 + (abs(hash(code)) % 101) / 100.0  # 0.5 ~ 1.51
        vol = max(1e-6, self._cfg.volatility * factor)
        self._code_params[code] = vol
        return vol

    def _build_row(self, code: str, period: str, bar_dt: datetime, open_price: float, close_price: float) -> Dict[str, Any]:
        spread = abs(close_price - open_price) or 0.01
        swing = abs(self._rng.gauss(0, spread * 0.3))
        high = max(open_price, close_price) + swing
        low = min(open_price, close_price) - swing
        high = round(max(high, open_price, close_price), 4)
        low = round(max(0.01, min(low, open_price, close_price)), 4)
        volume = max(1, int(abs(self._rng.gauss(self._cfg.volume_mean, self._cfg.volume_std))))
        amount = round(close_price * volume, 2)
        ts_str = bar_dt.astimezone(CN_TZ).replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:%S")
        return {
            "time": ts_str,
            "code": code,
            "period": period,
            "open": round(open_price, 4),
            "high": high,
            "low": low,
            "close": round(close_price, 4),
            "volume": volume,
            "amount": amount,
            "isClosed": True,
            "source": self._cfg.source,
        }
# =========================
# 主服务类
# =========================

class RealtimeSubscriptionService:
    """类说明：实时订阅服务
    功能：
        - 在 xtdata 中注册订阅（subscribe_quote），使用官方签名的回调 callback(datas) 接收行情；
        - 将回调数据规范化为“宽表”并通过 Publisher 推送到 Redis；
        - 在新增订阅时可先进行历史预热，确保时序连续；
        - 提供订阅状态查询；
    上游：
        - run_with_config.py 在启动时构造本服务并调用 run_forever()；
        - ControlPlane 在运行时调用 add_subscription/remove_subscription 动态变更订阅；
    下游：
        - PubSubPublisher（负责 Redis 发布），本类不关心 Redis 细节；
    """

    def __init__(self, cfg: RealtimeConfig, publisher, cache=None, logger: Optional[logging.Logger] = None):
        """
        :param cfg: RealtimeConfig，订阅行为配置
        :param publisher: 发布器对象，需提供 publish(dict) 方法（下游为 Redis PubSub）
        :param cache: 可选的历史预热实现（需提供 ensure_downloaded_date_range(codes, periods, days)）
        :param logger: 可选日志器
        """

        self.cfg = cfg
        self.publisher = publisher
        self.cache = cache
        self._log = logger or logging.getLogger(__name__)
        self._mock_feeder: Optional[MockBarFeeder] = None

        # 并发保护
        self._lock = threading.RLock()

        # `_subs` 保留为业务期望流的兼容视图，底层真实状态由 `_active_streams` 表示。
        self._subs: set[Tuple[str, str]] = set()
        self._active_streams: set[Tuple[str, str]] = set()
        self._sub_ref_counts: Dict[Tuple[str, str], int] = {}
        self._quote_sub_ids: Dict[Tuple[str, str], Any] = {}

        # 会话身份和状态仅属于实时行情进程，不复用 HTTP Bridge 的实例身份。
        self._instance_id = str(uuid.uuid4())
        self._instance_started_at_ms = int(time.time() * 1000)
        self._session_generation = 1
        self._reconnect_count = 0
        self._service_state = SERVICE_STARTING
        self._last_error: Optional[str] = None
        self._stop_evt = threading.Event()
        self._active_callback_token: Optional[object] = object()
        self._startup_prepared = False

        # 去重：LRU + 集合
        self._dedup_set: set[Tuple[Any, ...]] = set()
        self._dedup_q: deque[Tuple[Any, ...]] = deque()
        self._dedup_max = int(self.cfg.dedup_max_size or 50000)

        # 最近发布时间（观测用途）
        self._last_pub_ts: Dict[Tuple[str, str], float] = {}
        # bar 状态机缓存（key = (code, period)）
        self._bar_states: Dict[Tuple[str, str], _BarState] = {}

        if not self.cfg.mock.enabled and xtdata is None:
            raise RuntimeError(f"缺少依赖或 QMT/MiniQMT 未正确安装：{_XT_IMPORT_ERR}")

    # ----------------------------------------------------------------------
    # 入口：阻塞运行
    # ----------------------------------------------------------------------
    def run_forever(self) -> None:
        """
        阻塞运行实时行情生命周期。

        真实行情会话非预期结束时，在软期限内重连并重建全部期望行情流。人工停止时不进入
        重连；恢复失败时抛出 `RealtimeRecoveryError`，由进程入口形成非零退出码。

        Returns:
            None
        """
        if self.cfg.mock.enabled:
            if self.cfg.codes and self.cfg.periods:
                self.add_subscription(self.cfg.codes, self.cfg.periods, preload_days=self.cfg.preload_days)
            self._set_service_state(SERVICE_READY)
            self._log.info("[RT] mock 行情模式启用（step=%.2fs, base=%.2f, vol=%.4f）",
                           float(self.cfg.mock.step_seconds or 1.0),
                           self.cfg.mock.base_price,
                           self.cfg.mock.volatility)
            self._mock_feeder = MockBarFeeder(self, self.cfg.mock, logger=self._log.getChild("MockFeeder"))
            self._mock_feeder.start()
            try:
                while True:
                    time.sleep(1.0)
            except KeyboardInterrupt:
                self._log.info("[RT] 接收到中断信号，准备停止 Mock 行情。")
            finally:
                self.stop()
                if self._mock_feeder:
                    self._mock_feeder.join(timeout=2.0)
            return

        if xtdata is None:
            raise RuntimeError(f"缺少依赖或 QMT/MiniQMT 未正确安装：{_XT_IMPORT_ERR}")

        try:
            self._prepare_real_session()
            while not self._stop_evt.is_set():
                self._log.info(
                    "[RT] xtdata.run() 开始阻塞运行 generation=%d",
                    self._session_generation,
                )
                run_error: Optional[Exception] = None
                try:
                    xtdata.run()
                except KeyboardInterrupt:
                    self._log.info("[RT] 接收到人工中断，停止真实行情服务。")
                    self._stop_evt.set()
                except Exception as exc:
                    if self._stop_evt.is_set():
                        self._log.info("[RT] 人工停止已使 xtdata.run() 结束。")
                    else:
                        run_error = exc
                        self._log.warning("[RT] xtdata.run() 非预期结束: %s", exc)

                if self._stop_evt.is_set():
                    break
                if not self._recover_real_session(run_error):
                    if self._stop_evt.is_set():
                        break
                    message = self._last_error or "MiniQMT 会话恢复失败"
                    raise RealtimeRecoveryError(message)
        except RealtimeRecoveryError:
            raise
        except Exception as exc:
            if not self._stop_evt.is_set():
                self._set_service_state(SERVICE_ERROR, f"{type(exc).__name__}: {exc}")
            raise
        finally:
            if self._service_state != SERVICE_ERROR:
                self._set_service_state(SERVICE_STOPPING)
            self._disconnect_xtdata()
            self._log.info("[RT] xtdata.run() 生命周期结束 state=%s", self._service_state)

    def _prepare_real_session(self) -> None:
        """
        建立初始 xtdata 会话并注册配置中的初始行情流。

        Returns:
            None

        Note:
            初次启动失败不使用 60 秒恢复窗口，异常直接交给进程入口形成非零退出。
        """
        if self._startup_prepared:
            return
        self._connect_xtdata()
        if self.cfg.codes and self.cfg.periods:
            self.add_subscription(
                self.cfg.codes,
                self.cfg.periods,
                preload_days=self.cfg.preload_days,
            )
        with self._lock:
            if self._subs != self._active_streams:
                raise RuntimeError("初始行情流未全部建立")
            self._startup_prepared = True
        self._set_service_state(SERVICE_READY)

    def _connect_xtdata(self) -> Any:
        """
        建立 xtdata 会话并执行最小连接状态校验。

        Returns:
            Any: xtdata 客户端对象；测试替身未提供客户端时可为 `None`。
        """
        if hasattr(xtdata, "reconnect"):
            client = xtdata.reconnect()
        elif hasattr(xtdata, "get_client"):
            client = xtdata.get_client()
        else:
            client = None
        if client is not None and hasattr(client, "is_connected") and not client.is_connected():
            raise RuntimeError("xtdata 客户端未连接 MiniQMT")
        return client

    def _recover_real_session(self, run_error: Optional[Exception]) -> bool:
        """
        在软期限内恢复 MiniQMT 会话和全部期望行情流。

        Args:
            run_error (Optional[Exception]): 导致 `xtdata.run()` 结束的原始异常。

        Returns:
            bool: 全部行情流恢复成功时返回 `True`，否则返回 `False`。
        """
        self._begin_reconnecting(run_error)
        timeout = max(0.0, float(self.cfg.reconnect_timeout_sec))
        deadline = time.monotonic() + timeout
        backoffs = [max(0.0, float(item)) for item in self.cfg.reconnect_backoff_sec] or [1.0]
        attempt = 0

        while not self._stop_evt.is_set() and time.monotonic() < deadline:
            if attempt > 0:
                delay = backoffs[min(attempt - 1, len(backoffs) - 1)]
                remaining = deadline - time.monotonic()
                if remaining <= 0 or self._stop_evt.wait(min(delay, remaining)):
                    break

            attempt += 1
            candidate_ids: Dict[Tuple[str, str], Any] = {}
            candidate_token = object()
            with self._lock:
                desired = sorted(self._subs)
                candidate_generation = self._session_generation + 1

            try:
                if time.monotonic() >= deadline:
                    break
                self._log.info("[RT] 尝试恢复 MiniQMT 会话 attempt=%d", attempt)
                self._connect_xtdata()
                if time.monotonic() >= deadline:
                    raise TimeoutError("xtdata.reconnect() 返回时已超过恢复软期限")

                for code, period in desired:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("重建底层订阅时已超过恢复软期限")
                    sub_id = self._register_one(
                        code,
                        period,
                        callback_generation=candidate_generation,
                        callback_token=candidate_token,
                    )
                    if sub_id is None:
                        raise RuntimeError(f"底层订阅未返回 ID: {code} {period}")
                    candidate_ids[(code, period)] = sub_id
                    if time.monotonic() >= deadline:
                        raise TimeoutError("subscribe_quote() 返回时已超过恢复软期限")

                with self._lock:
                    if set(desired) != self._subs:
                        raise RuntimeError("恢复期间 desired_streams 发生变化")
                    self._quote_sub_ids = candidate_ids
                    self._active_streams = set(desired)
                    self._session_generation = candidate_generation
                    self._active_callback_token = candidate_token
                    self._reconnect_count += 1
                    self._last_error = None
                    self._service_state = SERVICE_READY
                self._log.info(
                    "[RT] MiniQMT 会话恢复成功 generation=%d streams=%d",
                    candidate_generation,
                    len(desired),
                )
                return True
            except KeyboardInterrupt:
                self._stop_evt.set()
                self._last_error = None
                self._discard_candidate_subscriptions(candidate_ids)
                return False
            except Exception as exc:
                self._set_service_state(
                    SERVICE_RECONNECTING,
                    f"{type(exc).__name__}: {exc}",
                )
                self._log.warning("[RT] MiniQMT 会话恢复失败 attempt=%d err=%s", attempt, exc)
                self._discard_candidate_subscriptions(candidate_ids)

        if self._stop_evt.is_set():
            return False
        with self._lock:
            self._service_state = SERVICE_ERROR
            if not self._last_error:
                self._last_error = "MiniQMT 会话恢复超过软期限"
        self._log.error("[RT] MiniQMT 会话恢复最终失败: %s", self._last_error)
        return False

    def _begin_reconnecting(self, run_error: Optional[Exception]) -> None:
        """
        切换到重连状态并废弃旧会话的底层状态。

        Args:
            run_error (Optional[Exception]): 会话结束异常；正常返回时为 `None`。

        Returns:
            None
        """
        with self._lock:
            self._service_state = SERVICE_RECONNECTING
            self._last_error = (
                f"{type(run_error).__name__}: {run_error}"
                if run_error is not None
                else "xtdata.run() 非预期返回"
            )
            self._active_streams.clear()
            self._quote_sub_ids.clear()
            self._bar_states.clear()
            self._active_callback_token = None

    def _discard_candidate_subscriptions(self, candidate_ids: Dict[Tuple[str, str], Any]) -> None:
        """
        尽力退订本轮已创建但未提交的候选订阅。

        Args:
            candidate_ids (Dict[Tuple[str, str], Any]): 候选底层订阅 ID。

        Returns:
            None
        """
        for (code, period), sub_id in candidate_ids.items():
            self._unsubscribe_one(code, period, sub_id)

    @staticmethod
    def _disconnect_xtdata() -> None:
        """
        尽力断开 xtdata 会话。

        Returns:
            None
        """
        if xtdata is None or not hasattr(xtdata, "disconnect"):
            return
        try:
            xtdata.disconnect()
        except Exception:
            pass

    # ----------------------------------------------------------------------
    # 动态增删订阅
    # ----------------------------------------------------------------------
    def add_subscription(self, codes: List[str], periods: List[str], preload_days: Optional[int] = None) -> None:
        """新增订阅引用，并在首次引用时注册底层行情。

        Args:
            codes (List[str]): 标的列表。
            periods (List[str]): 周期列表，例如 `1m`、`1h`、`1d`。
            preload_days (Optional[int]): 覆盖配置中的预热天数，`0` 表示不预热。

        Note:
            引用计数按 `(code, period)` 统计。重复订阅同一行情流只增加计数，不重复调用
            `xtdata.subscribe_quote`。
        """
        self._ensure_subscription_mutation_allowed()
        days = int(preload_days if preload_days is not None else self.cfg.preload_days or 0)
        if not self.cfg.mock.enabled and days > 0:
            self._preload_history(codes, periods, days)

        with self._lock:
            for c in codes:
                for p in periods:
                    key = (c, p)
                    current_ref = self._sub_ref_counts.get(key, 0)
                    if current_ref > 0:
                        self._sub_ref_counts[key] = current_ref + 1
                        self._log.info("[RT] 订阅引用增加: %s %s ref=%d", c, p, current_ref + 1)
                        continue
                    if not self.cfg.mock.enabled:
                        sub_id = self._register_one(c, p)
                        if sub_id is None:
                            raise RuntimeError(f"底层订阅未返回 ID: {c} {p}")
                        self._quote_sub_ids[key] = sub_id
                        self._active_streams.add(key)
                    self._subs.add(key)
                    if self.cfg.mock.enabled:
                        self._active_streams.add(key)
                    self._sub_ref_counts[key] = 1
                    if self.cfg.mock.enabled:
                        self._log.info("[RT] Mock 订阅已登记: %s %s ref=1", c, p)
                    else:
                        self._log.info("[RT] 订阅已注册: %s %s ref=1", c, p)

    def remove_subscription(self, codes: List[str], periods: List[str]) -> None:
        """移除订阅引用，并在引用归零时取消底层行情。

        Args:
            codes (List[str]): 标的列表。
            periods (List[str]): 周期列表。

        Returns:
            None

        Note:
            如果同一 `(code, period)` 仍被其他策略引用，只减少计数，不调用底层退订。
        """
        self._ensure_subscription_mutation_allowed()
        with self._lock:
            for c in codes:
                for p in periods:
                    key = (c, p)
                    current_ref = self._sub_ref_counts.get(key, 0)
                    if current_ref <= 0:
                        continue
                    if current_ref > 1:
                        self._sub_ref_counts[key] = current_ref - 1
                        self._log.info("[RT] 订阅引用减少: %s %s ref=%d", c, p, current_ref - 1)
                        continue
                    if not self.cfg.mock.enabled:
                        ok = self._unsubscribe_one(c, p, self._quote_sub_ids.get(key))
                        if not ok:
                            continue
                    self._quote_sub_ids.pop(key, None)
                    self._sub_ref_counts.pop(key, None)
                    self._subs.discard(key)
                    self._active_streams.discard(key)
                    self._bar_states.pop(key, None)
                    if self.cfg.mock.enabled:
                        self._log.info("[RT] Mock 订阅已移除: %s %s", c, p)
                    else:
                        self._log.info("[RT] 订阅已移除: %s %s", c, p)

    def _unsubscribe_one(self, code: str, period: str, sub_id: Any = None) -> bool:
        """取消单个底层行情订阅。

        Args:
            code (str): 标的代码。
            period (str): 周期。
            sub_id (Any): `xtdata.subscribe_quote` 返回的订阅 ID。

        Returns:
            bool: 底层退订是否成功。

        Note:
            当前 MiniQMT 环境只接受订阅 ID。保留旧签名回退，兼容早期环境或测试替身。
        """
        if sub_id is not None:
            try:
                xtdata.unsubscribe_quote(sub_id)
                return True
            except Exception as e:
                self._log.warning("[RT] unsubscribe by sub_id failed: %s %s sub_id=%s err=%s",
                                  code, period, sub_id, e)
                return False

        try:
            xtdata.unsubscribe_quote(stock_code=code, period=period)
            return True
        except TypeError:
            try:
                xtdata.unsubscribe_quote(code)
                return True
            except Exception as e:
                self._log.warning("[RT] unsubscribe failed: %s %s err=%s", code, period, e)
                return False
        except Exception as e:
            self._log.warning("[RT] unsubscribe failed: %s %s err=%s", code, period, e)
            return False

    def _list_subscriptions(self) -> List[Tuple[str, str]]:
        with self._lock:
            return list(self._subs)

    def stop(self) -> None:
        """
        请求实时服务人工停止。

        Returns:
            None
        """
        self._stop_evt.set()
        if self._service_state != SERVICE_ERROR:
            self._set_service_state(SERVICE_STOPPING)
        if self.cfg.mock.enabled and self._mock_feeder:
            self._mock_feeder.stop()
        elif not self.cfg.mock.enabled:
            self._disconnect_xtdata()

    def _ensure_subscription_mutation_allowed(self) -> None:
        """
        拒绝在重连、停止或错误状态下修改订阅结构。

        Returns:
            None
        """
        if self.cfg.mock.enabled:
            return
        with self._lock:
            state = self._service_state
        if state in {SERVICE_RECONNECTING, SERVICE_STOPPING, SERVICE_ERROR}:
            raise RealtimeServiceUnavailable(f"service_{state.lower()}")

    def control_mutation_error(self) -> Optional[str]:
        """
        返回控制面修改订阅时应使用的错误码。

        Returns:
            Optional[str]: READY 时返回 `None`，其他状态返回稳定错误码。
        """
        with self._lock:
            state = self._service_state
        if state == SERVICE_READY:
            return None
        return f"service_{state.lower()}"

    # ----------------------------------------------------------------------
    # 订阅注册与回调处理
    # ----------------------------------------------------------------------
    @staticmethod
    def _normalize_bar_end_ts(raw: Any) -> Optional[str]:
        if raw is None:
            return None
        parsed = parse_local_naive_time_series(pd.Series([raw])).iloc[0]
        if pd.isna(parsed):
            return None
        return pd.Timestamp(parsed).strftime("%Y-%m-%dT%H:%M:%S")

    def _register_one(
        self,
        code: str,
        period: str,
        callback_generation: Optional[int] = None,
        callback_token: Optional[object] = None,
    ) -> Any:
        """
        注册单标的、单周期底层订阅。

        Args:
            code (str): xtdata 标的代码。
            period (str): xtdata 行情周期。
            callback_generation (Optional[int]): 回调所属会话代次。
            callback_token (Optional[object]): 回调所属尝试的内部隔离令牌。

        Returns:
            Any: `xtdata.subscribe_quote()` 返回的内部订阅 ID。
        """
        generation = callback_generation if callback_generation is not None else self._session_generation
        token = callback_token if callback_token is not None else self._active_callback_token

        def _cb(datas: Dict[str, List[Dict[str, Any]]]):
            if not self._is_current_callback(generation, token):
                return
            try:
                self._on_datas(period, datas)
            except Exception:
                self._log.exception("[RT] 回调处理异常 period=%s", period)

        # 官方建议：订阅实时部分时 count=0；历史由预热负责
        return xtdata.subscribe_quote(
            stock_code=code,
            period=period,
            start_time="",
            end_time="",
            count=0,
            callback=_cb
        )

    def _is_current_callback(self, generation: int, token: Optional[object]) -> bool:
        """
        判断回调是否属于当前已提交的 xtdata 会话。

        Args:
            generation (int): 回调注册时的会话代次。
            token (Optional[object]): 回调注册尝试的内部令牌。

        Returns:
            bool: 回调属于当前 READY 会话时返回 `True`。
        """
        with self._lock:
            return (
                self._service_state == SERVICE_READY
                and generation == self._session_generation
                and token is not None
                and token is self._active_callback_token
            )

    def _on_datas(self, period: str, datas: Dict[str, List[Dict[str, Any]]]) -> None:
        """方法说明：处理 QMT 回调数据（datas）
        功能：
            - 先做字段归一化；
            - 按时间戳排序后交给 bar 状态机；
        """
        if not datas:
            return

        for code, rows in datas.items():
            if not rows:
                continue
            normalized_rows: List[Tuple[datetime, Dict[str, Any]]] = []
            for row in rows:
                payload = self._build_payload_from_row(code, period, row)
                if not payload:
                    continue
                bar_iso = self._normalize_bar_end_ts(payload.get("bar_end_ts"))
                if not bar_iso:
                    continue
                payload["bar_end_ts"] = bar_iso
                try:
                    bar_dt = datetime.fromisoformat(bar_iso)
                except Exception:
                    self._log.debug("[RT] bar_end_ts 无法解析: code=%s period=%s ts=%s", code, period, bar_iso)
                    continue
                normalized_rows.append((bar_dt, payload))

            if not normalized_rows:
                continue

            normalized_rows.sort(key=lambda item: item[0])
            for bar_dt, payload in normalized_rows:
                self._handle_bar_update(code, period, bar_dt, payload)

    # ----------------------------------------------------------------------
    # bar 状态机：基于时间戳判定收盘
    # ----------------------------------------------------------------------
    def _handle_bar_update(self, code: str, period: str,
                           bar_dt: datetime, payload: Dict[str, Any]) -> None:
        """方法说明：维护单标的/周期的 bar 状态并在需要时发布"""
        key = (code, period)
        to_publish: List[Dict[str, Any]] = []
        store_payload = dict(payload)
        store_payload["code"] = code
        store_payload["period"] = period
        # forming 阶段统一视为未收盘，等待时间戳前进触发最终发布
        store_payload["is_closed"] = False

        with self._lock:
            state = self._bar_states.setdefault(key, _BarState())

            if state.current_dt is None:
                state.current_dt = bar_dt
                state.current_payload = store_payload
                if self.cfg.mode == "forming_and_close":
                    forming_payload = dict(store_payload)
                    forming_payload["is_closed"] = False
                    to_publish.append(forming_payload)
            elif bar_dt < state.current_dt:
                if state.last_published_dt and bar_dt <= state.last_published_dt:
                    return
                self._log.debug("[RT] 检测到乱序 bar，已跳过 code=%s period=%s ts=%s current=%s",
                                code, period, bar_dt.isoformat(), state.current_dt.isoformat())
                return
            elif bar_dt == state.current_dt:
                state.current_payload = store_payload
                if self.cfg.mode == "forming_and_close":
                    forming_payload = dict(store_payload)
                    forming_payload["is_closed"] = False
                    to_publish.append(forming_payload)
            else:
                if state.current_payload:
                    finalized = dict(state.current_payload)
                    finalized["is_closed"] = True
                    to_publish.append(finalized)
                    state.last_published_dt = state.current_dt
                state.current_dt = bar_dt
                state.current_payload = store_payload
                if self.cfg.mode == "forming_and_close":
                    forming_payload = dict(store_payload)
                    forming_payload["is_closed"] = False
                    to_publish.append(forming_payload)

        for item in to_publish:
            self._publish_payload(item)

    def _publish_payload(self, payload: Dict[str, Any]) -> None:
        """方法说明：统一处理去重与时间戳刷新后推送到 Redis"""
        code = payload.get("code")
        period = payload.get("period")
        bar_ts = payload.get("bar_end_ts")
        if not code or not period or not bar_ts:
            return
        is_closed = bool(payload.get("is_closed"))
        if self.cfg.mode == "close_only" and not is_closed:
            return
        if self.cfg.mode == "forming_and_close":
            dkey = (code, period, bar_ts, 1 if is_closed else 0)
        else:
            dkey = (code, period, bar_ts)
        if self._is_dup_and_mark(dkey):
            return
        enriched = dict(payload)
        enriched.setdefault("source", "qmt")
        enriched["recv_ts"] = datetime.now(CN_TZ).replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:%S")
        enriched = self._normalize_market_numeric_payload(enriched)
        self.publisher.publish(enriched)
        with self._lock:
            self._last_pub_ts[(code, period)] = time.time()

    @classmethod
    def _normalize_market_numeric_payload(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        """统一规整 Redis 行情 payload 中的数值字段。"""
        return {key: cls._normalize_market_numeric_value(value) for key, value in payload.items()}

    @staticmethod
    def _normalize_market_numeric_value(value: Any) -> Any:
        """将行情数值统一保留 10 位小数，时间、字符串、布尔值保持原样。"""
        if isinstance(value, bool):
            return value
        if isinstance(value, Integral):
            return int(value)
        if isinstance(value, Real):
            numeric = float(value)
            if math.isfinite(numeric):
                return round(numeric, MARKET_NUMERIC_DECIMALS)
            return numeric
        return value

    # ----------------------------------------------------------------------
    # 预热（历史补齐）
    # ----------------------------------------------------------------------
    def _preload_history(self, codes: List[str], periods: List[str], days: int) -> None:
        """方法说明：历史预热
        功能：
            - 若提供了 cache.ensure_downloaded_date_range，则优先使用；
            - 否则回退至逐标的/逐周期调用 xtdata.download_history_data；
        注意：
            - xtdata.download_history_data(stock_code: str, period: str, start_time: str, end_time: str)
              仅接受单只股票代码，且需要 YYYYMMDD（或更精细）格式；
        """
        if days <= 0:
            return

        if self.cache and hasattr(self.cache, "ensure_downloaded_date_range"):
            try:
                self.cache.ensure_downloaded_date_range(codes, periods, days=days)
                self._log.info("[RT] 历史预热(使用cache)完成 days=%d codes=%d periods=%d", days, len(codes), len(periods))
                return
            except Exception as e:
                self._log.warning("[RT] 历史预热(cache)异常，将使用直接下载：%s", e)

        end = datetime.today().date()
        start = end - timedelta(days=days)
        start_s = start.strftime("%Y%m%d")
        end_s = end.strftime("%Y%m%d")

        for c in codes:
            for p in periods:
                try:
                    xtdata.download_history_data(
                        stock_code=c,
                        period=p,
                        start_time=start_s,
                        end_time=end_s,
                        incrementally=True
                    )
                except Exception as e:
                    self._log.warning("[RT] 历史下载异常: %s %s err=%s", c, p, e)
        self._log.info("[RT] 历史预热完成 days=%d", days)

    # ----------------------------------------------------------------------
    # 构建“宽表”payload
    # ----------------------------------------------------------------------
    def _build_payload_from_row(self, code: str, period: str, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        raw_ts = row.get("time") or row.get("Time") or row.get("barTime") or row.get("bar_time")
        bar_end_ts = self._normalize_bar_end_ts(raw_ts)
        if bar_end_ts is None:
            return None

        is_closed = row.get("isClosed")
        if is_closed is None:
            is_closed = row.get("isClose")
        if is_closed is None:
            is_closed = row.get("closed")

        src = row.get("source") or "qmt"
        payload = {
            "code": code,
            "period": period,
            "bar_end_ts": bar_end_ts,
            "is_closed": bool(is_closed) if is_closed is not None else None,
            "open": row.get("open"),
            "high": row.get("high"),
            "low": row.get("low"),
            "close": row.get("close"),
            "volume": row.get("volume"),
            "amount": row.get("amount"),
            "preClose": row.get("preClose"),
            "suspendFlag": row.get("suspendFlag"),
            "openInterest": row.get("openInterest"),
            "settlementPrice": row.get("settlementPrice") or row.get("settelementPrice"),
            "source": src,
        }
        return payload


    # ----------------------------------------------------------------------
    # 去重（LRU）
    # ----------------------------------------------------------------------
    def _is_dup_and_mark(self, key: Tuple[Any, ...]) -> bool:
        """方法说明：判断是否重复并写入 LRU 结构
        功能：
            - 若 key 已在集合中：返回 True；
            - 否则：写入集合与队列，若超容量则弹出最旧项；
        """
        with self._lock:
            if key in self._dedup_set:
                return True
            self._dedup_set.add(key)
            self._dedup_q.append(key)
            if len(self._dedup_q) > self._dedup_max:
                old = self._dedup_q.popleft()
                self._dedup_set.discard(old)
        return False

    # ----------------------------------------------------------------------
    # 状态查询
    # ----------------------------------------------------------------------
    def status(self) -> Dict[str, Any]:
        """
        返回实时服务状态和会话身份。

        Returns:
            Dict[str, Any]: 当前服务状态、期望流、活跃流、引用计数及最近发布时间。
        """
        with self._lock:
            subs = sorted([{
                "code": c,
                "period": p,
                "ref_count": int(self._sub_ref_counts.get((c, p), 0)),
            } for (c, p) in self._active_streams],
                          key=lambda x: (x["code"], x["period"]))
            last_pub = {f"{c}|{p}": ts for (c, p), ts in self._last_pub_ts.items()}
            desired = self._stream_payload(self._subs)
            active = self._stream_payload(self._active_streams)
            payload = {
                "service_state": self._service_state,
                "desired_streams": desired,
                "active_streams": active,
                "subs": subs,
                "last_published": last_pub,
                "reconnect_count": self._reconnect_count,
                "last_error": self._last_error,
            }
            payload.update(self.protocol_identity())
        return payload

    def protocol_identity(self) -> Dict[str, Any]:
        """
        返回 ACK 和健康协议共用的实时进程身份字段。

        Returns:
            Dict[str, Any]: 实例 UUID、启动时间、会话代次和协议版本。
        """
        with self._lock:
            return {
                "instance_id": self._instance_id,
                "instance_started_at_ms": self._instance_started_at_ms,
                "session_generation": self._session_generation,
                "protocol_version": PROTOCOL_VERSION,
            }

    def health_snapshot(self) -> Dict[str, Any]:
        """
        构造固定健康键需要的线程安全快照。

        Returns:
            Dict[str, Any]: QMTD 实时行情进程健康协议 payload。
        """
        with self._lock:
            return {
                "service": "qmtdataer-realtime",
                "instance_id": self._instance_id,
                "instance_started_at_ms": self._instance_started_at_ms,
                "session_generation": self._session_generation,
                "protocol_version": PROTOCOL_VERSION,
                "service_state": self._service_state,
                "reconnect_count": self._reconnect_count,
                "last_error": self._last_error,
                "desired_streams": self._stream_payload(self._subs),
                "active_streams": self._stream_payload(self._active_streams),
                "last_published": {
                    f"{code}|{period}": published_at
                    for (code, period), published_at in self._last_pub_ts.items()
                },
                "updated_at_ms": int(time.time() * 1000),
            }

    def _set_service_state(self, state: str, error: Optional[str] = None) -> None:
        """
        原子更新实时服务状态。

        Args:
            state (str): 目标状态。
            error (Optional[str]): 可选错误摘要。

        Returns:
            None
        """
        with self._lock:
            self._service_state = state
            if error is not None:
                self._last_error = error

    @staticmethod
    def _stream_payload(streams: set[Tuple[str, str]]) -> List[Dict[str, str]]:
        """
        将内部行情流集合转换为稳定排序的协议结构。

        Args:
            streams (set[Tuple[str, str]]): `(code, period)` 集合。

        Returns:
            List[Dict[str, str]]: 按代码和周期排序的行情流列表。
        """
        return [
            {"code": code, "period": period}
            for code, period in sorted(streams)
        ]
