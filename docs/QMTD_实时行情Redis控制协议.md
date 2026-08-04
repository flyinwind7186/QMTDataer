# QMTD 实时行情 Redis 控制协议

## 1. 协议定位

本文定义策略端或 BTLive 端与 QMTD 实时行情服务之间的 Redis 控制协议。

该协议只负责实时行情需求声明与结果确认，不负责：

- 启动或停止 QMTD 服务进程。
- 管理统一控制台 HTTP bridge。
- 发起历史行情下载或 FD 数据库入库任务。
- 管理策略生命周期。

## 1.1 唯一行情器规则

同一套 Redis 行情 topic 在同一运行环境内只能有一个权威 QMTD 实时行情器。

规则如下：

- 真行情和 Mock 行情由 QMTD 启动入口决定，不由下游 `subscribe` 请求决定。
- 下游 BTLive / 策略端只能声明 `codes`、`periods`、`mode` 与 `topic`，不能在订阅时要求“真行情”或“假行情”。
- `run_realtime_control.py` 是真实行情空白控制入口，会连接 MiniQMT / xtdata。
- `run_realtime_mock_control.py` 是虚拟行情空白控制入口，会强制启用 Mock 行情。
- 两个入口不能同时作为同一个 `xt:topic:bar` 的权威发布源运行。
- 如果确需同时运行真/假两套服务，必须显式配置不同 Redis topic 和不同控制通道，并让下游明确订阅对应 topic。

违反该规则会导致同一 `(code, period, bar_end_ts)` 可能被两个来源同时发布，下游无法可靠判断哪一条是权威行情。

## 2. 通道约定

默认通道如下：

- 控制通道：`xt:ctrl:sub`
- ACK 前缀：`xt:ctrl:ack`
- 策略 ACK 通道：`xt:ctrl:ack:<strategy_id>`
- 行情推送 topic：`xt:topic:bar`
- 订阅注册表前缀：`xt:bridge`

部署时可以通过 QMTD 配置文件覆盖这些值，但同一套运行环境内应保持策略端与 QMTD 配置一致。

## 3. 通用字段

所有控制命令均使用 JSON 字符串发布到控制通道。

### 3.1 请求通用字段

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `action` | string | 是 | 命令类型：`subscribe`、`unsubscribe`、`status` |
| `strategy_id` | string | 是 | 策略或调用方标识，用于 ACK 通道和订阅归属 |

### 3.2 ACK 通用字段

QMTD 处理命令后，将 ACK JSON 发布到：

```text
<ack_prefix>:<strategy_id>
```

ACK 通用字段：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `ok` | bool | 是 | 命令是否处理成功 |
| `action` | string | 否 | 成功时通常返回对应命令类型 |
| `error` | string | 否 | 失败原因 |
| `instance_id` | string | 是 | 当前 QMTD 实时行情进程 UUID |
| `instance_started_at_ms` | int | 是 | 当前实时进程启动时间，epoch 毫秒 |
| `session_generation` | int | 是 | 当前 xtdata 会话代次，初始为 1 |
| `protocol_version` | int | 是 | 当前协议版本，第一版会话恢复固定为 2 |

这里的 `instance_id` 只代表 QMTD 实时行情进程，不复用 HTTP Bridge 的实例身份。

## 4. subscribe

### 4.1 请求

```json
{
  "action": "subscribe",
  "strategy_id": "ma_cross_demo",
  "codes": ["510050.SH", "518880.SH"],
  "periods": ["1m"],
  "mode": "close_only",
  "preload_days": 0,
  "topic": "xt:topic:bar"
}
```

字段说明：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `codes` | list[string] | 是 | 订阅标的列表 |
| `periods` | list[string] | 是 | 订阅周期列表，例如 `1m`、`1d` |
| `mode` | string | 否 | 推送模式，默认取 QMTD 服务配置 |
| `preload_days` | int | 否 | 订阅前历史预热天数，建议策略运行时传 `0` |
| `topic` | string | 否 | 行情推送 topic，默认取 QMTD 服务配置 |

### 4.2 成功 ACK

```json
{
  "ok": true,
  "action": "subscribe",
  "sub_id": "sub-20260423-103000-xxxxxxxx",
  "codes": ["510050.SH", "518880.SH"],
  "periods": ["1m"],
  "mode": "close_only",
  "topic": "xt:topic:bar",
  "instance_id": "realtime-process-uuid",
  "instance_started_at_ms": 1785800000000,
  "session_generation": 1,
  "protocol_version": 2
}
```

`sub_id` 是本次订阅请求的唯一标识，后续退订应优先使用该字段。

### 4.3 失败 ACK

```json
{
  "ok": false,
  "error": "codes/periods required"
}
```

常见失败原因：

- `strategy_id` 为空。
- `strategy_id` 不在允许列表中。
- `codes` 或 `periods` 为空。
- QMTD 底层订阅失败。
- QMTD 处于 `STARTING`、`RECONNECTING`、`STOPPING` 或 `ERROR`，不能安全修改订阅。

## 5. unsubscribe

### 5.1 按 sub_id 退订

推荐方式：

```json
{
  "action": "unsubscribe",
  "strategy_id": "ma_cross_demo",
  "sub_id": "sub-20260423-103000-xxxxxxxx"
}
```

这里的 `sub_id` 是 QMTD ControlPlane / Registry 生成并通过 subscribe ACK 返回的协议订阅 ID。
它不是 xtdata 底层订阅 ID。

### 5.2 按 codes 与 periods 退订

兼容方式：

```json
{
  "action": "unsubscribe",
  "strategy_id": "ma_cross_demo",
  "codes": ["510050.SH"],
  "periods": ["1m"]
}
```

按 `codes × periods` 退订时，调用方必须确保退订口径与订阅口径一致。

### 5.3 成功 ACK

```json
{
  "ok": true,
  "action": "unsubscribe",
  "codes": ["510050.SH"],
  "periods": ["1m"]
}
```

### 5.4 失败 ACK

```json
{
  "ok": false,
  "error": "sub_id not found"
}
```

常见失败原因：

- `sub_id` 不存在。
- 未提供 `sub_id`，且 `codes` 或 `periods` 为空。
- QMTD 底层退订失败。

### 5.5 退订内部实现说明

当前退订链路如下：

1. 策略端按协议 `sub_id` 发送 `unsubscribe`。
2. ControlPlane 从 Registry 读取该 `sub_id` 对应的 `codes` 与 `periods`。
3. RealtimeSubscriptionService 按 `(code, period)` 减少引用计数。
4. 只有引用计数归零时，QMTD 才取消底层 xtdata 订阅。
5. 当前 MiniQMT / xtdata 环境中，底层 `unsubscribe_quote()` 需要使用 `subscribe_quote()` 返回的内部订阅 ID。

因此有两个不同的 ID：

- 协议 `sub_id`：暴露给策略端 / BTLive 端，用于向 QMTD 退订。
- xtdata 内部订阅 ID：QMTD 内部保存，用于真正调用 `xtdata.unsubscribe_quote()`。

策略端不需要、也不应该感知 xtdata 内部订阅 ID。

## 6. status

### 6.1 请求

```json
{
  "action": "status",
  "strategy_id": "ma_cross_demo"
}
```

### 6.2 当前 ACK

```json
{
  "ok": true,
  "action": "status",
  "status": {
    "service_state": "READY",
    "desired_streams": [
      {"code": "510050.SH", "period": "1m"}
    ],
    "active_streams": [
      {"code": "510050.SH", "period": "1m"}
    ],
    "subs": [
      {"code": "510050.SH", "period": "1m", "ref_count": 2}
    ],
    "last_published": {
      "510050.SH|1m": 1770000000.0
    },
    "reconnect_count": 0,
    "last_error": null,
    "instance_id": "realtime-process-uuid",
    "instance_started_at_ms": 1785800000000,
    "session_generation": 1,
    "protocol_version": 2
  },
  "subs": ["sub-20260423-103000-xxxxxxxx"],
  "instance_id": "realtime-process-uuid",
  "instance_started_at_ms": 1785800000000,
  "session_generation": 1,
  "protocol_version": 2
}
```

当前 `subs` 字段包含注册表里的 `sub_id` 列表；`status.subs` 包含实时服务当前活跃行情流。
`ref_count` 表示该 `(code, period)` 当前被多少次订阅引用。

注意：

- 判断当前 QMTD 是否仍在推某个底层行情流，应以 `status.status.subs` 为准。
- 顶层 `subs` 来自 Registry，表示注册表中保存的协议 `sub_id` 列表。
- 新实时进程启动时会清理旧进程 Registry，不自动加载或恢复旧 `sub_id`。
- 策略端退订后，建议再发送一次 `status`，确认目标 `(code, period)` 已从 `status.status.subs` 中消失。

### 6.3 desired_streams 与 active_streams

- `desired_streams`：当前协议引用计数要求 QMTD 保持的行情流。
- `active_streams`：当前 xtdata 会话中已成功取得底层订阅 ID 的行情流。
- `READY` 状态下两者必须一致。
- `RECONNECTING` 状态下保留 `desired_streams`，`active_streams` 暂时为空。

## 7. 幂等与重复命令规则

### 7.1 subscribe

当前实现会为每次成功订阅生成新的 `sub_id`。因此同一策略重复发送相同 `codes` 与 `periods`：

- 会产生多个订阅记录。
- 后续需要分别退订，或由后续引用计数机制统一处理。

当前服务端底层行情流已按 `(code, period)` 做引用计数。同一行情流被多次引用时，只向 xtdata
注册一次底层订阅。

### 7.2 unsubscribe

推荐按 `sub_id` 退订，因为它能精确对应一次订阅请求。

当前引用计数规则：

- 每次退订只减少对应 `sub_id` 展开的 `(code, period)` 引用。
- 只有某个 `(code, period)` 的引用计数降为 `0` 时，才真正取消底层 xtdata 订阅。
- 如果策略漏退订，最坏情况是多保留一个行情流，不应误关其他策略仍在使用的行情。

### 7.3 status

`status` 是只读命令，不改变订阅状态。

## 8. 错误处理原则

QMTD 应尽量返回结构化 ACK，而不是让调用方只能依赖日志。

错误 ACK 最低要求：

```json
{
  "ok": false,
  "error": "human readable error"
}
```

后续可扩展为：

```json
{
  "ok": false,
  "error": "codes/periods required",
  "error_code": "BAD_REQUEST",
  "detail": {
    "action": "subscribe"
  }
}
```

第一阶段先保持兼容当前实现，不强制引入 `error_code`。

### 8.1 MiniQMT 会话有限恢复

真实行情进程使用以下最小状态：

```text
STARTING -> READY -> RECONNECTING -> READY
STARTING -> ERROR
RECONNECTING -> ERROR
READY/RECONNECTING -> STOPPING
```

`xtdata.run()` 非预期结束后，QMTD 从检测时点开始执行 60 秒软期限恢复：

- 每次 `reconnect()` 和 `subscribe_quote()` 前后检查剩余时间。
- 到达期限后不再发起新尝试；同步调用超期返回后立即判定失败。
- 恢复时只重建当前 `desired_streams`，不补行情、不追平、不回放。
- 全部底层订阅成功后才递增 `session_generation`、`reconnect_count` 并恢复 `READY`。
- 恢复期间 `status` 可用，`subscribe/unsubscribe` 返回 `service_reconnecting`。
- 恢复失败时进入 `ERROR`，QMTD 真实行情进程以非零状态退出。
- 同进程恢复保持 `instance_id`；进程重启必须生成新 `instance_id`。

不得根据午休、收盘、周末、节假日、停牌或长时间没有 bar 触发重连。

### 8.2 固定健康键

真实行情入口默认每 5 秒使用 Redis `SET` 刷新：

```text
xt:bridge:health:current
```

TTL 默认 20 秒，payload 至少包含 `instance_id`、`session_generation`、`service_state`、
`reconnect_count`、`desired_streams`、`active_streams`、`last_published` 和 `updated_at_ms`。
退出时仅当固定键仍属于自身 `instance_id` 才删除，避免旧进程误删新实例健康信息。

## 9. 调用方建议

策略端或 BTLive 端应遵守：

- 启动策略前发送 `subscribe`。
- 必须监听 `xt:ctrl:ack:<strategy_id>` 并确认 `ok=true`。
- 保存 `sub_id`。
- 策略停止时优先按 `sub_id` 发送 `unsubscribe`。
- 不通过本协议启动、停止或配置 QMTD 服务。
- 不把 `status` 当作服务治理接口，只用于诊断或订阅确认。

## 10. 现有示例

当前可参考：

- `tests/flow_demo_subscribe_and_listen.py`
- `scripts/send_control_cmd.py`

后续建议新增更贴近策略端的示例客户端，把订阅、等待 ACK、退订封装为可复用函数。

## 11. 真实联调记录

最近一次真实联调已覆盖以下链路：

```text
QMTD 空白启动
-> Redis 控制通道发送 subscribe
-> 收到 subscribe ACK
-> Redis 行情 topic 收到 1m close-only bar
-> 自动发送 unsubscribe
-> status.status.subs 返回空数组
```

测试环境：

- MiniQMT：已启动并可通过 xtdata 获取实时数据。
- Redis：`redis://127.0.0.1:6379/0`
- QMTD 启动入口：`scripts/run_realtime_control.py`
- 控制通道：`xt:ctrl:sub`
- ACK 前缀：`xt:ctrl:ack`
- 行情 topic：`xt:topic:bar`
- 测试标的：`510050.SH`、`510900.SH`
- 测试周期：`1m`

收到的示例行情：

```text
[BAR] 510900.SH 1m 2026-04-23T14:50:00 close=1.082 is_closed=True
[BAR] 510050.SH 1m 2026-04-23T14:50:00 close=3.002... is_closed=True
```

退订后 status 示例：

```json
{
  "ok": true,
  "action": "status",
  "status": {
    "subs": [],
    "last_published": {
      "510900.SH|1m": 1776927121.201879,
      "510050.SH|1m": 1776927122.2272217
    }
  },
  "subs": ["sub-历史遗留示例"]
}
```

上例是旧协议联调记录。协议版本 2 启动时会清理旧进程 Registry，因此新进程不会继续展示历史遗留 `sub_id`。
