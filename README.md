# 隔离网 NTP 服务

面向隔离网的 NTP 服务：对外提供标准 NTPv4 UDP 应答，网页可操作、可观测，并附带两个
可在网页调偏 / 停用 / 谎报 / 丢包的本地 NTP 上游（Docker Compose 同时启动）。

## 快速开始

```bash
docker compose up --build
```

- 服务网页：<http://localhost:8000>
- 上游 A 控制页：<http://localhost:8081>
- 上游 B 控制页：<http://localhost:8082>
- NTP 服务：`udp/123`（容器内同一进程同时跑 HTTP 与 UDP，共享同一份时钟状态）

本机不带 Docker 时，可直接运行：

```bash
python -m pip install -r requirements.txt
UPSTREAMS='[{"id":"a","host":"127.0.0.1","port":11123},{"id":"b","host":"127.0.0.1","port":11125}]' \
DATA_PATH=/tmp/history.json NTP_PORT=123 python run_service.py
```

## 功能

网页（`/`）展示：

- 当前对外时间、同步状态（`synced` / `holdover` / `holdover-expired` / `never-synced`）、
  stratum、LI、refid、根延迟/色散、当前来源、最近取样年龄、保鲜期剩余；
- 两路上游的启用状态、可达性、失败次数、层数、RTT，并可一键停用/启用；
- “发送真实 NTP 请求”：对任一上游或对本机服务真正发一个 UDP NTP 包并解析回复，
  如实显示有效性、offset、RTT 或错误原因；
- 事件日志（取样、回拨拒绝、钳制、切源、启动时忽略旧样本等）。

## 关键行为与实现

### 1. 应答归属与同步状态校验

`app/upstream.py` 只接受同时满足以下条件的回复：

- 用 `connect()` 的 UDP socket，内核层面只收该上游地址的报文；再显式校验源地址与端口；
- 请求的 transmit 字段是 64 位随机 nonce，回复的 origin 字段必须**逐字节**相等
  （重新编码浮点会丢精度，所以按原始位比较）；
- 必须是 mode 4、version 3/4 的 **server** 报文，stratum 在 1..15，LI≠3，
  root delay/dispersion 在合理范围，receive/transmit/reference 时间戳非零；
- RTT 合理（不为负、不超过上限）。

不匹配的报文会被忽略并继续等，直到超时；不会“收到包就信”。

### 2. 对外时间不倒退

`app/clock.py` 对外时间线是 `base_real + (monotonic - base_mono)`：

- 宿主墙钟被回拨不影响对外时间（只依赖 monotonic）；
- 候选时间比当前对外时间落后超过 `ROLLBACK_TOLERANCE`（默认 0.25s，另加 RTT/2）→
  **拒绝并记录回拨事件**，不切换来源；两路同时回拨时不会跟着倒退，而是冻结后过期；
- 落后但在容差内 → 接受样本但**钳住**对外时间（clamp 事件），不后退；
- 超前 → 前跳（大步会记录 step-forward 事件）；
- 每个应答用同一把锁下的同一份快照生成，时间字段和 LI/stratum 必然对应同一状态；
  另有 emit 下限保证同一进程内发出的 transmit 时间单调不减。

### 3. 失联保鲜期

- 最后一次**已验证**样本之后 `HOLDOVER_SECONDS`（默认 120s）内，按 monotonic 继续推进；
- 超过后对外应答 `stratum=16, LI=3`，时间冻结在保鲜期末，不冒充正常；
- `FRESH_SECONDS`（默认 10s）内视为 `synced`，之后到保鲜期末视为 `holdover`（仍可用，
  但网页明确标注“上游失联”）。

### 4. 重启后旧样本不生效

持久化的 `history.json` 仅作审计展示；`ClockState` 启动时只读取条数用于提示，
**绝不**用其中时间恢复时钟。重启后必须由上游新样本确认才重新同步。

## 测试与自测

```bash
python -m pip install -r requirements.txt -r requirements-dev.txt
python -m pytest tests/ -q          # 41 个单元/集成测试
python tests/selftest_scenarios.py  # 真实 UDP、真实线程的操作员视角自测
```

覆盖：协议编解码与拒绝路径、回拨/钳制/切源/前跳、保鲜期与过期、重启不复用旧样本、
并发“取样 vs 应答”一致性、真实 loopback UDP 端到端、Web API 与探测接口。

## 目录

```
app/ntp_proto.py     NTPv4 编解码（严格校验）
app/clock.py         时钟状态机、事件、审计历史
app/upstream.py      UDP 取样客户端（归属校验）
app/sampler.py       轮询、来源选择、失联处理
app/ntp_server.py    UDP 应答服务（单快照生成应答）
app/web.py           FastAPI 网页与探测接口
app/templates/       网页
fake_upstream/       可调偏/停用的本地上游（NTP + HTTP 控制页）
tests/               测试与自测脚本
```
