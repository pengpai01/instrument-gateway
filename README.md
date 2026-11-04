# instrument-gateway

仪器设备网关（第一迭代）：**HTTP / TCP / MQTT 入站 → 鉴权 → 归一 `InboundEvent` → 写 Kafka**。

- **不做**：下行控制、业务库、多租户、原始报文落盘、死信库
- **单模块** Maven / Spring Boot 3.3+ / JDK 17

## 依赖

| 组件 | 说明 |
|------|------|
| **JDK 17+** | 必须 |
| **Maven 3.9+** | 必须 |
| **Kafka** | 必须。topic `instr.event.v1`。box / 默认 `localhost:9092`；笔记本 profile `laptop` → `192.168.11.114:9092` |
| **MQTT broker** | **可选**。不配 `gateway.mqtt.broker-url` 时进程仍可启动，HTTP/TCP 可用 |

### 可选：Docker Compose

仓库内 `docker-compose.yml` 可起 **Kafka(+Zookeeper) + Mosquitto**，**不是硬依赖**。你已有集群时直接改配置即可。

```bash
docker compose up -d
# 停：docker compose down
```

## 配置与密钥

- 全局一把 API Key：环境变量 **`GATEWAY_API_KEY`**（优先），或 `gateway.api-key`
- 本地覆盖：复制 `src/main/resources/application-local.example.yml` → `application-local.yml`（已 gitignore）
- **禁止**把生产密钥提交进库

```bash
export GATEWAY_API_KEY='change-me-local-only'
# 可选 MQTT
export GATEWAY_MQTT_BROKER_URL='tcp://localhost:1883'
```

默认端口：

- HTTP `:8080` — `POST /api/v1/ingest`，`GET /actuator/health`
- TCP `:9100`
- Kafka topic `instr.event.v1`，key=`sourceId`（见下文「身份」），acks=all + idempotence

### Kafka bootstrap：box 与笔记本（二选一）

不要同时打开 `local` 和 `laptop`。两个 profile 都写死了 bootstrap，后写的会盖住先写的。未加 profile 时走 `application.yml` 默认，可用环境变量 `KAFKA_BOOTSTRAP_SERVERS` 覆盖；**profile 里的地址不再读这个变量**。

| 场景 | 一条命令 | bootstrap |
|------|----------|-----------|
| **box 联调**（本仓库 `docker compose`） | `java -jar target/instrument-gateway-0.1.0-SNAPSHOT.jar --spring.profiles.active=local` | `localhost:9092` |
| **box 默认**（不复制 local yml 也行） | `java -jar target/instrument-gateway-0.1.0-SNAPSHOT.jar` | `localhost:9092`（或 `KAFKA_BOOTSTRAP_SERVERS`） |
| **Windows 笔记本** → 内网 Kafka | `java -jar target\instrument-gateway-0.1.0-SNAPSHOT.jar --spring.profiles.active=laptop` | `192.168.11.114:9092` |

- box：`src/main/resources/application-local.example.yml` → 复制成 gitignore 的 `application-local.yml`，bootstrap 保持 `localhost:9092`
- 笔记本：直接用已提交的 `src/main/resources/application-laptop.yml`（打进 jar）。API Key 仍是占位 `change-me-local-only`，或环境变量 `GATEWAY_API_KEY`。该 profile **不配置 MQTT**

`mvn` 等价写法：`-Dspring-boot.run.profiles=local` 或 `-Dspring-boot.run.profiles=laptop`。

## 获取代码

```bash
git clone https://github.com/pengpai01/instrument-gateway.git E:\grok_bot\instrument-gateway
cd /d E:\grok_bot\instrument-gateway
```

## 启动（Windows 本机）

先设 JDK 17（按你本机安装路径调整）：

PowerShell:

```powershell
$env:JAVA_HOME='E:\jdk-17'
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
$env:GATEWAY_API_KEY='change-me-local-only'
# 可选 MQTT（不设则 MQTT 降级，HTTP/TCP 仍可用）
# $env:GATEWAY_MQTT_BROKER_URL='tcp://localhost:1883'
cd E:\grok_bot\instrument-gateway
mvn -q spring-boot:run
```

CMD:

```bat
set JAVA_HOME=E:\jdk-17
set PATH=%JAVA_HOME%\bin;%PATH%
set GATEWAY_API_KEY=change-me-local-only
cd /d E:\grok_bot\instrument-gateway
mvn -q spring-boot:run
```

或打包后运行。连笔记本内网 Kafka（`192.168.11.114:9092`）加 profile `laptop`：

```bat
mvn -q -DskipTests package
java -jar target\instrument-gateway-0.1.0-SNAPSHOT.jar --spring.profiles.active=laptop
```

PowerShell 用 `mvn` 直跑同一 profile：

```powershell
mvn -q spring-boot:run "-Dspring-boot.run.profiles=laptop"
```

box 上不要用 `laptop`。本机若只想连 docker compose 的 `localhost:9092`，用 `local`（或什么 profile 都不加）：

```bat
copy src\main\resources\application-local.example.yml src\main\resources\application-local.yml
mvn -q spring-boot:run -Dspring-boot.run.profiles=local
```

Linux / macOS / box：`export JAVA_HOME=...` 与 `export GATEWAY_API_KEY=...` 后 `mvn -q spring-boot:run`（默认 `localhost:9092`）。联调 profile：`--spring.profiles.active=local`。

## 身份：`sourceId` 必有，`deviceId` 可选

不是每台仪器都知道自己是谁：有的固件不存序列号，有的设备名写在无法配置的界面里，有的干脆没有配置项。所以**网关不再要求报文带 `deviceId`**，而是自己派生一个必有的 `sourceId`：

| 优先级 | 来源 | 谁提供 | 例子 |
|---|---|---|---|
| 1 | 报文 `sourceId` | 客户端 | `"sourceId":"ward-3-analyzer"` |
| 2 | HTTP 头 `X-Client-Id` | 客户端 | `-H 'X-Client-Id: ward-3-analyzer'` |
| 3 | 凭证绑定 `gateway.clients` | **运维**（推荐） | `krd-lab-key-01: krd-lab-01` |
| 4 | MQTT topic 第二段 | broker 路由 | `instr/krd-lab-01/up` |
| 5 | 报文 `deviceId` | 客户端 | `"deviceId":"100C200017"` |
| 6 | TCP / HTTP 对端 IP | 网络 | `10.2.3.4` |
| 7 | 以上全空 | — | `anonymous` |

- `sourceId` **必有**：它是 Kafka 的 key（同源数据落同一分区、保持顺序），也是日志里的 `sourceId=`
- `deviceId` **可选**：有就原样保留，网关不覆盖、不推断；下游做设备台账时用它
- 前 6 条都能被客户端或网络伪造，**不是安全边界**。认证仍然只靠 token，它们只决定“这条数据被标记成谁”
- 全部落空时 `sourceId=anonymous`：数据照常入库，只是所有匿名客户端共用一个 Kafka 分区键

### 推荐：一台仪器一个 key

仪器报不出 `deviceId` 时，唯一稳定且不用改固件的身份就是**它连网关用的那个 key**——安装时必然要配。把 key 绑到一个名字上即可：

```yaml
gateway:
  clients:
    krd-lab-key-01: krd-lab-01
    dnk-screen-6-key: dnk-screen-6
```

命中 `clients` 的 key 本身就能通过认证（此时 `gateway.api-key` 可以留空）。key 里不要带 `.`（Spring 会当路径分隔符）。

## 幂等：`idempotencyKey` 由网关透传，去重在下游

TCP 没有 ACK，客户端超时重发是常态，所以每条记录都应当带一个**稳定的** `idempotencyKey`：同一份业务数据重发时，这个字符串必须逐字节相同。

```json
{"type":"REPORT","token":"<KEY>","idempotencyKey":"100C200017-20261103232734","payload":{ ... }}
```

推荐构造 `{稳定标识}-{yyyyMMddHHmmss}`：

| 类型 | 稳定标识 | 例子 |
|---|---|---|
| `REPORT` | `payload.serialsId` | `100C200017-20261103232734` |
| `SAMPLE` | `payload.iccid`（ASCII，不随固件语言变） | `89860625790032599518-20261104112129` |

网关负责三件事：

- **校验**：长度 ≤ 200 字符，首尾空白 trim，空串等同于没传
- **透传**：写进 Kafka 消息体，**同时**写进 Kafka header `idempotencyKey`
- **不去重**

为什么网关不做内存去重：

| 做法 | 后果 |
|---|---|
| 收到就去重再写 Kafka | Kafka 写失败时那条数据**永久丢失**——比重复严重得多 |
| 等 Kafka ack 再去重 | 每条消息都阻塞在网络往返上，吞吐量会下降 |
| 任何一个内存集合 | 网关重启后清空、多实例各记各的，两头都不保证 |

去重放在**能回放的持久层**：消费者按 `idempotencyKey` 建唯一约束（Redis `SETNX` / 数据库唯一键 / Flink keyed state）。Kafka 里的消息是可重放的原始事实，网关只负责把它完整、有序地放进去。

header 的意义：流处理作业不必反序列化整个 body 就能拿到去重键；`kafka-console-consumer --property print.headers=true` 也能直接看到。

## HTTP 样例（五类）

鉴权：`Authorization: Bearer <GATEWAY_API_KEY>` 或头 `X-Api-Key`。无 Key → **401**。校验失败 → **400**，不写 Kafka。

`deviceId` 不是必填项。仪器拿不到时，用 `X-Client-Id` 头或 `gateway.clients` 里的 key 绑定给出身份，见上文「身份」。

```bash
KEY="${GATEWAY_API_KEY:-change-me-local-only}"

# DETECT
curl -sS -X POST http://localhost:8080/api/v1/ingest \
  -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"deviceId":"dev-001","type":"DETECT","occurredAt":"2026-11-03T09:00:00Z","payload":{"itemCode":"TEMP","value":36.5,"unit":"C"}}'

# POWER
curl -sS -X POST http://localhost:8080/api/v1/ingest \
  -H "X-Api-Key: $KEY" -H 'Content-Type: application/json' \
  -d '{"deviceId":"dev-001","type":"POWER","payload":{"state":"ON","reason":"boot"}}'

# ALARM
curl -sS -X POST http://localhost:8080/api/v1/ingest \
  -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"deviceId":"dev-001","type":"ALARM","payload":{"level":"WARN","code":"E01","message":"overheat","cleared":false}}'

# REPORT（仪器报告：表头 + result 明细数组）
curl -sS -X POST http://localhost:8080/api/v1/ingest \
  -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"deviceId":"100C200017","type":"REPORT","occurredAt":"2026-11-03T23:27:34Z","siteId":"krd","payload":{"orgCode":"krd","checkTime":"2026-11-03 23:27:34","result":[{"num":"1","projectName":"GM试验","concentration":"0.050"}]}}'

# SAMPLE（单条样本结果：表头 + content 对象）
curl -sS -X POST http://localhost:8080/api/v1/ingest \
  -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"deviceId":"screen-6","type":"SAMPLE","occurredAt":"2026-11-04T11:21:29Z","siteId":"dnk","payload":{"orgCode":"dnk","iccid":"89860625790032599518","content":{"concentration":"-1.000","temperature":"36.000"}}}'

# 不带 deviceId：身份来自 X-Client-Id 头
curl -sS -X POST http://localhost:8080/api/v1/ingest \
  -H "Authorization: Bearer $KEY" -H 'X-Client-Id: ward-3-analyzer' \
  -H 'Content-Type: application/json' \
  -d '{"type":"DETECT","payload":{"itemCode":"TEMP","value":36.5,"unit":"C"}}'

# 无 Key → 401
curl -sS -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8080/api/v1/ingest \
  -H 'Content-Type: application/json' -d '{"deviceId":"dev-001","type":"POWER","payload":{"state":"OFF"}}'
```

## TCP（默认帧，可替换）

**默认实现**（策略接口 `TcpFrameDecoder` / `LengthPrefixTcpFrameDecoder`）：

- 监听 `0.0.0.0:9100`
- 帧 = **2 字节大端 unsigned length** + **UTF-8 JSON**
- **整帧（含 2 字节头）必须 &lt; 1024**；超限丢弃 + 日志，不写 Kafka
- **一条记录可以拆成多帧**（大记录分片，见下文）；单帧内的 JSON 也仍可按“一帧一记录”发送
- JSON 内含 `token`（与全局 Key 同值）；验完**剥离**，不进 Kafka
- `deviceId` 非必填；省略时身份依次取报文 `sourceId`、key 绑定的 clientId、对端 IP（见上文「身份」）
- 坏 JSON / 校验失败 → 丢弃 + 日志

> **默认长度前缀帧可替换**：厂商帧格式不同时实现/替换 `TcpFrameDecoder`（或换 pipeline factory）即可，不必改 ingest 语义。勿把当前帧格式当成唯一协议。

用 Python 发一帧 DETECT：

```bash
python3 - <<'PY'
import socket, json, struct, os
key = os.environ.get("GATEWAY_API_KEY", "change-me-local-only")
body = {
  "deviceId": "dev-tcp-1",
  "type": "DETECT",
  "token": key,
  "payload": {"itemCode": "PH", "value": 7.1, "unit": "pH"}
}
payload = json.dumps(body).encode("utf-8")
frame = struct.pack(">H", len(payload)) + payload
assert len(frame) < 1024
s = socket.create_connection(("127.0.0.1", 9100))
s.sendall(frame)
s.close()
print("sent", len(frame), "bytes")
PY
```

POWER / ALARM / REPORT / SAMPLE 同理，改 `type` 与 `payload`（POWER: `state` ON|OFF；ALARM: `level`/`code`/`message`；REPORT: `result` 必填且必须是对象数组；SAMPLE: `content` 必填且必须是对象）。

### 大记录分片（可选，旧客户端不受影响）

一条记录大于一帧（payload 上限 1021 字节）时，客户端负责切片，服务端负责拼回。每帧是一个“信封”：

| 字段 | 说明 |
|------|------|
| `msgId` | 同一条记录的标识，所有分片相同；**固定 16 位小写十六进制**（客户端约定，服务端不强制） |
| `seq` | 第几片，**从 1 开始**，最多 3 位（上限 128 片） |
| `total` | 总片数，最多 3 位；`seq == total` 即最后一片 |
| `data` | 该片原文（**转义后的 JSON 文本，不是 base64**） |

```json
{"msgId":"000001c9a3f4b7d2","seq":1,"total":3,"data":"{\"type\":\"DETECT\",\"token\":\"change-me"}
```

信封尺寸是**算得出来**的，不是估算：`msgId` 定长 16、`seq`/`total` 各最多 3 位，所以最宽信封 = `{"msgId":"…","seq":999,"total":999,"data":""}` = **60 字节**，一片 `data` 最多带 **961 字节**转义文本（1021 − 60）。要算缓冲区、定分包上限、写文档，都用这两个数，不用再根据片号、总片数、`msgId` 长短去猜。

- `data` 放的是原文本身，只做 JSON 字符串转义（`"` → `\"`、换行 → `\n`，控制字符 → `\u0000`）：线上全程是文本，抓包/日志直接可读，也不再多付 base64 的 33% 膨胀；代价是引号密度高的报文会多花约 10–25%
- **切片必须落在 UTF-8 字符边界上**：从中间切断会把一个多字节字符拆成两片，拼回来的记录不再是合法 UTF-8。切点落在续接字节（`10xxxxxx`）上时向后挪到下一个字符起始
- **单片大小是目标值，不是硬要求**：每片切多长由“转义后能不能装进 961 字节”决定。引号密、控制字符多时切片会自动变短、片数变多，而不是报错拒发——必须守住的是帧上限，片大小只是达到它的手段
- 服务端按连接缓存，**按 `seq` 索引，乱序到达也能还原**；收齐 `total` 片才解析、鉴权、写 Kafka → 一条记录只产生一个 `eventId`
- 同一 `msgId` 的所有分片必须走**同一条 TCP 连接**；断连时未完成记录丢弃并记 warn
- `token` 放在**原始记录**里（拼好后校验），不需要每片都带
- 不带 `msgId`/`seq`/`total` 的帧仍按“一帧一记录”解析（向后兼容）
- 上限（`application.yml` 的 `gateway.tcp.assembly.*`）：`max-bytes` 64KB / `max-fragments` 128 / `timeout-ms` 10s / `max-pending-per-channel` 8
- 容量：一帧 1021 字节，定长信封最宽 60 字节，转义一般膨胀 1.1–1.25 倍 → 默认目标 **740 字节原文**（实际每片会按转义结果自动微调，但不超 961）；128 片 × 740 字节 ≈ 92KB，足以覆盖 64KB 的记录上限（`TcpFrameCapacityTest` 会守护这两个值不自相矛盾）

Python 分片发送示例：

```python
import socket, json, struct, os, uuid

key = os.environ.get("GATEWAY_API_KEY", "change-me-local-only")
body = {"type": "DETECT", "token": key,
        "payload": {"itemCode": "PH", "value": 7.1, "unit": "pH"}}
text = json.dumps(body, ensure_ascii=False)
raw = text.encode("utf-8")
msg_id = uuid.uuid4().hex[:16]   # 定长 16 位：信封尺寸才是常数
chunk = 740

def slices(s, size):
    """按 UTF-8 字符边界切片：多字节字符绝不跨片。"""
    out, buf, used = [], [], 0
    for ch in s:
        n = len(ch.encode("utf-8"))
        if used + n > size:
            out.append("".join(buf))
            buf, used = [], 0
        buf.append(ch)
        used += n
    if buf:
        out.append("".join(buf))
    return out

parts = slices(text, chunk)

total = len(parts)
s = socket.create_connection(("127.0.0.1", 9100))
for i, part in enumerate(parts, 1):
    env = json.dumps({"msgId": msg_id, "seq": i, "total": total, "data": part},
                     ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    frame = struct.pack(">H", len(env)) + env
    assert len(frame) < 1024, len(frame)   # json.dumps 已替我们完成转义
    s.sendall(frame)
s.close()
print("sent", total, "fragments", len(raw), "bytes")
```

### REPORT 大报告怎么组织（真实样本）

检验仪器（如 GM 试验）一份报告常有几十上百条明细：实测 77 条 = **12,086 字节 ≈ 17 片**。推荐外层只加网关字段，报告本体原样放进 `payload`：

```json
{
  "deviceId": "100C200017",
  "type": "REPORT",
  "token": "<GATEWAY_API_KEY>",
  "occurredAt": "2026-11-03T23:27:34Z",
  "siteId": "krd",
  "idempotencyKey": "100C200017-20261103232734",
  "payload": {"orgCode":"krd","checkTime":"2026-11-03 23:27:34","result":[ ... 77 条明细 ... ]}
}
```

| 项 | 说明 |
|---|---|
| `type` | 固定 `REPORT`；`payload.result` 必填，且必须是对象数组（明细内部字段不做强制，因仪器而异） |
| `occurredAt` | 由 `checkTime` 转换（空格 → `T`，补 `Z`）；`payload.checkTime` 原样保留便于追溯 |
| `siteId` / `idempotencyKey` | 取 `orgCode` / `serialsId-checkTime`，重发可去重 |
| 切片 | 740 字节/片 → 77 条约 **17 片**；`max-bytes` 64KB 约支持 420 条明细 |
| 回归样本 | `src/test/resources/tcp/sample-report-gm.json`（真实 GM 报告，`TcpInboundHandlerTest` 与 `IngestServiceTest` 均引用） |

### SAMPLE 单条结果（真实样本）

屏幕测试类仪器每次只上报一个样本，形态是“表头 + `content` 对象”，没有 `result` 数组：

```json
{
  "deviceId": "screen-6",
  "type": "SAMPLE",
  "token": "<GATEWAY_API_KEY>",
  "occurredAt": "2026-11-04T11:21:29Z",
  "siteId": "dnk",
  "idempotencyKey": "89860625790032599518-20261104112129",
  "payload": {"orgCode":"dnk","sdkVersion":"0.0.1","iccid":"89860625790032599518","content":{ "concentration":"-1.000", "temperature":"36.000", "sampleType":"其他" }}
}
```

| 项 | 说明 |
|---|---|
| `type` | 固定 `SAMPLE`；`payload.content` 必填且必须是对象（`concentration` / `result` / `temperature` 等测量值字段不强制，因仪器而异） |
| 体积 | 原始 521 字节，加外层信封 **701 字节** → 单帧（≤ 1021）直发，**不需要分片**；余量仅 **320 字节** |
| `idempotencyKey` | 用 `iccid-checkTime`，不用 `content.deviceId`——后者可能是中文（如 `屏幕测试6`），会影响日志检索与下游键值处理 |
| `deviceId` | 可选，取 `content.deviceId` 也行、留空也行；真要长期关联设备，建议换成 ASCII 稳定标识（如 `iccid` 或仪器序列号），中文值会影响日志检索与下游键值处理 |
| 连续上报 | 同一 TCP 连接可连续发多条 SAMPLE，每条独立成帧、各自入库；余量不足时客户端必须改走分片（`data` 字段） |
| 回归样本 | `src/test/resources/tcp/sample-result-screen.json`（`IngestServiceTest` 与 `TcpInboundHandlerTest` 均引用） |

### 用真实样本模拟发送（可执行工具）

`src/test/java/com/danah/instrument/gateway/tools/TcpSampleSender.java` 把上面两份真实样本按 TCP 协议发到运行中的网关：零依赖，JDK 17+ 在项目根目录直接跑。放在测试源码目录是有意的——`mvn test-compile` 会编译它（不会随时间腐烂），但不会打进 jar。

```bash
# 只看切帧结果，不连服务（单文件源码模式，无需构建）
java src/test/java/com/danah/instrument/gateway/tools/TcpSampleSender.java --dry-run

# 真实发送（默认 127.0.0.1:9100，token 默认 change-me-local-only）
java src/test/java/com/danah/instrument/gateway/tools/TcpSampleSender.java --token change-me-local-only

# 或者用编译产物（mvn test-compile 之后）
java -cp target/test-classes com.danah.instrument.gateway.tools.TcpSampleSender --dry-run
```

实测输出：GM 报告 `12,276 B → 17 片`（最大整帧 978 B，最后一片 599 B）；单条结果 `635 B → 1 帧`（637 B）；合计 18 帧 / 16,842 字节线上流量。

报文里**只带 `type` / `token` / `idempotencyKey` / `payload` 四个字段**：`deviceId` 不是必填，`occurredAt` 缺省时取接收时间，`siteId` 由下游按 `payload.orgCode` 补。`type` 和 `idempotencyKey` 是唯一不能省的两个——前者决定校验规则，后者决定下游能不能去重。

服务端每个记录只打印一行 `ingested eventId=... sourceId=... deviceId=... type=REPORT|SAMPLE protocol=TCP`，随后异步写入 `instr.event.v1`（DEBUG 可看到 `Kafka sent ... offset=...`）。**该协议没有 ACK**，发送成功与否只能看服务端日志与 Kafka，客户端不能靠 socket 返回值判断。

| 参数 | 默认 | 说明 |
|---|---|---|
| `--host` / `--port` | `127.0.0.1` / `9100` | 也可用 `GATEWAY_TCP_HOST`、`GATEWAY_TCP_PORT` |
| `--token` | `change-me-local-only` | 对应 `gateway.api-key`，也可用 `GATEWAY_API_KEY` |
| `--chunk` | `740` | 每片的目标原文字节数；转义后装不进一帧时会自动切短，不会报错 |
| `--data-dir` | `src/test/resources/tcp` | 样本目录 |

切帧逻辑由 `TcpSampleSenderTest` 守护：`msgId` 定长 16 位十六进制、信封 60 B + 数据 961 B = 一帧 1021 B 的恒等式、单帧/分片边界、`seq`/`total` 1 基、转义文本拼接还原（含 UTF-8 跨片用例）、`--chunk` 超限自动切短而不是报错（引号密、控制字符多两种情形）、`data` 必须是原文而非 base64、信封必带 `idempotencyKey` 且不得出现冗余业务字段、重发两次字节完全一致，以及上面两个实测数字（12,276 B → 17 片、635 B → 1 帧）。

## MQTT 样例

配置 `gateway.mqtt.broker-url`（或 `GATEWAY_MQTT_BROKER_URL`）后订阅 `instr/+/up` QoS1，clientId=`gw-{hostname}`。  
**topic 第二段作为 `sourceId`**（比报文里的任何自报值都可靠，且 body 无需带 `deviceId`）；body 里如果也写了 `deviceId`，原样保留、不覆盖也不报错。payload 内 `token` 验完剥离。

```bash
KEY="${GATEWAY_API_KEY:-change-me-local-only}"

mosquitto_pub -h localhost -t 'instr/dev-mqtt-1/up' -q 1 -m "{\"type\":\"DETECT\",\"token\":\"$KEY\",\"payload\":{\"itemCode\":\"TEMP\",\"value\":22.0}}"

mosquitto_pub -h localhost -t 'instr/dev-mqtt-1/up' -q 1 -m "{\"type\":\"POWER\",\"token\":\"$KEY\",\"payload\":{\"state\":\"OFF\"}}"

mosquitto_pub -h localhost -t 'instr/dev-mqtt-1/up' -q 1 -m "{\"type\":\"ALARM\",\"token\":\"$KEY\",\"payload\":{\"level\":\"CRITICAL\",\"code\":\"X9\",\"message\":\"fault\"}}"
```

## 消费 Kafka 验证

box / docker compose（`localhost:9092`）：

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic instr.event.v1 \
  --from-beginning --property print.key=true
```

或本机已装 CLI。笔记本 profile `laptop` 时把 bootstrap 换成 `192.168.11.114:9092`（topic 仍是 `instr.event.v1`）：

```bash
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic instr.event.v1 --from-beginning --property print.key=true

kafka-console-consumer --bootstrap-server 192.168.11.114:9092 \
  --topic instr.event.v1 --from-beginning --property print.key=true
```

归一后消息字段：`eventId`（网关生成）、`sourceId`（必有，Kafka key）、`deviceId`（可空）、`type`、`occurredAt`、`receivedAt`、`protocol`、`payload`，以及可选 `siteId` / `idempotencyKey` / `ext`。**不含** `token`。

`idempotencyKey` 除消息体外，还会重复写一份到 Kafka header `idempotencyKey`。想看 header：

```bash
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic instr.event.v1 --from-beginning \
  --property print.key=true --property print.headers=true
```

## Health

```bash
curl -sS http://localhost:8080/actuator/health
```

应含 **kafka**；MQTT 已配置且可探测时含 **mqtt**（未配置为 UNKNOWN/DISABLED，连不上为 DOWN，不阻止进程启动）。

## 模块结构（摘要）

```
com.danah.instrument.gateway
  GatewayApplication
  inbound/          IngestService + model + payloads + http + tcp + mqtt
  auth/             ApiKeyAuthFilter / ApiKeyAuthenticator
  kafka/            EventKafkaProducer
  config/           *Properties
```

## 仓库说明

- 远程：`https://github.com/pengpai01/instrument-gateway.git`
- **主开发**：box `/workspace/instrument-gateway`（改完 push）
- **本机查看**：`git clone` / `git pull` 到 `E:\grok_bot\instrument-gateway`；`JAVA_HOME=E:\jdk-17`
- 勿动本机 `E:\grok_bot\bak`
- Kafka / MQTT 均可配置；**Docker Compose 仅可选**
