# instrument-gateway

仪器设备网关（第一迭代）：**HTTP / TCP / MQTT 入站 → 鉴权 → 归一 `InboundEvent` → 写 Kafka**。

- **不做**：下行控制、业务库、多租户、原始报文落盘、死信库
- **单模块** Maven / Spring Boot 3.3+ / JDK 17

## 依赖

| 组件 | 说明 |
|------|------|
| **JDK 17+** | 必须 |
| **Maven 3.9+** | 必须 |
| **Kafka** | 必须（默认 `localhost:9092`，topic `instr.event.v1`） |
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
- Kafka bootstrap `localhost:9092`，topic `instr.event.v1`，key=`deviceId`，acks=all + idempotence

## 启动

```bash
cd instrument-gateway
mvn -q spring-boot:run
# 或
mvn -q -DskipTests package && java -jar target/instrument-gateway-0.1.0-SNAPSHOT.jar
```

带 local profile：

```bash
cp src/main/resources/application-local.example.yml src/main/resources/application-local.yml
# 编辑 api-key / mqtt
mvn -q spring-boot:run -Dspring-boot.run.profiles=local
```

## HTTP 样例（三类）

鉴权：`Authorization: Bearer <GATEWAY_API_KEY>` 或头 `X-Api-Key`。无 Key → **401**。校验失败 → **400**，不写 Kafka。

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

# 无 Key → 401
curl -sS -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8080/api/v1/ingest \
  -H 'Content-Type: application/json' -d '{"deviceId":"dev-001","type":"POWER","payload":{"state":"OFF"}}'
```

## TCP（默认帧，可替换）

**默认实现**（策略接口 `TcpFrameDecoder` / `LengthPrefixTcpFrameDecoder`）：

- 监听 `0.0.0.0:9100`
- 帧 = **2 字节大端 unsigned length** + **UTF-8 JSON**
- **整帧（含 2 字节头）必须 &lt; 1024**；超限丢弃 + 日志，不写 Kafka
- JSON 内含 `token`（与全局 Key 同值）；验完**剥离**，不进 Kafka
- 坏 JSON / 校验失败 → 丢弃 + 日志

> 厂商帧格式不同时，实现/替换 `TcpFrameDecoder` 即可，不必改 ingest 语义。

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

POWER / ALARM 同理，改 `type` 与 `payload`（POWER: `state` ON|OFF；ALARM: `level`/`code`/`message`）。

## MQTT 样例

配置 `gateway.mqtt.broker-url`（或 `GATEWAY_MQTT_BROKER_URL`）后订阅 `instr/+/up` QoS1，clientId=`gw-{hostname}`。  
`deviceId` **优先取 topic 第二段**；与 body 不一致打 warn 并以 topic 为准。payload 内 `token` 验完剥离。

```bash
KEY="${GATEWAY_API_KEY:-change-me-local-only}"

mosquitto_pub -h localhost -t 'instr/dev-mqtt-1/up' -q 1 -m "{\"type\":\"DETECT\",\"token\":\"$KEY\",\"payload\":{\"itemCode\":\"TEMP\",\"value\":22.0}}"

mosquitto_pub -h localhost -t 'instr/dev-mqtt-1/up' -q 1 -m "{\"type\":\"POWER\",\"token\":\"$KEY\",\"payload\":{\"state\":\"OFF\"}}"

mosquitto_pub -h localhost -t 'instr/dev-mqtt-1/up' -q 1 -m "{\"type\":\"ALARM\",\"token\":\"$KEY\",\"payload\":{\"level\":\"CRITICAL\",\"code\":\"X9\",\"message\":\"fault\"}}"
```

## 消费 Kafka 验证

```bash
# 若使用 docker compose 中的 Kafka 镜像：
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic instr.event.v1 \
  --from-beginning --property print.key=true
```

或本机已装 CLI：

```bash
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic instr.event.v1 --from-beginning --property print.key=true
```

归一后消息字段：`eventId`（网关生成）、`deviceId`、`type`、`occurredAt`、`receivedAt`、`protocol`、`payload`，以及可选 `siteId` / `idempotencyKey` / `ext`。**不含** `token`。

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

## 从 box 拷到 Windows

本仓库在 Linux box 上路径：`/workspace/instrument-gateway`。拷到 `E:\grok_bot\instrument-gateway` 可用：

- Cursor / VS Code Remote 下载目录
- `scp -r ... /workspace/instrument-gateway user@windows-host:...`（按你环境）
- 或打包：`tar czf instrument-gateway.tgz -C /workspace instrument-gateway` 后传文件

到 Windows 后确保本机 **JDK 17 + Maven**，按上文 `mvn spring-boot:run` 即可（Kafka 地址按本机改）。
