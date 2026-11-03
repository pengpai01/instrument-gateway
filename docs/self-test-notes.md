# Instrument Gateway — First-iteration self-test notes

**When:** 2026-11-03 (Asia/Shanghai)  
**Where:** box `/workspace/instrument-gateway`  
**API key placeholder:** `change-me-local-only` (env `GATEWAY_API_KEY` / `application-local.yml`, **not** committed)

## Environment

```bash
export JAVA_HOME=/home/box/tools/jdk-17.0.21+9
export PATH="$JAVA_HOME/bin:/home/box/tools/apache-maven-3.9.9/bin:/home/box/tools/docker/bin:$PATH"
export DOCKER_HOST=tcp://127.0.0.1:2375
export GATEWAY_API_KEY=change-me-local-only
```

Kafka + Zookeeper (+ optional Mosquitto):

```bash
cd /workspace/instrument-gateway
docker compose up -d zookeeper kafka
# optional: docker compose up -d mosquitto
```

Compose Kafka listeners (required for host `localhost:9092`):

- `KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092`
- `KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092`
- `KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT`
- Topic: `instr.event.v1` (auto-create / NewTopic bean)

## Run gateway

```bash
cd /workspace/instrument-gateway
mvn -q -DskipTests package
# MQTT disabled (empty broker-url) — proves process still starts:
java -jar target/instrument-gateway-0.1.0-SNAPSHOT.jar --spring.profiles.active=local
# Optional MQTT smoke against compose mosquitto:
# GATEWAY_MQTT_BROKER_URL=tcp://localhost:1883 java -jar target/instrument-gateway-0.1.0-SNAPSHOT.jar --spring.profiles.active=local
```

Windows clone note: `JAVA_HOME=E:\jdk-17` then `mvn` (see README).

## Definition of done

| # | Item | Result | Evidence |
|---|------|--------|----------|
| 1 | Process starts (`java -jar` / `mvn spring-boot:run`) | **pass** | Log: `Started GatewayApplication`; TCP `:9100`; HTTP `:8080`. With empty MQTT: `gateway.mqtt.broker-url empty — MQTT adapter disabled`. |
| 2 | HTTP DETECT/POWER/ALARM + valid key → Kafka; no key → 401 | **pass** | No key → `401` `{"error":"unauthorized",...}`. With `X-Api-Key: change-me-local-only` → `202` for DETECT/POWER/ALARM. Kafka dump keys `dev-http-1` for all three types (see `docs/kafka-sample-dump.txt`). |
| 3 | TCP three types → Kafka; oversize/bad JSON discarded + logged | **pass** | Three TCP frames → Kafka `dev-tcp-1` DETECT/POWER/ALARM. Bad JSON: `TCP bad JSON ... discarded`. Oversize (len=2000): `TCP frame discarded (too long): Adjusted frame length exceeds 1023: 2002`. |
| 4 | MQTT three types | **pass (optional compose)** / **blocked (user broker)** | User production/broker address **pending tomorrow**. Optional smoke on compose `mosquitto:1883`: gateway subscribed `instr/+/up`; three `mosquitto_pub` messages → log `protocol=MQTT` DETECT/POWER/ALARM (`dev-mqtt-1`). Unconfigured broker earlier: process still UP. |
| 5 | `GET /actuator/health` includes kafka | **pass** | JSON includes `"kafka":{"status":"UP","details":{"clusterId":"...","nodes":1,"bootstrapServers":["localhost:9092"]}}`. MQTT shows `UNKNOWN` when disabled, `UP` when connected to compose mosquitto. |
| 6 | README + `application-local.example.yml` | **pass** | Present in repo tip. |
| 7 | No prod secrets / no downlink API / no business DB | **pass** | `.gitignore` has `application-local.yml`; no downlink controllers; no JPA/DB. |

## Kafka observe (host bootstrap `localhost:9092`)

Prefer a **client container** or host tools; avoid relying on `docker exec … kafka-topics --list` (flaky empty stdout on this daemon).

```bash
# Console consumer via ephemeral client on compose network
docker run --rm --network instrument-gateway_default confluentinc/cp-kafka:7.6.1 \
  kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic instr.event.v1 --from-beginning --timeout-ms 10000 \
  --property print.key=true --property key.separator='|'

# Or from host if you have Kafka CLI / kcat pointed at localhost:9092
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic instr.event.v1 --from-beginning --property print.key=true --property key.separator='|'
```

Sample dump from this run (HTTP+TCP): `docs/kafka-sample-dump.txt`.

## Health JSON highlights

```json
{
  "status": "UP",
  "components": {
    "kafka": {
      "status": "UP",
      "details": {
        "clusterId": "b1EWD-48SwmvC9LbKZtrVg",
        "nodes": 1,
        "nodeIds": "1",
        "bootstrapServers": ["localhost:9092"]
      }
    },
    "mqtt": {
      "status": "UNKNOWN",
      "details": { "mqtt": "broker-url not configured" }
    },
    "ping": { "status": "UP" }
  }
}
```

(With `GATEWAY_MQTT_BROKER_URL=tcp://localhost:1883`, `mqtt.status` became `UP` / `connected to tcp://localhost:1883`.)

## HTTP / TCP quick commands

```bash
# 401
curl -s -D- -H 'Content-Type: application/json' \
  -d '{"deviceId":"d1","type":"DETECT","payload":{"itemCode":"T","value":1}}' \
  http://127.0.0.1:8080/api/v1/ingest

# 202 DETECT
curl -s -H 'Content-Type: application/json' -H 'X-Api-Key: change-me-local-only' \
  -d '{"deviceId":"dev-http-1","type":"DETECT","payload":{"itemCode":"TEMP","value":23.5,"unit":"C"}}' \
  http://127.0.0.1:8080/api/v1/ingest
```

TCP: 2-byte big-endian length + UTF-8 JSON (include `"token":"change-me-local-only"`).

## Code changes made during self-test

1. `docker-compose.yml` — added `KAFKA_LISTENERS` + `KAFKA_INTER_BROKER_LISTENER_NAME`; mosquitto without host bind-mount (remote Docker API).
2. `KafkaHealthIndicator` — explicit actuator component named `kafka`.
3. This notes file + sample Kafka dump.

## Open

- User MQTT broker URL: **blocked / tomorrow**.
- Remote GitHub already: `https://github.com/pengpai01/instrument-gateway.git`.
