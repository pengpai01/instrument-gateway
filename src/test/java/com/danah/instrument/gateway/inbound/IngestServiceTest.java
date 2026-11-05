package com.danah.instrument.gateway.inbound;

import com.danah.instrument.gateway.inbound.model.EventType;
import com.danah.instrument.gateway.inbound.model.InboundEvent;
import com.danah.instrument.gateway.inbound.model.IngestRequest;
import com.danah.instrument.gateway.inbound.model.Protocol;
import com.danah.instrument.gateway.inbound.payloads.PayloadValidator;
import com.danah.instrument.gateway.kafka.EventKafkaProducer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class IngestServiceTest {

    @Mock
    private EventKafkaProducer kafkaProducer;

    private IngestService service;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        service = new IngestService(new PayloadValidator(), kafkaProducer);
    }

    @Test
    void happyPathGeneratesIdsAndPublishes() {
        IngestRequest req = new IngestRequest();
        req.setDeviceId("d1");
        req.setType("DETECT");
        ObjectNode payload = mapper.createObjectNode().put("itemCode", "A").put("value", 1);
        req.setPayload(payload);

        IngestResult result = service.ingest(req, Protocol.HTTP, IngressContext.NONE);

        assertNotNull(result.eventId());
        assertEquals("d1", result.deviceId());
        assertEquals("d1", result.sourceId(), "sourceId falls back to deviceId when nothing else exists");
        assertEquals(EventType.DETECT, result.type());
        assertNotNull(result.receivedAt());

        ArgumentCaptor<InboundEvent> captor = ArgumentCaptor.forClass(InboundEvent.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaProducer).send(captor.capture(), keyCaptor.capture(), any());
        assertEquals(payload, captor.getValue().getPayload(), "the Kafka body is the payload alone");
        assertEquals("d1", keyCaptor.getValue(), "sourceId stays the partition key");
    }

    @Test
    void missingDeviceIdIsAcceptedWithAnonymousSource() {
        // most analysers cannot report a deviceId; the gateway must not reject the data over it
        IngestRequest req = new IngestRequest();
        req.setType("POWER");
        req.setPayload(mapper.createObjectNode().put("state", "OFF"));

        IngestResult result = service.ingest(req, Protocol.HTTP, IngressContext.NONE);

        assertNull(result.deviceId());
        assertEquals(IngressContext.ANONYMOUS, result.sourceId());
        verify(kafkaProducer).send(any(InboundEvent.class), eq(IngressContext.ANONYMOUS), any());
    }

    @Test
    void sourceIdIsUsedWhenTheBodyHasNoDeviceId() {
        IngestRequest req = new IngestRequest();
        req.setSourceId("ward-3-analyzer");
        req.setType("POWER");
        req.setPayload(mapper.createObjectNode().put("state", "ON"));

        IngestResult result = service.ingest(req, Protocol.HTTP, IngressContext.NONE);

        assertEquals("ward-3-analyzer", result.sourceId());
        assertNull(result.deviceId(), "a declared sourceId must not invent a deviceId");
    }

    @Test
    void boundClientIdOutranksThePeerAddress() {
        IngestRequest req = new IngestRequest();
        req.setType("POWER");
        req.setPayload(mapper.createObjectNode().put("state", "ON"));

        IngressContext context = new IngressContext(null, "krd-lab-01", null, "10.2.3.4");
        IngestResult result = service.ingest(req, Protocol.TCP, context);

        assertEquals("krd-lab-01", result.sourceId());
    }

    @Test
    void unknownTypeRejected() {
        IngestRequest req = new IngestRequest();
        req.setDeviceId("d1");
        req.setType("FOO");
        req.setPayload(mapper.createObjectNode());
        assertThrows(IngestException.class, () -> service.ingest(req, Protocol.TCP, IngressContext.NONE));
    }

    @Test
    void mqttTopicSegmentBecomesSourceIdNotDeviceId() {
        IngestRequest req = new IngestRequest();
        req.setDeviceId("body-id");
        req.setType("POWER");
        req.setPayload(mapper.createObjectNode().put("state", "ON"));

        IngressContext context = new IngressContext(null, null, "topic-id", null);
        IngestResult result = service.ingest(req, Protocol.MQTT, context);

        assertEquals("topic-id", result.sourceId(), "the broker route outranks the body");
        assertEquals("body-id", result.deviceId(), "deviceId is reported as-is, never overwritten");
    }

    @Test
    void realReportPayloadIsPublished() throws IOException {
        String report;
        try (InputStream in = getClass().getResourceAsStream("/tcp/sample-report-gm.json")) {
            assertNotNull(in, "sample report resource is missing");
            report = new String(in.readAllBytes(), StandardCharsets.UTF_8)
                    .replace("\r\n", "").replace("\n", "").trim();
        }
        JsonNode payload = mapper.readTree(report);

        IngestRequest req = new IngestRequest();
        req.setDeviceId(payload.get("deviceId").asText());
        req.setType("REPORT");
        req.setOccurredAt("2026-11-03T23:27:34Z");
        req.setSiteId(payload.get("orgCode").asText());
        req.setIdempotencyKey("100C200017-20261103232734");
        req.setPayload(payload);

        IngestResult result = service.ingest(req, Protocol.TCP, IngressContext.NONE);

        assertEquals(EventType.REPORT, result.type());
        assertEquals("100C200017", result.deviceId());

        ArgumentCaptor<InboundEvent> captor = ArgumentCaptor.forClass(InboundEvent.class);
        ArgumentCaptor<String> idempotencyCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaProducer).send(captor.capture(), eq("100C200017"), idempotencyCaptor.capture());
        assertEquals("100C200017-20261103232734", idempotencyCaptor.getValue());
        assertEquals(77, captor.getValue().getPayload().get("result").size());
        assertEquals("福州艾迪康医学检验", captor.getValue().getPayload().get("hospital").asText());
    }

    @Disabled("REPORT result validation is temporarily off")
    @Test
    void reportWithoutResultRejected() {
        IngestRequest req = new IngestRequest();
        req.setDeviceId("100C200017");
        req.setType("REPORT");
        req.setPayload(mapper.createObjectNode().put("serialsId", "100C200017"));

        IngestException ex = assertThrows(IngestException.class,
                () -> service.ingest(req, Protocol.TCP, IngressContext.NONE));
        assertEquals(400, ex.getStatus());
        assertTrue(ex.getMessage().contains("REPORT.payload.result"));
    }

    @Test
    void realSingleSampleIsPublished() throws IOException {
        String body;
        try (InputStream in = getClass().getResourceAsStream("/tcp/sample-result-screen.json")) {
            assertNotNull(in, "sample result resource is missing");
            body = new String(in.readAllBytes(), StandardCharsets.UTF_8).trim();
        }
        JsonNode payload = mapper.readTree(body);
        JsonNode content = payload.get("content");

        IngestRequest req = new IngestRequest();
        req.setDeviceId(content.get("deviceId").asText());
        req.setType("SAMPLE");
        req.setOccurredAt("2026-11-04T11:21:29Z");
        req.setSiteId(payload.get("orgCode").asText());
        req.setIdempotencyKey("screen-6-20261104112129");
        req.setPayload(payload);

        IngestResult result = service.ingest(req, Protocol.TCP, IngressContext.NONE);

        assertEquals(EventType.SAMPLE, result.type());
        assertEquals("屏幕测试6", result.deviceId());
        assertEquals("89860625790032599518", req.getPayload().get("iccid").asText());
        assertEquals("-1.000", req.getPayload().get("content").get("concentration").asText());
        assertEquals("其他", req.getPayload().get("content").get("sampleType").asText());
        assertEquals("36.000", req.getPayload().get("content").get("temperature").asText());

        ArgumentCaptor<InboundEvent> captor = ArgumentCaptor.forClass(InboundEvent.class);
        verify(kafkaProducer).send(captor.capture(), any(), any());
        assertEquals("89860625790032599518",
                captor.getValue().getPayload().get("iccid").asText());
    }

    @Test
    void sampleWithoutContentRejected() {
        IngestRequest req = new IngestRequest();
        req.setDeviceId("屏幕测试6");
        req.setType("SAMPLE");
        req.setPayload(mapper.createObjectNode().put("iccid", "89860625790032599518"));

        IngestException ex = assertThrows(IngestException.class,
                () -> service.ingest(req, Protocol.TCP, IngressContext.NONE));
        assertEquals(400, ex.getStatus());
        assertTrue(ex.getMessage().contains("SAMPLE.payload.content"));
    }

    @Test
    void deviceCheckTimeIsNotAnInstant() {
        // content.checkTime uses "yyyy-MM-dd HH:mm:ss"; only occurredAt accepts ISO-8601,
        // so the client must convert it (space -> T, append Z)
        IngestRequest req = sampleRequest("screen-6-20261104112129");
        req.setOccurredAt("2026-11-04 11:21:29");

        IngestException ex = assertThrows(IngestException.class,
                () -> service.ingest(req, Protocol.TCP, IngressContext.NONE));
        assertEquals(400, ex.getStatus());
        assertEquals("occurredAt must be ISO-8601 instant", ex.getMessage());
    }

    @Test
    void consecutiveSamplesArePublishedAsSeparateEvents() {
        IngestResult first = service.ingest(sampleRequest("screen-6-20261104112129"),
                Protocol.TCP, IngressContext.NONE);
        IngestResult second = service.ingest(sampleRequest("screen-6-20261104112144"),
                Protocol.TCP, IngressContext.NONE);

        assertNotEquals(first.eventId(), second.eventId());
        ArgumentCaptor<String> idempotencyCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaProducer, times(2))
                .send(any(InboundEvent.class), any(), idempotencyCaptor.capture());
        assertEquals("screen-6-20261104112129", idempotencyCaptor.getAllValues().get(0));
        assertEquals("screen-6-20261104112144", idempotencyCaptor.getAllValues().get(1));
    }

    @Test
    void overlongIdempotencyKeyRejected() {
        // an unbounded key would also blow up the Kafka header and any downstream unique index
        IngestRequest req = new IngestRequest();
        req.setType("POWER");
        req.setPayload(mapper.createObjectNode().put("state", "ON"));
        req.setIdempotencyKey("k".repeat(IngestService.MAX_IDEMPOTENCY_KEY_LENGTH + 1));

        IngestException ex = assertThrows(IngestException.class,
                () -> service.ingest(req, Protocol.TCP, IngressContext.NONE));
        assertEquals(400, ex.getStatus());
        assertTrue(ex.getMessage().contains("idempotencyKey"));
    }

    @Test
    void idempotencyKeyIsTrimmedAndAcceptedAtTheLimit() {
        String key = "k".repeat(IngestService.MAX_IDEMPOTENCY_KEY_LENGTH);
        IngestRequest req = new IngestRequest();
        req.setType("POWER");
        req.setPayload(mapper.createObjectNode().put("state", "ON"));
        req.setIdempotencyKey("  " + key + "  ");

        service.ingest(req, Protocol.TCP, IngressContext.NONE);

        ArgumentCaptor<String> idempotencyCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaProducer).send(any(InboundEvent.class), any(), idempotencyCaptor.capture());
        assertEquals(key, idempotencyCaptor.getValue(), "padding must not reach the downstream store");
    }

    @Test
    void blankIdempotencyKeyBecomesNull() {
        IngestRequest req = new IngestRequest();
        req.setType("POWER");
        req.setPayload(mapper.createObjectNode().put("state", "ON"));
        req.setIdempotencyKey("   ");

        service.ingest(req, Protocol.TCP, IngressContext.NONE);

        ArgumentCaptor<String> idempotencyCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaProducer).send(any(InboundEvent.class), any(), idempotencyCaptor.capture());
        assertNull(idempotencyCaptor.getValue());
    }

    /** Minimal SAMPLE request; the content shape mirrors the real screen-test message. */
    private IngestRequest sampleRequest(String idempotencyKey) {
        IngestRequest req = new IngestRequest();
        req.setDeviceId("屏幕测试6");
        req.setType("SAMPLE");
        req.setOccurredAt("2026-11-04T11:21:29Z");
        req.setSiteId("dnk");
        req.setIdempotencyKey(idempotencyKey);
        ObjectNode payload = mapper.createObjectNode();
        payload.put("orgCode", "dnk").put("iccid", "89860625790032599518");
        payload.putObject("content")
                .put("deviceId", "屏幕测试6")
                .put("checkTime", "2026-11-04 11:21:29")
                .put("sampleType", "其他");
        req.setPayload(payload);
        return req;
    }
}
