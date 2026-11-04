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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;
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

        InboundEvent event = service.ingest(req, Protocol.HTTP, IngressContext.NONE);

        assertNotNull(event.getEventId());
        assertEquals("d1", event.getDeviceId());
        assertEquals("d1", event.getSourceId(), "sourceId falls back to deviceId when nothing else exists");
        assertEquals(EventType.DETECT, event.getType());
        assertEquals(Protocol.HTTP, event.getProtocol());
        assertNotNull(event.getReceivedAt());
        assertNotNull(event.getOccurredAt());

        ArgumentCaptor<InboundEvent> captor = ArgumentCaptor.forClass(InboundEvent.class);
        verify(kafkaProducer).send(captor.capture());
        assertEquals(event.getEventId(), captor.getValue().getEventId());
    }

    @Test
    void missingDeviceIdIsAcceptedWithAnonymousSource() {
        // most analysers cannot report a deviceId; the gateway must not reject the data over it
        IngestRequest req = new IngestRequest();
        req.setType("POWER");
        req.setPayload(mapper.createObjectNode().put("state", "OFF"));

        InboundEvent event = service.ingest(req, Protocol.HTTP, IngressContext.NONE);

        assertNull(event.getDeviceId());
        assertEquals(IngressContext.ANONYMOUS, event.getSourceId());
        verify(kafkaProducer).send(event);
    }

    @Test
    void sourceIdIsUsedWhenTheBodyHasNoDeviceId() {
        IngestRequest req = new IngestRequest();
        req.setSourceId("ward-3-analyzer");
        req.setType("POWER");
        req.setPayload(mapper.createObjectNode().put("state", "ON"));

        InboundEvent event = service.ingest(req, Protocol.HTTP, IngressContext.NONE);

        assertEquals("ward-3-analyzer", event.getSourceId());
        assertNull(event.getDeviceId(), "a declared sourceId must not invent a deviceId");
    }

    @Test
    void boundClientIdOutranksThePeerAddress() {
        IngestRequest req = new IngestRequest();
        req.setType("POWER");
        req.setPayload(mapper.createObjectNode().put("state", "ON"));

        IngressContext context = new IngressContext(null, "krd-lab-01", null, "10.2.3.4");
        InboundEvent event = service.ingest(req, Protocol.TCP, context);

        assertEquals("krd-lab-01", event.getSourceId());
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
        InboundEvent event = service.ingest(req, Protocol.MQTT, context);

        assertEquals("topic-id", event.getSourceId(), "the broker route outranks the body");
        assertEquals("body-id", event.getDeviceId(), "deviceId is reported as-is, never overwritten");
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

        InboundEvent event = service.ingest(req, Protocol.TCP, IngressContext.NONE);

        assertEquals(EventType.REPORT, event.getType());
        assertEquals("100C200017", event.getDeviceId());
        assertEquals("krd", event.getSiteId());
        assertEquals("100C200017-20261103232734", event.getIdempotencyKey());
        assertEquals(Instant.parse("2026-11-03T23:27:34Z"), event.getOccurredAt());
        assertEquals(77, event.getPayload().get("result").size());
        assertEquals("福州艾迪康医学检验", event.getPayload().get("hospital").asText());

        ArgumentCaptor<InboundEvent> captor = ArgumentCaptor.forClass(InboundEvent.class);
        verify(kafkaProducer).send(captor.capture());
        assertEquals(77, captor.getValue().getPayload().get("result").size());
    }

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

        InboundEvent event = service.ingest(req, Protocol.TCP, IngressContext.NONE);

        assertEquals(EventType.SAMPLE, event.getType());
        assertEquals("屏幕测试6", event.getDeviceId());
        assertEquals("dnk", event.getSiteId());
        assertEquals("89860625790032599518", event.getPayload().get("iccid").asText());
        assertEquals("-1.000", event.getPayload().get("content").get("concentration").asText());
        assertEquals("其他", event.getPayload().get("content").get("sampleType").asText());
        assertEquals("36.000", event.getPayload().get("content").get("temperature").asText());

        ArgumentCaptor<InboundEvent> captor = ArgumentCaptor.forClass(InboundEvent.class);
        verify(kafkaProducer).send(captor.capture());
        assertEquals(EventType.SAMPLE, captor.getValue().getType());
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
        InboundEvent first = service.ingest(sampleRequest("screen-6-20261104112129"), Protocol.TCP, IngressContext.NONE);
        InboundEvent second = service.ingest(sampleRequest("screen-6-20261104112144"), Protocol.TCP, IngressContext.NONE);

        assertNotEquals(first.getEventId(), second.getEventId());
        ArgumentCaptor<InboundEvent> captor = ArgumentCaptor.forClass(InboundEvent.class);
        verify(kafkaProducer, times(2)).send(captor.capture());
        assertEquals("screen-6-20261104112129", captor.getAllValues().get(0).getIdempotencyKey());
        assertEquals("screen-6-20261104112144", captor.getAllValues().get(1).getIdempotencyKey());
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

        InboundEvent event = service.ingest(req, Protocol.TCP, IngressContext.NONE);

        assertEquals(key, event.getIdempotencyKey(), "padding must not reach the downstream store");
    }

    @Test
    void blankIdempotencyKeyBecomesNull() {
        IngestRequest req = new IngestRequest();
        req.setType("POWER");
        req.setPayload(mapper.createObjectNode().put("state", "ON"));
        req.setIdempotencyKey("   ");

        assertNull(service.ingest(req, Protocol.TCP, IngressContext.NONE).getIdempotencyKey());
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
