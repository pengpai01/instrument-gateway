package com.danah.instrument.gateway.inbound;

import com.danah.instrument.gateway.inbound.model.EventType;
import com.danah.instrument.gateway.inbound.model.InboundEvent;
import com.danah.instrument.gateway.inbound.model.IngestRequest;
import com.danah.instrument.gateway.inbound.model.Protocol;
import com.danah.instrument.gateway.inbound.payloads.PayloadValidator;
import com.danah.instrument.gateway.kafka.EventKafkaProducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
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

        InboundEvent event = service.ingest(req, Protocol.HTTP, null);

        assertNotNull(event.getEventId());
        assertEquals("d1", event.getDeviceId());
        assertEquals(EventType.DETECT, event.getType());
        assertEquals(Protocol.HTTP, event.getProtocol());
        assertNotNull(event.getReceivedAt());
        assertNotNull(event.getOccurredAt());

        ArgumentCaptor<InboundEvent> captor = ArgumentCaptor.forClass(InboundEvent.class);
        verify(kafkaProducer).send(captor.capture());
        assertEquals(event.getEventId(), captor.getValue().getEventId());
    }

    @Test
    void missingDeviceIdRejected() {
        IngestRequest req = new IngestRequest();
        req.setType("POWER");
        req.setPayload(mapper.createObjectNode().put("state", "OFF"));
        IngestException ex = assertThrows(IngestException.class,
                () -> service.ingest(req, Protocol.HTTP, null));
        assertEquals(400, ex.getStatus());
    }

    @Test
    void unknownTypeRejected() {
        IngestRequest req = new IngestRequest();
        req.setDeviceId("d1");
        req.setType("FOO");
        req.setPayload(mapper.createObjectNode());
        assertThrows(IngestException.class, () -> service.ingest(req, Protocol.TCP, null));
    }

    @Test
    void mqttTopicDeviceIdWins() {
        IngestRequest req = new IngestRequest();
        req.setDeviceId("body-id");
        req.setType("POWER");
        req.setPayload(mapper.createObjectNode().put("state", "ON"));

        InboundEvent event = service.ingest(req, Protocol.MQTT, "topic-id");
        assertEquals("topic-id", event.getDeviceId());
    }
}
