package com.danah.instrument.gateway.kafka;

import com.danah.instrument.gateway.config.KafkaTopicProperties;
import com.danah.instrument.gateway.inbound.model.InboundEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Guards what a consumer relies on: the body is the payload alone, the partition key is the
 * ingress identity, and the de-duplication key travels on a header.
 */
class EventKafkaProducerTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private KafkaTemplate<String, String> template;
    private EventKafkaProducer producer;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        template = mock(KafkaTemplate.class);
        CompletableFuture<SendResult<String, String>> pending = new CompletableFuture<>();
        when(template.send(any(ProducerRecord.class))).thenReturn(pending);

        KafkaTopicProperties topics = new KafkaTopicProperties();
        topics.setTopic("instr.event.v1");
        producer = new EventKafkaProducer(template, topics, mapper);
    }

    @Test
    @SuppressWarnings("unchecked")
    void bodyIsThePayloadAlone() throws Exception {
        producer.send(event(payload()), "dev-1", "order-1-20261104120000");

        ProducerRecord<String, String> record = sentRecord();
        JsonNode body = mapper.readTree(record.value());

        assertTrue(body.isObject(), "the body must stay a JSON object");
        assertEquals(2, body.size(), "no envelope keys around the payload");
        assertEquals("ON", body.get("state").asText());
        assertEquals("boot", body.get("reason").asText());
    }

    @Test
    @SuppressWarnings("unchecked")
    void idempotencyKeyGoesOnAHeaderOnly() {
        producer.send(event(payload()), "dev-1", "order-1-20261104120000");

        ProducerRecord<String, String> record = sentRecord();

        assertEquals("instr.event.v1", record.topic());
        Header header = record.headers().lastHeader(EventKafkaProducer.HEADER_IDEMPOTENCY_KEY);
        assertNotNull(header, "the consumer de-duplicates on this header");
        assertEquals("order-1-20261104120000",
                new String(header.value(), StandardCharsets.UTF_8));
        assertFalse(record.value().contains("idempotencyKey"),
                "the key must not reappear in the body");
    }

    @Test
    @SuppressWarnings("unchecked")
    void noHeaderWhenTheClientSentNoKey() {
        producer.send(event(payload()), "dev-1", null);

        ProducerRecord<String, String> record = sentRecord();

        assertNull(record.headers().lastHeader(EventKafkaProducer.HEADER_IDEMPOTENCY_KEY));
        assertFalse(record.value().contains("idempotencyKey"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void partitionKeyIsTheGivenSourceId() {
        // one analyser can report many deviceIds; ordering follows the ingress identity
        producer.send(event(payload()), "krd-lab-01", "k1");

        assertEquals("krd-lab-01", sentRecord().key());
    }

    @SuppressWarnings("unchecked")
    private ProducerRecord<String, String> sentRecord() {
        ArgumentCaptor<ProducerRecord<String, String>> captor =
                ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(captor.capture());
        return captor.getValue();
    }

    private JsonNode payload() {
        return mapper.createObjectNode().put("state", "ON").put("reason", "boot");
    }

    private static InboundEvent event(JsonNode payload) {
        InboundEvent event = new InboundEvent();
        event.setPayload(payload);
        return event;
    }
}
