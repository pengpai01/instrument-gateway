package com.danah.instrument.gateway.kafka;

import com.danah.instrument.gateway.config.KafkaTopicProperties;
import com.danah.instrument.gateway.inbound.model.EventType;
import com.danah.instrument.gateway.inbound.model.InboundEvent;
import com.danah.instrument.gateway.inbound.model.Protocol;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Guards the two things a consumer relies on: the partition key and the idempotency header.
 * The body carries the key too, but a header lets a stream job de-duplicate without parsing.
 */
class EventKafkaProducerTest {

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
        // same shape the application uses: Instant as an ISO string, not an epoch array
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        producer = new EventKafkaProducer(template, topics, mapper);
    }

    @Test
    @SuppressWarnings("unchecked")
    void idempotencyKeyGoesOnAHeaderAndInTheBody() {
        producer.send(event("dev-1", "dev-1", "order-1-20261104120000"));

        ProducerRecord<String, String> record = sentRecord();

        assertEquals("instr.event.v1", record.topic());
        Header header = record.headers().lastHeader(EventKafkaProducer.HEADER_IDEMPOTENCY_KEY);
        assertNotNull(header, "the consumer de-duplicates on this header");
        assertEquals("order-1-20261104120000",
                new String(header.value(), StandardCharsets.UTF_8));
        assertTrue(record.value().contains("\"idempotencyKey\":\"order-1-20261104120000\""),
                "the body must keep the key as well, so a replay from the topic is self-contained");
    }

    @Test
    @SuppressWarnings("unchecked")
    void noHeaderWhenTheClientSentNoKey() {
        producer.send(event("dev-1", "dev-1", null));

        ProducerRecord<String, String> record = sentRecord();

        assertNull(record.headers().lastHeader(EventKafkaProducer.HEADER_IDEMPOTENCY_KEY));
        assertTrue(record.value().contains("\"idempotencyKey\":null"),
                "the field stays in the body as null, so consumers see an explicit absence");
    }

    @Test
    @SuppressWarnings("unchecked")
    void partitionKeyIsSourceIdEvenWhenDeviceIdDiffers() {
        // one analyser can report many deviceIds; ordering follows the ingress identity
        producer.send(event("krd-lab-01", "100C200017", "k1"));

        assertEquals("krd-lab-01", sentRecord().key());
    }

    @Test
    @SuppressWarnings("unchecked")
    void partitionKeySurvivesAMissingDeviceId() {
        producer.send(event("10.2.3.4", null, "k2"));

        ProducerRecord<String, String> record = sentRecord();
        assertEquals("10.2.3.4", record.key(), "a null deviceId must not null the key");
        assertTrue(record.value().contains("\"deviceId\":null"));
    }

    @SuppressWarnings("unchecked")
    private ProducerRecord<String, String> sentRecord() {
        ArgumentCaptor<ProducerRecord<String, String>> captor =
                ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(captor.capture());
        return captor.getValue();
    }

    private static InboundEvent event(String sourceId, String deviceId, String idempotencyKey) {
        InboundEvent event = new InboundEvent();
        event.setEventId("event-1");
        event.setSourceId(sourceId);
        event.setDeviceId(deviceId);
        event.setType(EventType.POWER);
        event.setOccurredAt(Instant.parse("2026-11-04T12:00:00Z"));
        event.setReceivedAt(Instant.parse("2026-11-04T12:00:01Z"));
        event.setProtocol(Protocol.TCP);
        event.setIdempotencyKey(idempotencyKey);
        return event;
    }
}
