package com.danah.instrument.gateway.kafka;

import com.danah.instrument.gateway.config.KafkaTopicProperties;
import com.danah.instrument.gateway.inbound.model.InboundEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

@Component
public class EventKafkaProducer {

    /** Kafka header carrying the idempotency key, so a consumer can de-duplicate on it. */
    public static final String HEADER_IDEMPOTENCY_KEY = "idempotencyKey";

    private static final Logger log = LoggerFactory.getLogger(EventKafkaProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaTopicProperties topicProperties;
    private final ObjectMapper objectMapper;

    public EventKafkaProducer(KafkaTemplate<String, String> kafkaTemplate,
                              KafkaTopicProperties topicProperties,
                              ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicProperties = topicProperties;
        this.objectMapper = objectMapper;
    }

    public void send(InboundEvent event, String key, String idempotencyKey) {
        String json;
        try {
            // the topic value is the payload itself: writing `event` would add a "payload" wrapper
            json = objectMapper.writeValueAsString(event.getPayload());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("failed to serialize payload", e);
        }
        String topic = topicProperties.getTopic();
        // sourceId, not deviceId: deviceId is optional and null for instruments that cannot
        // report one, and a null key would round-robin one source across partitions. It stays a
        // producer record key, so it never appears in the body.

        ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, (Integer) null, key, json);
        // The only place the de-duplication key still travels in: the body is the payload alone,
        // so a consumer reads this header instead of deserialising the JSON. The gateway itself
        // does not drop duplicates — see the comment in IngestService for why.
        if (idempotencyKey != null) {
            record.headers().add(HEADER_IDEMPOTENCY_KEY,
                    idempotencyKey.getBytes(StandardCharsets.UTF_8));
        }

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Kafka send failed topic={} key={}: {}",
                        topic, key, ex.toString());
            } else {
                log.debug("Kafka sent topic={} partition={} offset={}",
                        topic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}
