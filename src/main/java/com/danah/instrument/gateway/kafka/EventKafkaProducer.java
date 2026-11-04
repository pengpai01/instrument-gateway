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

    public void send(InboundEvent event) {
        String json;
        try {
            json = objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("failed to serialize InboundEvent", e);
        }
        String topic = topicProperties.getTopic();
        // sourceId, not deviceId: deviceId is optional and null for instruments that cannot
        // report one, and a null key would round-robin one source across partitions.
        String key = event.getSourceId();

        ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, (Integer) null, key, json);
        // Duplicated on a header on purpose: the consumer can de-duplicate without parsing the
        // body, and kafka-console-consumer can print it next to the key. The gateway itself does
        // not drop duplicates — see the comment in IngestService for why.
        if (event.getIdempotencyKey() != null) {
            record.headers().add(HEADER_IDEMPOTENCY_KEY,
                    event.getIdempotencyKey().getBytes(StandardCharsets.UTF_8));
        }

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Kafka send failed topic={} key={} eventId={}: {}",
                        topic, key, event.getEventId(), ex.toString());
            } else {
                log.debug("Kafka sent topic={} partition={} offset={} eventId={}",
                        topic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        event.getEventId());
            }
        });
    }
}
