package com.danah.instrument.gateway.kafka;

import com.danah.instrument.gateway.config.KafkaTopicProperties;
import com.danah.instrument.gateway.inbound.model.InboundEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class EventKafkaProducer {

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
        String key = event.getDeviceId();
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, json);
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
