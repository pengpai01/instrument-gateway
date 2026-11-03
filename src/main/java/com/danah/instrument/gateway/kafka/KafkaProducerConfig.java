package com.danah.instrument.gateway.kafka;

import com.danah.instrument.gateway.config.KafkaTopicProperties;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerConfig.class);

    @Bean
    public ProducerFactory<String, String> producerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties(null));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
        props.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public NewTopic instrEventTopic(KafkaTopicProperties topicProperties) {
        if (!topicProperties.isAutoCreateTopic()) {
            log.info("gateway.kafka.auto-create-topic=false; ensure topic {} exists", topicProperties.getTopic());
            // Still declare bean with same name would fail conditional; use a no-op topic of 0? Better always declare.
        }
        log.info("Ensuring Kafka topic {} (partitions={}, rf={})",
                topicProperties.getTopic(), topicProperties.getPartitions(), topicProperties.getReplicationFactor());
        return TopicBuilder.name(topicProperties.getTopic())
                .partitions(topicProperties.getPartitions())
                .replicas(topicProperties.getReplicationFactor())
                .build();
    }
}
