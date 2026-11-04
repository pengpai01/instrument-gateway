package com.danah.instrument.gateway.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Locks the two Kafka bootstrap paths without opening a broker connection.
 * {@code laptop} is the intranet broker; default and {@code local} stay on localhost.
 */
class KafkaProfileConfigTest {

    @Test
    void defaultProfileStaysOnLocalhostPlaceholder() throws IOException {
        PropertySource<?> source = load("application.yml");
        assertEquals("${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}",
                source.getProperty("spring.kafka.bootstrap-servers"));
        assertEquals("instr.event.v1", source.getProperty("gateway.kafka.topic"));
        assertEquals("${GATEWAY_MQTT_BROKER_URL:}", source.getProperty("gateway.mqtt.broker-url"));
    }

    @Test
    void localExampleStaysOnComposeLocalhostAndOptionalMqtt() throws IOException {
        PropertySource<?> source = load("application-local.example.yml");
        assertEquals("localhost:9092", source.getProperty("spring.kafka.bootstrap-servers"));
        assertEquals("tcp://localhost:1883", source.getProperty("gateway.mqtt.broker-url"));
        assertEquals("change-me-local-only", source.getProperty("gateway.api-key"));
    }

    @Test
    void laptopProfilePointsAtIntranetKafkaAndLeavesMqttUnset() throws IOException {
        PropertySource<?> source = load("application-laptop.yml");
        assertEquals("192.168.11.114:9092", source.getProperty("spring.kafka.bootstrap-servers"));
        assertEquals("instr.event.v1", source.getProperty("gateway.kafka.topic"));
        assertEquals("${GATEWAY_API_KEY:change-me-local-only}", source.getProperty("gateway.api-key"));
        assertNull(source.getProperty("gateway.mqtt.broker-url"),
                "laptop profile must not configure MQTT");
        assertNull(source.getProperty("gateway.mqtt.username"));
        assertNull(source.getProperty("gateway.mqtt.password"));
    }

    private static PropertySource<?> load(String classpathYaml) throws IOException {
        List<PropertySource<?>> loaded = new YamlPropertySourceLoader()
                .load(classpathYaml, new ClassPathResource(classpathYaml));
        assertEquals(1, loaded.size(), classpathYaml);
        return loaded.get(0);
    }
}
