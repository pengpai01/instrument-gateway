package com.danah.instrument.gateway.inbound.mqtt;

import com.danah.instrument.gateway.auth.ApiKeyAuthenticator;
import com.danah.instrument.gateway.config.MqttProperties;
import com.danah.instrument.gateway.inbound.IngestException;
import com.danah.instrument.gateway.inbound.IngestService;
import com.danah.instrument.gateway.inbound.IngressContext;
import com.danah.instrument.gateway.inbound.model.IngestRequest;
import com.danah.instrument.gateway.inbound.model.Protocol;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

/**
 * MQTT adapter. If broker-url is empty or connection fails, process still starts;
 * HTTP/TCP remain available. Health reflects connection state.
 */
@Component
public class MqttInboundConfig implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(MqttInboundConfig.class);

    private final MqttProperties properties;
    private final IngestService ingestService;
    private final ApiKeyAuthenticator authenticator;
    private final ObjectMapper objectMapper;
    private final MqttHealthIndicator healthIndicator;

    private final AtomicReference<MqttClient> clientRef = new AtomicReference<>();

    public MqttInboundConfig(MqttProperties properties,
                             IngestService ingestService,
                             ApiKeyAuthenticator authenticator,
                             ObjectMapper objectMapper,
                             MqttHealthIndicator healthIndicator) {
        this.properties = properties;
        this.ingestService = ingestService;
        this.authenticator = authenticator;
        this.objectMapper = objectMapper;
        this.healthIndicator = healthIndicator;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!properties.isConfigured()) {
            log.info("gateway.mqtt.broker-url empty — MQTT adapter disabled (HTTP/TCP still active)");
            healthIndicator.setState(MqttHealthIndicator.State.DISABLED, "broker-url not configured");
            return;
        }
        try {
            connectAndSubscribe();
        } catch (Exception e) {
            log.warn("MQTT connect failed — continuing without MQTT: {}", e.toString());
            healthIndicator.setState(MqttHealthIndicator.State.DOWN, e.getMessage());
        }
    }

    private void connectAndSubscribe() throws Exception {
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            hostname = "unknown";
        }
        String clientId = properties.getClientIdPrefix() + hostname;
        MqttClient client = new MqttClient(properties.getBrokerUrl(), clientId, new MemoryPersistence());
        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setAutomaticReconnect(true);
        opts.setCleanSession(true);
        opts.setConnectionTimeout(10);
        if (StringUtils.hasText(properties.getUsername())) {
            opts.setUserName(properties.getUsername());
            if (properties.getPassword() != null) {
                opts.setPassword(properties.getPassword().toCharArray());
            }
        }
        client.connect(opts);
        client.subscribe(properties.getTopicFilter(), properties.getQos(), messageListener());
        clientRef.set(client);
        healthIndicator.setState(MqttHealthIndicator.State.UP, "connected to " + properties.getBrokerUrl());
        log.info("MQTT subscribed filter={} qos={} clientId={} broker={}",
                properties.getTopicFilter(), properties.getQos(), clientId, properties.getBrokerUrl());
    }

    private IMqttMessageListener messageListener() {
        return (topic, message) -> handleMessage(topic, message);
    }

    void handleMessage(String topic, MqttMessage message) {
        String json = new String(message.getPayload(), StandardCharsets.UTF_8);
        String topicSegment = extractSourceSegment(topic);

        IngestRequest request;
        try {
            request = objectMapper.readValue(json, IngestRequest.class);
        } catch (Exception e) {
            log.warn("MQTT bad JSON topic={}: {} — discarded", topic, e.getMessage());
            return;
        }

        String token = request.getToken();
        request.setToken(null);
        // Token optional if absent? Spec says 可验 token — verify when present or always?
        // Task: TCP/MQTT 校验 JSON 内 token. Require token.
        if (!authenticator.matches(token)) {
            log.warn("MQTT auth failed topic={} — discarded", topic);
            return;
        }

        try {
            // the topic segment names the ingress channel, so it outranks a body deviceId;
            // neither is required, and the body deviceId is still carried through as-is
            IngressContext ingress = new IngressContext(
                    null, authenticator.clientIdOf(token), topicSegment, null);
            ingestService.ingest(request, Protocol.MQTT, ingress);
        } catch (IngestException e) {
            log.warn("MQTT ingest rejected topic={}: {} — discarded", topic, e.getMessage());
        } catch (Exception e) {
            log.error("MQTT ingest error topic={}: {}", topic, e.toString());
        }
    }

    /**
     * Topic pattern instr/+/up — the second segment labels the source. It is a routing fact
     * owned by the broker, which makes it a better identity than anything in the payload.
     */
    static String extractSourceSegment(String topic) {
        if (topic == null) {
            return null;
        }
        String[] parts = topic.split("/");
        if (parts.length >= 2) {
            return parts[1];
        }
        return null;
    }

    @PreDestroy
    public void shutdown() {
        MqttClient client = clientRef.getAndSet(null);
        if (client != null) {
            try {
                if (client.isConnected()) {
                    client.disconnect();
                }
                client.close();
            } catch (MqttException e) {
                log.debug("MQTT shutdown: {}", e.toString());
            }
        }
        healthIndicator.setState(MqttHealthIndicator.State.DOWN, "stopped");
    }
}
