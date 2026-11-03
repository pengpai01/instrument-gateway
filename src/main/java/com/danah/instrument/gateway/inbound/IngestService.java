package com.danah.instrument.gateway.inbound;

import com.danah.instrument.gateway.inbound.model.EventType;
import com.danah.instrument.gateway.inbound.model.InboundEvent;
import com.danah.instrument.gateway.inbound.model.IngestRequest;
import com.danah.instrument.gateway.inbound.model.Protocol;
import com.danah.instrument.gateway.inbound.payloads.PayloadValidator;
import com.danah.instrument.gateway.kafka.EventKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.util.UUID;

/**
 * Validates + normalizes inbound payloads and publishes to Kafka.
 * Controllers / handlers must not assemble Kafka JSON themselves.
 */
@Service
public class IngestService {

    private static final Logger log = LoggerFactory.getLogger(IngestService.class);

    private final PayloadValidator payloadValidator;
    private final EventKafkaProducer kafkaProducer;

    public IngestService(PayloadValidator payloadValidator, EventKafkaProducer kafkaProducer) {
        this.payloadValidator = payloadValidator;
        this.kafkaProducer = kafkaProducer;
    }

    /**
     * @param request   raw body (token already verified/stripped by caller if needed)
     * @param protocol  HTTP | TCP | MQTT
     * @param deviceIdOverride when non-null (MQTT topic segment), wins over body.deviceId
     * @return normalized event that was published
     */
    public InboundEvent ingest(IngestRequest request, Protocol protocol, String deviceIdOverride) {
        if (request == null) {
            throw new IngestException(400, "body is required");
        }

        String deviceId = StringUtils.hasText(deviceIdOverride) ? deviceIdOverride.trim() : null;
        if (!StringUtils.hasText(deviceId)) {
            deviceId = request.getDeviceId();
        }
        if (!StringUtils.hasText(deviceId)) {
            throw new IngestException(400, "deviceId is required");
        }
        if (StringUtils.hasText(deviceIdOverride)
                && StringUtils.hasText(request.getDeviceId())
                && !deviceIdOverride.trim().equals(request.getDeviceId().trim())) {
            log.warn("deviceId mismatch: topic/override='{}' body='{}' — using override",
                    deviceIdOverride, request.getDeviceId());
        }

        EventType type;
        try {
            type = EventType.from(request.getType());
        } catch (Exception e) {
            throw new IngestException(400, "unknown or missing type: " + request.getType());
        }
        if (type == null) {
            throw new IngestException(400, "type is required (DETECT|POWER|ALARM)");
        }

        try {
            payloadValidator.validate(type, request.getPayload());
        } catch (IllegalArgumentException e) {
            throw new IngestException(400, e.getMessage());
        }

        Instant now = Instant.now();
        Instant occurredAt = now;
        if (StringUtils.hasText(request.getOccurredAt())) {
            try {
                occurredAt = Instant.parse(request.getOccurredAt());
            } catch (Exception e) {
                throw new IngestException(400, "occurredAt must be ISO-8601 instant");
            }
        }

        InboundEvent event = new InboundEvent();
        event.setEventId(UUID.randomUUID().toString());
        event.setDeviceId(deviceId.trim());
        event.setType(type);
        event.setOccurredAt(occurredAt);
        event.setReceivedAt(now);
        event.setProtocol(protocol);
        event.setPayload(request.getPayload());
        event.setSiteId(blankToNull(request.getSiteId()));
        event.setIdempotencyKey(blankToNull(request.getIdempotencyKey()));
        event.setExt(request.getExt());

        kafkaProducer.send(event);
        log.info("ingested eventId={} deviceId={} type={} protocol={}",
                event.getEventId(), event.getDeviceId(), event.getType(), event.getProtocol());
        return event;
    }

    private static String blankToNull(String s) {
        return StringUtils.hasText(s) ? s.trim() : null;
    }
}
