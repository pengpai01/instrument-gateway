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

    /** Longest accepted idempotency key; also bounds the Kafka header and any downstream store. */
    static final int MAX_IDEMPOTENCY_KEY_LENGTH = 200;

    private final PayloadValidator payloadValidator;
    private final EventKafkaProducer kafkaProducer;

    public IngestService(PayloadValidator payloadValidator, EventKafkaProducer kafkaProducer) {
        this.payloadValidator = payloadValidator;
        this.kafkaProducer = kafkaProducer;
    }

    /**
     * @param request   raw body (token already verified/stripped by caller if needed)
     * @param protocol  HTTP | TCP | MQTT
     * @param context   where the message arrived from, or {@code null}; used to derive
     *                  {@code sourceId} when the body carries no identity of its own
     * @return ids and routing facts the caller needs to answer the client; the Kafka body
     *         itself is the payload alone
     */
    public IngestResult ingest(IngestRequest request, Protocol protocol, IngressContext context) {
        if (request == null) {
            throw new IngestException(400, "body is required");
        }

        IngressContext ingress = context != null ? context : IngressContext.NONE;
        // deviceId stays optional on purpose: most analysers cannot report a stable one.
        String deviceId = blankToNull(request.getDeviceId());
        String sourceId = ingress.resolveSource(blankToNull(request.getSourceId()), deviceId);

        EventType type;
        try {
            type = EventType.from(request.getType());
        } catch (Exception e) {
            throw new IngestException(400, "unknown or missing type: " + request.getType());
        }
        if (type == null) {
            throw new IngestException(400, "type is required (DETECT|POWER|ALARM|REPORT|SAMPLE)");
        }

        try {
            payloadValidator.validate(type, request.getPayload());
        } catch (IllegalArgumentException e) {
            throw new IngestException(400, e.getMessage());
        }

        String idempotencyKey = blankToNull(request.getIdempotencyKey());
        if (idempotencyKey != null && idempotencyKey.length() > MAX_IDEMPOTENCY_KEY_LENGTH) {
            throw new IngestException(400, "idempotencyKey must be at most "
                    + MAX_IDEMPOTENCY_KEY_LENGTH + " characters");
        }

        // occurredAt is still parsed so a malformed timestamp keeps failing with 400, but it has
        // no field to land in while the Kafka body carries the payload alone.
        if (StringUtils.hasText(request.getOccurredAt())) {
            try {
                Instant.parse(request.getOccurredAt());
            } catch (Exception e) {
                throw new IngestException(400, "occurredAt must be ISO-8601 instant");
            }
        }

        String eventId = UUID.randomUUID().toString();
        Instant receivedAt = Instant.now();

        InboundEvent event = new InboundEvent();
        event.setPayload(request.getPayload());

        // Duplicate suppression deliberately lives downstream, not here. An in-memory set would
        // either drop a record that Kafka then fails to persist (real data loss) or have to wait
        // for the ack before recording (blocking on every message), and it would still miss
        // duplicates after a restart or on a second instance. The key travels to Kafka in a
        // header, so the consumer de-duplicates a durable, replayable copy. Key and header are
        // metadata, not body: sourceId keeps one source on one partition.
        kafkaProducer.send(event, sourceId, idempotencyKey);
        log.info("ingested eventId={} sourceId={} deviceId={} type={} protocol={} idempotencyKey={}",
                eventId, sourceId, deviceId, type, protocol, idempotencyKey);
        return new IngestResult(eventId, sourceId, deviceId, type, receivedAt);
    }

    private static String blankToNull(String s) {
        return StringUtils.hasText(s) ? s.trim() : null;
    }
}
