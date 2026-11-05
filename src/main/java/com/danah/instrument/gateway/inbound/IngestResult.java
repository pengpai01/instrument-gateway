package com.danah.instrument.gateway.inbound;

import com.danah.instrument.gateway.inbound.model.EventType;

import java.time.Instant;

/**
 * What one ingest produced, for the caller that answers the client (HTTP 202) and for the log line.
 *
 * <p>Separate from the Kafka body on purpose: the topic value is the raw payload only, so the
 * metadata a caller needs cannot ride along in the message.
 */
public record IngestResult(String eventId,
                           String sourceId,
                           String deviceId,
                           EventType type,
                           Instant receivedAt) {
}
