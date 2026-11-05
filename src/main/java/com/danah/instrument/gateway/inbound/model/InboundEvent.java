package com.danah.instrument.gateway.inbound.model;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Kafka message body.
 *
 * <p>Carries the analyser payload and nothing else: {@code EventKafkaProducer} serialises
 * {@link #getPayload()}, so the topic value is exactly the JSON the instrument sent, with no
 * gateway envelope around it.
 *
 * <p>{@code eventId}, {@code sourceId}, {@code deviceId}, {@code type}, {@code occurredAt},
 * {@code receivedAt}, {@code protocol}, {@code siteId}, {@code idempotencyKey} and {@code ext}
 * were dropped on purpose while the downstream contract stays minimal. Identity and the
 * de-duplication key did not disappear — they travel as the Kafka record key and header instead,
 * which is why they must not be re-added here without also changing the producer.
 */
public class InboundEvent {

    private JsonNode payload;

    public JsonNode getPayload() {
        return payload;
    }

    public void setPayload(JsonNode payload) {
        this.payload = payload;
    }
}
