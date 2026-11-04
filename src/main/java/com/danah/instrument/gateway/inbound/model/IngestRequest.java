package com.danah.instrument.gateway.inbound.model;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Map;

/**
 * Raw ingest body shared by HTTP / TCP / MQTT (token stripped before Kafka).
 */
public class IngestRequest {

    /**
     * Optional ingress identity. Only needed when the client cannot supply {@link #deviceId}
     * and the transport carries no usable hint of its own (plain HTTP/TCP).
     */
    private String sourceId;
    /** Optional business identifier; many analysers cannot report one, so it must stay optional. */
    private String deviceId;
    private String type;
    private String occurredAt;
    private JsonNode payload;
    private String siteId;
    private String idempotencyKey;
    private Map<String, Object> ext;
    /** Auth token for TCP/MQTT; never written to Kafka. */
    private String token;

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getOccurredAt() {
        return occurredAt;
    }

    public void setOccurredAt(String occurredAt) {
        this.occurredAt = occurredAt;
    }

    public JsonNode getPayload() {
        return payload;
    }

    public void setPayload(JsonNode payload) {
        this.payload = payload;
    }

    public String getSiteId() {
        return siteId;
    }

    public void setSiteId(String siteId) {
        this.siteId = siteId;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public Map<String, Object> getExt() {
        return ext;
    }

    public void setExt(Map<String, Object> ext) {
        this.ext = ext;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }
}
