package com.danah.instrument.gateway.inbound.payloads;

import com.danah.instrument.gateway.inbound.model.EventType;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Component;

import java.util.Locale;
import java.util.Set;

/**
 * Validates DETECT / POWER / ALARM / REPORT / SAMPLE payload shapes. Field names must not change.
 */
@Component
public class PayloadValidator {

    private static final Set<String> POWER_STATES = Set.of("ON", "OFF");
    private static final Set<String> ALARM_LEVELS = Set.of("INFO", "WARN", "CRITICAL");

    public void validate(EventType type, JsonNode payload) {
        if (payload == null || payload.isNull() || !payload.isObject()) {
            throw new IllegalArgumentException("payload must be a JSON object");
        }
        switch (type) {
            case DETECT -> validateDetect(payload);
            case POWER -> validatePower(payload);
            case ALARM -> validateAlarm(payload);
            case REPORT -> validateReport(payload);
            case SAMPLE -> validateSample(payload);
            default -> throw new IllegalArgumentException("unknown type: " + type);
        }
    }

    private void validateDetect(JsonNode p) {
        requireText(p, "itemCode");
        JsonNode value = p.get("value");
        if (value == null || value.isNull()) {
            throw new IllegalArgumentException("DETECT.payload.value is required");
        }
        if (!(value.isNumber() || value.isTextual())) {
            throw new IllegalArgumentException("DETECT.payload.value must be number or string");
        }
        // unit?, raw? optional
    }

    private void validatePower(JsonNode p) {
        String state = requireText(p, "state").toUpperCase(Locale.ROOT);
        if (!POWER_STATES.contains(state)) {
            throw new IllegalArgumentException("POWER.payload.state must be ON or OFF");
        }
        // reason? optional
    }

    private void validateAlarm(JsonNode p) {
        String level = requireText(p, "level").toUpperCase(Locale.ROOT);
        if (!ALARM_LEVELS.contains(level)) {
            throw new IllegalArgumentException("ALARM.payload.level must be INFO|WARN|CRITICAL");
        }
        requireText(p, "code");
        requireText(p, "message");
        // cleared? optional bool
        if (p.has("cleared") && !p.get("cleared").isNull() && !p.get("cleared").isBoolean()) {
            throw new IllegalArgumentException("ALARM.payload.cleared must be boolean when present");
        }
    }

    /**
     * Instrument report: the header stays free-form. Result validation is disabled for now;
     * re-enable the commented checks once the report shape is agreed.
     */
    private void validateReport(JsonNode p) {
//        JsonNode result = p.get("result");
//        if (result == null || result.isNull()) {
//            throw new IllegalArgumentException("REPORT.payload.result is required");
//        }
//        if (!result.isArray()) {
//            throw new IllegalArgumentException("REPORT.payload.result must be an array");
//        }
//        for (JsonNode row : result) {
//            if (!row.isObject()) {
//                throw new IllegalArgumentException("REPORT.payload.result items must be objects");
//            }
//        }
    }

    /**
     * Single-sample analyser result (one measurement per message): the header stays at the top
     * level and the measurement values sit in {@code content}, which must be a JSON object.
     */
    private void validateSample(JsonNode p) {
        JsonNode content = p.get("content");
        if (content == null || content.isNull()) {
            throw new IllegalArgumentException("SAMPLE.payload.content is required");
        }
        if (!content.isObject()) {
            throw new IllegalArgumentException("SAMPLE.payload.content must be an object");
        }
    }

    private String requireText(JsonNode p, String field) {
        JsonNode n = p.get(field);
        if (n == null || n.isNull() || !n.isTextual() || n.asText().isBlank()) {
            throw new IllegalArgumentException("payload." + field + " is required");
        }
        return n.asText();
    }
}
