package com.danah.instrument.gateway.inbound.payloads;

import com.danah.instrument.gateway.inbound.model.EventType;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Component;

import java.util.Locale;
import java.util.Set;

/**
 * Validates DETECT / POWER / ALARM payload shapes. Field names must not change.
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

    private String requireText(JsonNode p, String field) {
        JsonNode n = p.get(field);
        if (n == null || n.isNull() || !n.isTextual() || n.asText().isBlank()) {
            throw new IllegalArgumentException("payload." + field + " is required");
        }
        return n.asText();
    }
}
