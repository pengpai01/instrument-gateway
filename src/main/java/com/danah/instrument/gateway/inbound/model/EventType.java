package com.danah.instrument.gateway.inbound.model;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum EventType {
    DETECT,
    POWER,
    ALARM;

    @JsonCreator
    public static EventType from(String value) {
        if (value == null) {
            return null;
        }
        return EventType.valueOf(value.trim().toUpperCase());
    }
}
