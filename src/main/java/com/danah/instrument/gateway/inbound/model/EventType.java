package com.danah.instrument.gateway.inbound.model;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum EventType {
    DETECT,
    POWER,
    ALARM,
    /** A lab instrument report: one header plus a rows array (e.g. GM test results). */
    REPORT,
    /** A single-sample analyser result: header at top level, measurement values in {@code content}. */
    SAMPLE;

    @JsonCreator
    public static EventType from(String value) {
        if (value == null) {
            return null;
        }
        return EventType.valueOf(value.trim().toUpperCase());
    }
}
