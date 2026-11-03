package com.danah.instrument.gateway.inbound;

import com.danah.instrument.gateway.inbound.model.EventType;
import com.danah.instrument.gateway.inbound.payloads.PayloadValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PayloadValidatorTest {

    private final PayloadValidator validator = new PayloadValidator();
    private final ObjectMapper mapper = new ObjectMapper();

    private ObjectNode obj() {
        return mapper.createObjectNode();
    }

    @Test
    void detectOk() {
        ObjectNode p = obj().put("itemCode", "TEMP").put("value", 36.5).put("unit", "C");
        assertDoesNotThrow(() -> validator.validate(EventType.DETECT, p));
    }

    @Test
    void detectValueStringOk() {
        ObjectNode p = obj().put("itemCode", "CODE").put("value", "OK");
        assertDoesNotThrow(() -> validator.validate(EventType.DETECT, p));
    }

    @Test
    void detectMissingItemCode() {
        ObjectNode p = obj().put("value", 1);
        assertThrows(IllegalArgumentException.class, () -> validator.validate(EventType.DETECT, p));
    }

    @Test
    void powerOk() {
        ObjectNode p = obj().put("state", "ON");
        assertDoesNotThrow(() -> validator.validate(EventType.POWER, p));
    }

    @Test
    void powerBadState() {
        ObjectNode p = obj().put("state", "MAYBE");
        assertThrows(IllegalArgumentException.class, () -> validator.validate(EventType.POWER, p));
    }

    @Test
    void alarmOk() {
        ObjectNode p = obj().put("level", "CRITICAL").put("code", "E1").put("message", "x").put("cleared", false);
        assertDoesNotThrow(() -> validator.validate(EventType.ALARM, p));
    }

    @Test
    void alarmBadLevel() {
        ObjectNode p = obj().put("level", "LOW").put("code", "E1").put("message", "x");
        assertThrows(IllegalArgumentException.class, () -> validator.validate(EventType.ALARM, p));
    }

    @Test
    void nullPayloadRejected() {
        assertThrows(IllegalArgumentException.class, () -> validator.validate(EventType.DETECT, null));
    }
}
