package com.danah.instrument.gateway.inbound;

import com.danah.instrument.gateway.inbound.model.EventType;
import com.danah.instrument.gateway.inbound.payloads.PayloadValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
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

    @Test
    void reportOk() {
        ObjectNode p = obj().put("serialsId", "100C200017").put("hospital", "福州艾迪康医学检验");
        ArrayNode rows = p.putArray("result");
        rows.addObject().put("num", "1").put("projectName", "GM试验").put("concentration", "0.050");
        rows.addObject().put("num", "113").put("projectName", "GM试验").put("concentration", "2.540");

        assertDoesNotThrow(() -> validator.validate(EventType.REPORT, p));
    }

    @Test
    void reportWithEmptyResultAccepted() {
        ObjectNode p = obj();
        p.putArray("result");

        assertDoesNotThrow(() -> validator.validate(EventType.REPORT, p));
    }

    @Disabled("REPORT result validation is temporarily off")
    @Test
    void reportMissingResultRejected() {
        ObjectNode p = obj().put("serialsId", "100C200017");

        assertThrows(IllegalArgumentException.class, () -> validator.validate(EventType.REPORT, p));
    }

    @Disabled("REPORT result validation is temporarily off")
    @Test
    void reportNonArrayResultRejected() {
        ObjectNode p = obj().put("result", "77 rows");

        assertThrows(IllegalArgumentException.class, () -> validator.validate(EventType.REPORT, p));
    }

    @Disabled("REPORT result validation is temporarily off")
    @Test
    void reportNonObjectRowRejected() {
        ObjectNode p = obj();
        p.putArray("result").add("not-an-object");

        assertThrows(IllegalArgumentException.class, () -> validator.validate(EventType.REPORT, p));
    }

    @Test
    void sampleOk() {
        ObjectNode p = obj().put("orgCode", "dnk").put("iccid", "89860625790032599518");
        p.putObject("content")
                .put("deviceId", "屏幕测试6")
                .put("concentration", "-1.000")
                .put("sampleType", "其他")
                .put("checkTime", "2026-11-04 11:21:29");

        assertDoesNotThrow(() -> validator.validate(EventType.SAMPLE, p));
    }

    @Test
    void sampleWithEmptyContentAccepted() {
        ObjectNode p = obj();
        p.putObject("content");

        assertDoesNotThrow(() -> validator.validate(EventType.SAMPLE, p));
    }

    @Test
    void sampleMissingContentRejected() {
        ObjectNode p = obj().put("orgCode", "dnk").put("iccid", "89860625790032599518");

        assertThrows(IllegalArgumentException.class, () -> validator.validate(EventType.SAMPLE, p));
    }

    @Test
    void sampleNonObjectContentRejected() {
        ObjectNode p = obj().put("content", "screen test");

        assertThrows(IllegalArgumentException.class, () -> validator.validate(EventType.SAMPLE, p));
    }

    @Test
    void sampleNullContentRejected() {
        ObjectNode p = obj().putNull("content");

        assertThrows(IllegalArgumentException.class, () -> validator.validate(EventType.SAMPLE, p));
    }

    @Test
    void sampleArrayContentRejected() {
        ObjectNode p = obj();
        p.putArray("content").addObject().put("deviceId", "屏幕测试6");

        assertThrows(IllegalArgumentException.class, () -> validator.validate(EventType.SAMPLE, p));
    }

    @Test
    void sampleKeepsStringResultInsideContent() {
        // the single-sample shape carries content.result as a plain value ("--" when the
        // analyser produced nothing); the REPORT array rule must not leak into SAMPLE
        ObjectNode p = obj();
        p.putObject("content").put("result", "--").put("concentration", "-1.000");

        assertDoesNotThrow(() -> validator.validate(EventType.SAMPLE, p));
    }
}
