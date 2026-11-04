package com.danah.instrument.gateway.inbound.mqtt;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class MqttTopicSourceTest {

    @Test
    void extractsSecondSegment() {
        assertEquals("dev-1", MqttInboundConfig.extractSourceSegment("instr/dev-1/up"));
    }

    @Test
    void shortTopic() {
        assertNull(MqttInboundConfig.extractSourceSegment("instr"));
    }
}
