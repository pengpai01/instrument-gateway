package com.danah.instrument.gateway.inbound.mqtt;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class MqttTopicDeviceIdTest {

    @Test
    void extractsSecondSegment() {
        assertEquals("dev-1", MqttInboundConfig.extractDeviceId("instr/dev-1/up"));
    }

    @Test
    void shortTopic() {
        assertNull(MqttInboundConfig.extractDeviceId("instr"));
    }
}
