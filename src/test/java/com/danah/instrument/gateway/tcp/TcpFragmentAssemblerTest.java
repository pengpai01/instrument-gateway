package com.danah.instrument.gateway.tcp;

import com.danah.instrument.gateway.config.TcpProperties;
import com.danah.instrument.gateway.inbound.tcp.TcpFragmentAssembler;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class TcpFragmentAssemblerTest {

    private TcpFragmentAssembler assembler;
    private EmbeddedChannel channel;

    @BeforeEach
    void setUp() {
        assembler = new TcpFragmentAssembler(new TcpProperties());
        channel = new EmbeddedChannel();
    }

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(byte[] raw) {
        return new String(raw, StandardCharsets.UTF_8);
    }

    @Test
    void reassemblesFragmentsArrivingOutOfOrder() {
        assertNull(assembler.accept(channel, "m1", 3, 3, bytes("\"v\":1}")));
        assertNull(assembler.accept(channel, "m1", 1, 3, bytes("{\"deviceId\":\"d1\",")));

        byte[] done = assembler.accept(channel, "m1", 2, 3, bytes("\"type\":\"DETECT\","));

        assertNotNull(done);
        assertEquals("{\"deviceId\":\"d1\",\"type\":\"DETECT\",\"v\":1}", text(done));
    }

    @Test
    void singleFragmentRecordCompletesImmediately() {
        byte[] done = assembler.accept(channel, "m2", 1, 1, bytes("{\"a\":1}"));

        assertNotNull(done);
        assertEquals("{\"a\":1}", text(done));
    }

    @Test
    void duplicateFragmentKeepsFirstCopy() {
        assertNull(assembler.accept(channel, "m3", 1, 2, bytes("A")));
        assertNull(assembler.accept(channel, "m3", 1, 2, bytes("X")));

        byte[] done = assembler.accept(channel, "m3", 2, 2, bytes("B"));

        assertNotNull(done);
        assertEquals("AB", text(done));
    }

    @Test
    void seqOutOfRangeRejected() {
        assertNull(assembler.accept(channel, "m4", 0, 3, bytes("A")));
        assertNull(assembler.accept(channel, "m4", 4, 3, bytes("D")));
        assertNull(assembler.accept(channel, "m4", 1, 0, bytes("A")));
    }

    @Test
    void blankMsgIdRejected() {
        assertNull(assembler.accept(channel, " ", 1, 2, bytes("A")));
        assertNull(assembler.accept(channel, null, 1, 2, bytes("A")));
    }

    @Test
    void totalMismatchDropsRecord() {
        assertNull(assembler.accept(channel, "m5", 1, 3, bytes("A")));
        assertNull(assembler.accept(channel, "m5", 2, 9, bytes("B")));

        // previous parts were discarded, so a later fragment cannot complete the record
        assertNull(assembler.accept(channel, "m5", 2, 3, bytes("B")));
        assertNull(assembler.accept(channel, "m5", 3, 3, bytes("C")));
    }

    @Test
    void tooManyFragmentsRejected() {
        TcpProperties props = new TcpProperties();
        props.getAssembly().setMaxFragments(2);
        TcpFragmentAssembler limited = new TcpFragmentAssembler(props);

        assertNull(limited.accept(channel, "m6", 1, 3, bytes("A")));
    }

    @Test
    void assemblesRecordBeyondLegacyFragmentLimit() {
        int total = 100;                 // above the old max-fragments=64
        StringBuilder expected = new StringBuilder(total * 2);
        byte[] done = null;

        for (int seq = 1; seq <= total; seq++) {
            String part = String.format("%02d", seq);
            expected.append(part);
            done = assembler.accept(channel, "m-big", seq, total, bytes(part));
            if (seq < total) {
                assertNull(done, "record must stay incomplete until the last fragment");
            }
        }

        assertNotNull(done);
        assertEquals(expected.toString(), text(done));
    }

    @Test
    void fragmentCountAboveLimitRejected() {
        int overLimit = new TcpProperties().getAssembly().getMaxFragments() + 1;

        assertNull(assembler.accept(channel, "m-over", 1, overLimit, bytes("A")));
    }

    @Test
    void oversizedRecordDropped() {
        TcpProperties props = new TcpProperties();
        props.getAssembly().setMaxBytes(4);
        TcpFragmentAssembler limited = new TcpFragmentAssembler(props);

        assertNull(limited.accept(channel, "m7", 1, 2, bytes("AAA")));
        assertNull(limited.accept(channel, "m7", 2, 2, bytes("BBB")));
    }

    @Test
    void pendingLimitPerChannelDropped() {
        TcpProperties props = new TcpProperties();
        props.getAssembly().setMaxPendingPerChannel(1);
        TcpFragmentAssembler limited = new TcpFragmentAssembler(props);

        assertNull(limited.accept(channel, "a", 1, 2, bytes("A")));
        assertNull(limited.accept(channel, "b", 1, 2, bytes("B")));

        byte[] done = limited.accept(channel, "a", 2, 2, bytes("B"));
        assertNotNull(done);
        assertEquals("AB", text(done));
    }

    @Test
    void discardClearsConnectionState() {
        assertNull(assembler.accept(channel, "m8", 1, 2, bytes("A")));

        assembler.discard(channel);

        assertNull(assembler.accept(channel, "m8", 2, 2, bytes("B")));
    }

    @Test
    void stateIsIsolatedPerChannel() {
        EmbeddedChannel other = new EmbeddedChannel();
        try {
            assertNull(assembler.accept(channel, "m9", 1, 2, bytes("A")));
            assertNull(assembler.accept(other, "m9", 2, 2, bytes("B")));

            byte[] done = assembler.accept(channel, "m9", 2, 2, bytes("C"));
            assertNotNull(done);
            assertEquals("AC", text(done));
        } finally {
            other.finishAndReleaseAll();
        }
    }
}
