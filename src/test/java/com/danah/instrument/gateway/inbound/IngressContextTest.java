package com.danah.instrument.gateway.inbound;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Locks the derivation order. Every case here is a real deployment shape: a smart analyser that
 * knows its serial, a dumb one that only has an install-time key, and one with nothing at all.
 */
class IngressContextTest {

    @Test
    void declaredSourceIdWins() {
        IngressContext ctx = new IngressContext("declared", "bound", "topic", "10.0.0.9");
        assertEquals("declared", ctx.resolveSource("device"));
    }

    @Test
    void boundClientIdBeatsTopicAndDevice() {
        // the operator named this key; that name is more trustworthy than anything the client says
        IngressContext ctx = new IngressContext(null, "krd-lab-01", "topic", "10.0.0.9");
        assertEquals("krd-lab-01", ctx.resolveSource("device"));
    }

    @Test
    void topicSegmentBeatsDeviceId() {
        IngressContext ctx = new IngressContext(null, null, "instr-7", "10.0.0.9");
        assertEquals("instr-7", ctx.resolveSource("device"));
    }

    @Test
    void deviceIdIsUsedWhenNothingElseExists() {
        IngressContext ctx = new IngressContext(null, null, null, "10.0.0.9");
        assertEquals("device", ctx.resolveSource("device"));
    }

    @Test
    void peerAddressIsTheLastResort() {
        IngressContext ctx = new IngressContext(null, null, null, "10.2.3.4");
        assertEquals("10.2.3.4", ctx.resolveSource(null));
    }

    @Test
    void allBlankFallsBackToAnonymous() {
        assertEquals(IngressContext.ANONYMOUS, IngressContext.NONE.resolveSource(null));
        assertEquals(IngressContext.ANONYMOUS, IngressContext.NONE.resolveSource("   "));
    }

    @Test
    void blankHintsAreSkippedNotTreatedAsValues() {
        IngressContext ctx = new IngressContext("  ", "", null, "10.2.3.4");
        assertEquals("10.2.3.4", ctx.resolveSource(null));
    }

    @Test
    void resolvedSourceIsTrimmed() {
        IngressContext ctx = new IngressContext("  ward-3  ", null, null, null);
        assertEquals("ward-3", ctx.resolveSource(null));
    }
}
