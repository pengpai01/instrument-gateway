package com.danah.instrument.gateway.tcp;

import com.danah.instrument.gateway.config.TcpProperties;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the frame limits against drifting apart.
 * <p>
 * {@code max-bytes} is only reachable when {@code max-fragments} can physically carry it:
 * every fragment must fit into one frame after JSON escaping + envelope overhead. Lowering
 * {@code max-fragments} or raising {@code max-bytes} alone would silently cap records at a
 * size nobody documented.
 */
class TcpFrameCapacityTest {

    /** Slice size recommended to clients in the README; keep in step with the sender's default. */
    private static final int RECOMMENDED_SLICE = 740;

    /**
     * Worst-case expansion of a slice written as escaped JSON text: every quote and backslash
     * becomes two bytes. Real report JSON sits around 1.24x, so 1.25 leaves a little margin.
     */
    private static final double ESCAPE_FACTOR = 1.25;

    @Test
    void fragmentLimitCoversDeclaredMaxBytes() {
        TcpProperties props = new TcpProperties();
        TcpProperties.Assembly assembly = props.getAssembly();

        int maxChunk = maxChunkBytes(props.getMaxFrameBytes(), assembly.getMaxFragments());
        int reachable = assembly.getMaxFragments() * maxChunk;

        assertTrue(reachable >= assembly.getMaxBytes(),
                "max-fragments=" + assembly.getMaxFragments() + " x " + maxChunk + "B/fragment = "
                        + reachable + "B, cannot reach max-bytes=" + assembly.getMaxBytes()
                        + "B; raise max-fragments or lower max-bytes");
    }

    @Test
    void recommendedSliceFitsWithinFragmentLimit() {
        TcpProperties props = new TcpProperties();
        TcpProperties.Assembly assembly = props.getAssembly();

        int reachable = assembly.getMaxFragments() * RECOMMENDED_SLICE;

        assertTrue(reachable >= assembly.getMaxBytes(),
                "recommended " + RECOMMENDED_SLICE + "B slices x " + assembly.getMaxFragments()
                        + " fragments = " + reachable + "B, cannot reach max-bytes="
                        + assembly.getMaxBytes() + "B");
    }

    @Test
    void worstCaseFragmentFrameStaysUnderFrameLimit() {
        TcpProperties props = new TcpProperties();
        int total = props.getAssembly().getMaxFragments();
        int maxChunk = maxChunkBytes(props.getMaxFrameBytes(), total);

        int frameBytes = 2 + envelopeBytes(total) + escapedLength(maxChunk);

        assertTrue(frameBytes < props.getMaxFrameBytes(),
                "largest fragment frame is " + frameBytes + "B, must stay < "
                        + props.getMaxFrameBytes() + "B");
    }

    /** Largest raw slice one frame can carry for a record split into {@code totalFragments} parts. */
    static int maxChunkBytes(int maxFrameBytes, int totalFragments) {
        int payloadRoom = maxFrameBytes - 2 - 1;            // entire frame must stay < maxFrameBytes
        int roomForData = payloadRoom - envelopeBytes(totalFragments);
        return (int) Math.floor(roomForData / ESCAPE_FACTOR);
    }

    /** Envelope with an empty data field, worst-case 16-char msgId and full-width seq/total. */
    static int envelopeBytes(int totalFragments) {
        String envelope = "{\"msgId\":\"0123456789abcdef\",\"seq\":" + totalFragments
                + ",\"total\":" + totalFragments + ",\"data\":\"\"}";
        return envelope.getBytes(StandardCharsets.UTF_8).length;
    }

    /** Bytes a raw slice occupies once quotes and backslashes are escaped. */
    static int escapedLength(int rawBytes) {
        return (int) Math.ceil(rawBytes * ESCAPE_FACTOR);
    }
}
