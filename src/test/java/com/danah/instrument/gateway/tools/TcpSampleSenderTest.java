package com.danah.instrument.gateway.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the wire format produced by {@link TcpSampleSender}. The sender is a manual tool, so
 * these tests are what keeps it honest when the frame limit or fragment envelope changes.
 */
class TcpSampleSenderTest {

    /** Entire frame = 2 + payload must stay below gateway.tcp.max-frame-bytes (1024). */
    private static final int MAX_PAYLOAD = 1021;
    /** Same shape as the tool's msgId: fixed width, lowercase hex. */
    private static final String MSG_ID = "0123456789abcdef";
    private static final String TOKEN = "change-me-local-only";

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void msgIdHasAFixedWidth() {
        String first = TcpSampleSender.nextMsgId();
        String second = TcpSampleSender.nextMsgId();

        assertEquals(TcpSampleSender.MSG_ID_LENGTH, first.length());
        assertEquals(first.length(), second.length(), "the width must never vary");
        assertFalse(first.equals(second), "two records must not share an id");
        assertTrue(first.matches("[0-9a-f]+"), "hex keeps the frame dump readable: " + first);
    }

    @Test
    void envelopeBudgetLeavesAConstantDataCeiling() {
        // 1021 = 60-byte worst-case envelope + 961 bytes of escaped data
        assertEquals(60, TcpSampleSender.MAX_ENVELOPE_BYTES);
        assertEquals(961, TcpSampleSender.MAX_DATA_BYTES);
        assertEquals(MAX_PAYLOAD,
                TcpSampleSender.MAX_ENVELOPE_BYTES + TcpSampleSender.MAX_DATA_BYTES);
    }

    @Test
    void sliceOfTheFullDataBudgetStillFitsOneFrame() {
        // quote-free text costs exactly one byte per character, so 961 of them fill the whole budget
        List<byte[]> frames = TcpSampleSender.frames("a".repeat(3000).getBytes(StandardCharsets.UTF_8),
                TcpSampleSender.nextMsgId(), 4000);

        assertEquals(4, frames.size());
        for (byte[] payload : frames) {
            assertTrue(payload.length <= MAX_PAYLOAD,
                    "a full " + TcpSampleSender.MAX_DATA_BYTES + " B slice must still fit, got "
                            + payload.length + " B");
        }
    }

    @Test
    void recordUnderLimitGoesOutAsSingleFrame() {
        byte[] record = "{\"deviceId\":\"d1\",\"type\":\"POWER\"}".getBytes(StandardCharsets.UTF_8);

        List<byte[]> frames = TcpSampleSender.frames(record, MSG_ID, 700);

        assertEquals(1, frames.size());
        assertArrayEquals(record, frames.get(0), "a fitting record must be sent untouched");
    }

    @Test
    void largeRecordIsSlicedAndReassemblesToTheOriginal() throws IOException {
        byte[] record = repeating(12351);

        List<byte[]> frames = TcpSampleSender.frames(record, MSG_ID, 700);

        assertEquals(18, frames.size(), "ceil(12351 / 700)");
        ByteArrayOutputStream merged = new ByteArrayOutputStream(record.length);
        for (int i = 0; i < frames.size(); i++) {
            byte[] payload = frames.get(i);
            assertTrue(payload.length <= MAX_PAYLOAD,
                    "frame " + (i + 1) + " is " + payload.length + " B, above " + MAX_PAYLOAD);

            JsonNode envelope = mapper.readTree(payload);
            assertEquals(MSG_ID, envelope.get("msgId").asText());
            assertEquals(i + 1, envelope.get("seq").asInt(), "seq is 1-based");
            assertEquals(frames.size(), envelope.get("total").asInt());
            merged.writeBytes(envelope.get("data").asText().getBytes(StandardCharsets.UTF_8));
        }
        assertArrayEquals(record, merged.toByteArray(), "fragments must rebuild the record exactly");
    }

    @Test
    void slicesNeverSplitAMultiByteCharacter() throws IOException {
        String text = "{\"a\":\"中文中文中文\"}".repeat(60);
        byte[] record = text.getBytes(StandardCharsets.UTF_8);
        assertTrue(record.length > MAX_PAYLOAD, "record must be fragmented");

        // chunk=29 is not a multiple of the 3-byte characters, so most cuts land mid-character
        List<byte[]> frames = TcpSampleSender.frames(record, MSG_ID, 29);

        ByteArrayOutputStream merged = new ByteArrayOutputStream(record.length);
        for (byte[] payload : frames) {
            String data = mapper.readTree(payload).get("data").asText();
            assertFalse(data.contains("\uFFFD"), "a slice ended mid-character: " + data);
            merged.writeBytes(data.getBytes(StandardCharsets.UTF_8));
        }
        assertArrayEquals(record, merged.toByteArray(),
                "character-aligned slices must rejoin losslessly");
    }

    @Test
    void escapedDataSurvivesJsonRoundTrip() throws IOException {
        // quotes, a backslash and CJK: every escape a real payload contains, repeated until the
        // record no longer fits one frame so fragmentation is actually exercised
        byte[] record = "{\"a\":\"x\\\"y\\\\z\",\"b\":\"中文\"}".repeat(60)
                .getBytes(StandardCharsets.UTF_8);
        assertTrue(record.length > MAX_PAYLOAD, "record must exceed one frame to be fragmented");

        List<byte[]> frames = TcpSampleSender.frames(record, MSG_ID, 97);

        assertTrue(frames.size() > 10, "chunk=97 must produce many slices, got " + frames.size());
        ByteArrayOutputStream merged = new ByteArrayOutputStream(record.length);
        for (byte[] payload : frames) {
            JsonNode envelope = mapper.readTree(payload);
            merged.writeBytes(envelope.get("data").asText().getBytes(StandardCharsets.UTF_8));
        }
        assertArrayEquals(record, merged.toByteArray(),
                "escaping must be invisible after the envelope is parsed");
    }

    @Test
    void fragmentDataIsReadableTextNotBase64() throws IOException {
        List<byte[]> frames = TcpSampleSender.frames(repeating(2000), MSG_ID, 700);

        String data = mapper.readTree(frames.get(0)).get("data").asText();

        assertTrue(data.contains("\""), "data must hold the raw slice, quotes and all: " + data);
        assertFalse(data.matches("[A-Za-z0-9+/=]*"), "data must not look like base64: " + data);
    }

    @Test
    void chunkAtCeilingStillFitsOneFrame() {
        // ~1.1x escaping: 860 raw bytes plus the envelope stay under the 1021-byte payload ceiling
        List<byte[]> frames = TcpSampleSender.frames(repeating(5000), MSG_ID, 860);

        for (byte[] payload : frames) {
            assertTrue(payload.length <= MAX_PAYLOAD,
                    "860 B slices must stay inside the frame limit, got " + payload.length + " B");
        }
    }

    @Test
    void oversizedChunkShrinksSlicesInsteadOfFailing() throws IOException {
        // 1000 raw bytes would escape past 1021: the slice is cut shorter, never rejected
        assertFramesFitAndRebuild(repeating(5000), 1000);
    }

    @Test
    void controlCharactersShrinkTheSliceToASixth() throws IOException {
        // a raw NUL is not allowed inside a JSON string; it becomes \\u0000, six bytes for one,
        // so a 300-byte target yields roughly 160-byte slices
        assertFramesFitAndRebuild(new byte[3000], 300);
    }

    /** Every frame must fit one payload, and parsing + unescaping them must give the record back. */
    private void assertFramesFitAndRebuild(byte[] record, int chunk) throws IOException {
        List<byte[]> frames = TcpSampleSender.frames(record, MSG_ID, chunk);
        ByteArrayOutputStream merged = new ByteArrayOutputStream(record.length);
        for (byte[] payload : frames) {
            assertTrue(payload.length <= MAX_PAYLOAD,
                    "chunk=" + chunk + " produced a " + payload.length + " B payload");
            merged.writeBytes(mapper.readTree(payload).get("data").asText()
                    .getBytes(StandardCharsets.UTF_8));
        }
        assertArrayEquals(record, merged.toByteArray(),
                "chunk=" + chunk + " must still rebuild the record");
    }

    @Test
    void realGmReportMatchesTheDocumentedSizeAndFrameCount() throws IOException {
        String record = new IngestRecord("GM", "REPORT", "100C200017-20261103232734",
                readResource("/tcp/sample-report-gm.json")).toJson();
        byte[] bytes = record.getBytes(StandardCharsets.UTF_8);

        assertEquals(12245, bytes.length, "12,086 B payload + 159 B envelope");
        assertEquals(17, TcpSampleSender.frames(bytes, MSG_ID, TcpSampleSender.DEFAULT_CHUNK).size());
    }

    @Test
    void realSingleSampleFitsOneFrame() throws IOException {
        String record = new IngestRecord("SAMPLE", "SAMPLE",
                "89860625790032599518-20261104112129",
                readResource("/tcp/sample-result-screen.json")).toJson();
        byte[] bytes = record.getBytes(StandardCharsets.UTF_8);

        assertEquals(604, bytes.length, "521 B payload + 83 B envelope");
        assertEquals(1, TcpSampleSender.frames(bytes, MSG_ID, 700).size());
    }

    @Test
    void authFrameIsTheDocumentedHandshake() throws IOException {
        byte[] frame = TcpSampleSender.authFrame(TOKEN);

        JsonNode body = mapper.readTree(frame);
        assertEquals("AUTH", body.get("type").asText());
        assertEquals(TOKEN, body.get("token").asText());
        assertEquals(2, body.size(), "the gateway reads type and token only");
        assertTrue(frame.length <= MAX_PAYLOAD, "a handshake must fit one frame, got " + frame.length + " B");
    }

    @Test
    void envelopeCarriesIdempotencyKeyAndNoCredential() {
        // idempotencyKey must stay: it is the only thing the downstream can de-duplicate on.
        // deviceId / siteId / occurredAt stay out: the gateway derives or defaults them.
        JsonNode body = readTree(new IngestRecord("x", "POWER", "pump-7-20261104121000",
                "{\"state\":\"ON\"}").toJson());

        assertEquals("POWER", body.get("type").asText());
        assertNull(body.get("token"), "the connection authenticated already: no credential in the record");
        assertEquals("pump-7-20261104121000", body.get("idempotencyKey").asText());
        assertEquals("ON", body.get("payload").get("state").asText());
        assertNull(body.get("deviceId"), "deviceId must not be written");
        assertNull(body.get("siteId"), "siteId must not be written");
        assertNull(body.get("occurredAt"), "occurredAt must not be written");
    }

    @Test
    void resendingTheSameSampleProducesTheSameIdempotencyKey() throws IOException {
        // the whole point of the field: a retry after a timeout must be recognisable
        String payload = readResource("/tcp/sample-report-gm.json");
        IngestRecord first = new IngestRecord("GM", "REPORT",
                "100C200017-20261103232734", payload);
        IngestRecord retry = new IngestRecord("GM", "REPORT",
                "100C200017-20261103232734", payload);

        assertEquals(first.toJson(), retry.toJson(),
                "two sends of the same record must be byte-identical, key included");
        assertEquals(TcpSampleSender.frames(first.toJson().getBytes(StandardCharsets.UTF_8),
                        MSG_ID, 700).size(),
                TcpSampleSender.frames(retry.toJson().getBytes(StandardCharsets.UTF_8),
                        MSG_ID, 700).size());
    }

    @Test
    void handshakeReplyIsReadFromTheSameFraming() throws IOException {
        try (InputStream in = new ByteArrayInputStream(framed("{\"type\":\"AUTH_OK\"}"))) {
            byte[] reply = TcpSampleSender.awaitAuthReply(in);

            assertEquals(TcpSampleSender.AUTH_OK, TcpSampleSender.requireAuthOk(reply));
        }
    }

    @Test
    void aSilentGatewayIsReportedAsAHandshakeTimeoutNotAConnectionFailure() {
        // the port answered and then said nothing: SO_TIMEOUT fires, and that is not "is the
        // gateway running?" — the TCP connection was up the whole time
        InputStream silent = new InputStream() {
            @Override
            public int read() throws IOException {
                throw new SocketTimeoutException("Read timed out");
            }
        };

        TcpSampleSender.HandshakeTimeoutException error = assertThrows(
                TcpSampleSender.HandshakeTimeoutException.class,
                () -> TcpSampleSender.awaitAuthReply(silent));

        assertTrue(error.getMessage().contains(TcpSampleSender.AUTH_REPLY_TIMEOUT_MS + " ms"),
                error.getMessage());
        assertTrue(error.getMessage().contains("no records sent"), error.getMessage());
    }

    @Test
    void authFailAbortsWithTheServerReasonAndSendsNoRecords() throws IOException {
        String json = "{\"type\":\"AUTH_FAIL\",\"reason\":\"invalid token\"}";
        try (InputStream in = new ByteArrayInputStream(framed(json))) {
            byte[] reply = TcpSampleSender.readFrame(in);

            IllegalStateException error = assertThrows(IllegalStateException.class,
                    () -> TcpSampleSender.requireAuthOk(reply));
            assertTrue(error.getMessage().contains("invalid token"), error.getMessage());
            assertTrue(error.getMessage().contains("no records sent"), error.getMessage());
        }
    }

    @Test
    void aConnectionClosedWithoutAnAnswerIsAnError() {
        assertThrows(IllegalStateException.class,
                () -> TcpSampleSender.readFrame(new ByteArrayInputStream(new byte[0])));
    }

    /** One length-prefixed frame, the way the gateway writes a handshake reply. */
    private static byte[] framed(String json) {
        byte[] payload = json.getBytes(StandardCharsets.UTF_8);
        byte[] frame = new byte[payload.length + 2];
        frame[0] = (byte) (payload.length >>> 8);
        frame[1] = (byte) payload.length;
        System.arraycopy(payload, 0, frame, 2, payload.length);
        return frame;
    }

    /** Printable ASCII with the ~10% quote density real JSON carries, so escaping is realistic. */
    private static byte[] repeating(int length) {
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++) {
            data[i] = (byte) (i % 10 == 0 ? '"' : 'a' + i % 26);
        }
        return data;
    }

    private JsonNode readTree(String json) {
        try {
            return mapper.readTree(json);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private String readResource(String path) throws IOException {
        try (InputStream in = getClass().getResourceAsStream(path)) {
            assertNotNull(in, "missing test resource " + path);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8).strip();
        }
    }
}
