package com.danah.instrument.gateway.tcp;

import com.danah.instrument.gateway.auth.ApiKeyAuthenticator;
import com.danah.instrument.gateway.config.TcpProperties;
import com.danah.instrument.gateway.inbound.IngestService;
import com.danah.instrument.gateway.inbound.IngressContext;
import com.danah.instrument.gateway.inbound.model.IngestRequest;
import com.danah.instrument.gateway.inbound.model.Protocol;
import com.danah.instrument.gateway.inbound.tcp.ConnectionAuthHandler;
import com.danah.instrument.gateway.inbound.tcp.LengthPrefixTcpFrameDecoder;
import com.danah.instrument.gateway.inbound.tcp.TcpFragmentAssembler;
import com.danah.instrument.gateway.inbound.tcp.TcpInboundHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Real decoder + real handshake, ingest mocked: proves one fragmented record produces exactly one
 * IngestRequest (and that a record that fits one frame still works).
 */
class TcpInboundHandlerTest {

    private static final String TOKEN = "test-key";
    private static final String RECORD = "{\"deviceId\":\"dev-1\",\"type\":\"POWER\","
            + "\"payload\":{\"state\":\"ON\"}}";

    private final ObjectMapper mapper = new ObjectMapper();

    private IngestService ingestService;
    private ApiKeyAuthenticator authenticator;
    private TcpProperties properties;
    private EmbeddedChannel channel;

    @BeforeEach
    void setUp() {
        ingestService = mock(IngestService.class);
        authenticator = mock(ApiKeyAuthenticator.class);
        when(authenticator.matches(TOKEN)).thenReturn(true);

        properties = new TcpProperties();
        channel = newChannel();
        authenticate();
    }

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    /** A connection without AUTH never reaches ingest, no matter how valid the record is. */
    @Test
    void framesBeforeHandshakeNeverReachIngest() {
        EmbeddedChannel unauthenticated = newChannel();
        try {
            fragment(RECORD.getBytes(StandardCharsets.UTF_8), "msg-auth", 16)
                    .forEach(payload -> send(unauthenticated, payload));

            verify(ingestService, never()).ingest(any(), any(), any());
        } finally {
            unauthenticated.finishAndReleaseAll();
        }
    }

    @Test
    void fragmentsAreMergedIntoOneIngest() {
        List<byte[]> frames = fragment(RECORD.getBytes(StandardCharsets.UTF_8), "msg-1", 16);
        assertTrue(frames.size() > 2, "record must be split into several fragments");

        frames.forEach(this::send);

        ArgumentCaptor<IngestRequest> captor = ArgumentCaptor.forClass(IngestRequest.class);
        verify(ingestService).ingest(captor.capture(), eq(Protocol.TCP), any(IngressContext.class));
        IngestRequest request = captor.getValue();
        assertEquals("dev-1", request.getDeviceId());
        assertEquals("POWER", request.getType());
        assertNull(request.getToken(), "token must be stripped before ingest");
        assertEquals("ON", request.getPayload().get("state").asText());
    }

    @Test
    void outOfOrderFragmentsAreMerged() {
        List<byte[]> frames = fragment(RECORD.getBytes(StandardCharsets.UTF_8), "msg-2", 16);
        int last = frames.size() - 1;

        // send everything except the last fragment, in reverse order
        for (int i = last - 1; i >= 0; i--) {
            send(frames.get(i));
        }
        verify(ingestService, never()).ingest(any(), any(), any());

        send(frames.get(last));

        verify(ingestService).ingest(any(), eq(Protocol.TCP), any(IngressContext.class));
    }

    @Test
    void incompleteFragmentsProduceNoIngest() {
        List<byte[]> frames = fragment(RECORD.getBytes(StandardCharsets.UTF_8), "msg-3", 16);

        send(frames.get(0));
        send(frames.get(1));

        verify(ingestService, never()).ingest(any(), any(), any());
    }

    @Test
    void singleFrameRecordStillWorks() {
        send(RECORD.getBytes(StandardCharsets.UTF_8));

        ArgumentCaptor<IngestRequest> captor = ArgumentCaptor.forClass(IngestRequest.class);
        verify(ingestService).ingest(captor.capture(), eq(Protocol.TCP), any(IngressContext.class));
        assertEquals("dev-1", captor.getValue().getDeviceId());
    }

    @Test
    void recordWithoutDeviceIdStillReachesIngest() {
        // the wire contract does not mandate deviceId: a bare POWER frame must survive
        String record = "{\"type\":\"POWER\",\"payload\":{\"state\":\"ON\"}}";

        send(record.getBytes(StandardCharsets.UTF_8));

        ArgumentCaptor<IngestRequest> captor = ArgumentCaptor.forClass(IngestRequest.class);
        verify(ingestService).ingest(captor.capture(), eq(Protocol.TCP), any(IngressContext.class));
        assertNull(captor.getValue().getDeviceId());
        assertEquals("POWER", captor.getValue().getType());
    }

    @Test
    void fragmentDataIsPlainTextNotBase64() {
        // one frame whose data holds the whole record as text: '{' and '"' are not base64, so the
        // old decoder rejected this frame outright
        List<byte[]> frames = fragment(RECORD.getBytes(StandardCharsets.UTF_8), "msg-4", 4096);

        assertEquals(1, frames.size());
        send(frames.get(0));

        ArgumentCaptor<IngestRequest> captor = ArgumentCaptor.forClass(IngestRequest.class);
        verify(ingestService).ingest(captor.capture(), eq(Protocol.TCP), any(IngressContext.class));
        assertEquals("POWER", captor.getValue().getType());
    }

    @Test
    void malformedFragmentEnvelopeDiscarded() {
        // "\q" is not a legal JSON escape, so the frame cannot be read as an envelope at all
        send("{\"msgId\":\"msg-5\",\"seq\":1,\"total\":1,\"data\":\"bad \\q escape\"}"
                .getBytes(StandardCharsets.UTF_8));

        verify(ingestService, never()).ingest(any(), any(), any());
    }

    private void send(byte[] payload) {
        send(channel, payload);
    }

    private static void send(EmbeddedChannel target, byte[] payload) {
        ByteBuf frame = Unpooled.buffer();
        frame.writeShort(payload.length);
        frame.writeBytes(payload);
        target.writeInbound(frame);
    }

    private EmbeddedChannel newChannel() {
        TcpInboundHandler handler = new TcpInboundHandler(ingestService, mapper, properties,
                new TcpFragmentAssembler(properties));
        return new EmbeddedChannel(
                new LengthPrefixTcpFrameDecoder().newDecoder(properties.getMaxFrameBytes()),
                new ConnectionAuthHandler(authenticator, mapper, properties),
                handler);
    }

    private void authenticate() {
        String auth = "{\"type\":\"AUTH\",\"token\":\"" + TOKEN + "\"}";
        send(auth.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Splits one record into fragment frames the way a client must: on UTF-8 character boundaries,
     * with {@code data} carrying the slice as escaped JSON text. The envelope is written by Jackson
     * rather than by hand, so the gateway is tested against a foreign encoder instead of a copy of
     * {@code TcpSampleSender}.
     */
    private List<byte[]> fragment(byte[] record, String msgId, int chunkSize) {
        List<int[]> ranges = new ArrayList<>();
        int from = 0;
        while (from < record.length) {
            int to = Math.min(from + chunkSize, record.length);
            while (to < record.length && (record[to] & 0xC0) == 0x80) {
                to++; // a continuation byte: never cut inside one character
            }
            ranges.add(new int[]{from, to});
            from = to;
        }

        List<byte[]> frames = new ArrayList<>(ranges.size());
        for (int i = 0; i < ranges.size(); i++) {
            Map<String, Object> envelope = new LinkedHashMap<>();
            envelope.put("msgId", msgId);
            envelope.put("seq", i + 1);
            envelope.put("total", ranges.size());
            envelope.put("data", new String(record, ranges.get(i)[0],
                    ranges.get(i)[1] - ranges.get(i)[0], StandardCharsets.UTF_8));
            try {
                frames.add(mapper.writeValueAsString(envelope).getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return frames;
    }

    @Test
    void fragmentEnvelopeFitsOneFrame() {
        // 740 B is the slice size the README recommends; escaping adds ~24%, the envelope ~60 B
        List<byte[]> big = fragment(jsonLike(1480), "msg-7", 740);

        assertEquals(2, big.size());
        big.forEach(f -> assertTrue(f.length + 2 < properties.getMaxFrameBytes(),
                "frame " + (f.length + 2) + " bytes must stay below " + properties.getMaxFrameBytes()));
    }

    /** ASCII with roughly the quote density of real JSON, so escaping costs what it really costs. */
    private static byte[] jsonLike(int length) {
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++) {
            data[i] = (byte) (i % 10 == 0 ? '"' : 'a' + i % 26);
        }
        return data;
    }

    @Test
    void largeRecordIsReassembledAndIngestedOnce() {
        int items = 12000;
        StringBuilder record = new StringBuilder(40000);
        record.append("{\"deviceId\":\"dev-big\",\"type\":\"DETECT\",")
                .append("\"occurredAt\":\"2026-11-04T10:00:00Z\",")
                .append("\"payload\":{\"itemCode\":\"SPEC\",\"value\":[");
        for (int i = 0; i < items; i++) {
            if (i > 0) {
                record.append(',');
            }
            record.append(i % 100);
        }
        record.append("]}}");

        List<byte[]> frames = fragment(record.toString().getBytes(StandardCharsets.UTF_8), "msg-big", 700);
        assertTrue(frames.size() > 30, "record must span many fragments, got " + frames.size());
        assertTrue(frames.size() <= properties.getAssembly().getMaxFragments(),
                "must stay within max-fragments=" + properties.getAssembly().getMaxFragments()
                        + ", got " + frames.size());

        // reverse order on purpose: reassembly must not depend on arrival order
        for (int i = frames.size() - 1; i >= 0; i--) {
            send(frames.get(i));
        }

        ArgumentCaptor<IngestRequest> captor = ArgumentCaptor.forClass(IngestRequest.class);
        verify(ingestService).ingest(captor.capture(), eq(Protocol.TCP), any(IngressContext.class));
        IngestRequest request = captor.getValue();
        assertEquals("dev-big", request.getDeviceId());
        assertEquals("DETECT", request.getType());
        assertEquals(items, request.getPayload().get("value").size());
        assertEquals(0, request.getPayload().get("value").get(0).asInt());
        assertEquals((items - 1) % 100, request.getPayload().get("value").get(items - 1).asInt());
    }

    @Test
    void realGmReportIsReassembledFromFragments() throws IOException {
        String report;
        try (InputStream in = getClass().getResourceAsStream("/tcp/sample-report-gm.json")) {
            assertNotNull(in, "sample report resource is missing");
            report = new String(in.readAllBytes(), StandardCharsets.UTF_8)
                    .replace("\r\n", "").replace("\n", "").trim();
        }

        // real body is 12086 bytes; the gateway envelope adds a few hundred more
        String record = "{\"deviceId\":\"100C200017\",\"type\":\"REPORT\","
                + "\"occurredAt\":\"2026-11-03T23:27:34Z\",\"siteId\":\"krd\",\"payload\":"
                + report + "}";

        List<byte[]> frames = fragment(record.getBytes(StandardCharsets.UTF_8), "msg-gm-report", 700);
        assertEquals(18, frames.size(), "12KB report must need about 18 fragments at 700B/slice");

        // reverse order on purpose: reassembly must not depend on arrival order
        for (int i = frames.size() - 1; i >= 0; i--) {
            send(frames.get(i));
        }

        ArgumentCaptor<IngestRequest> captor = ArgumentCaptor.forClass(IngestRequest.class);
        verify(ingestService).ingest(captor.capture(), eq(Protocol.TCP), any(IngressContext.class));
        IngestRequest request = captor.getValue();
        assertEquals("100C200017", request.getDeviceId());
        assertEquals("REPORT", request.getType());
        assertEquals("krd", request.getSiteId());

        JsonNode payload = request.getPayload();
        assertEquals(77, payload.get("result").size());
        assertEquals("1", payload.get("result").get(0).get("num").asText());
        assertEquals("113", payload.get("result").get(76).get("num").asText());
        assertEquals("福州艾迪康医学检验", payload.get("hospital").asText(),
                "UTF-8 content must survive slicing and reassembly");
        assertEquals("89860626860062670452", payload.get("iccid").asText());
    }

    @Test
    void shortSampleFitsOneFrameWithoutFragments() throws IOException {
        byte[] raw = sampleRecord();

        assertTrue(raw.length < properties.getMaxFrameBytes() - 2,
                "single sample must fit one frame, got " + raw.length + " bytes");
        send(raw);

        ArgumentCaptor<IngestRequest> captor = ArgumentCaptor.forClass(IngestRequest.class);
        verify(ingestService).ingest(captor.capture(), eq(Protocol.TCP), any(IngressContext.class));
        IngestRequest request = captor.getValue();
        assertEquals("SAMPLE", request.getType());
        assertEquals("屏幕测试6", request.getDeviceId());
        assertEquals("dnk", request.getSiteId());
        assertEquals("-1.000", request.getPayload().get("content").get("concentration").asText());
        assertEquals("其他", request.getPayload().get("content").get("sampleType").asText());
    }

    @Test
    void twoSamplesOnOneConnectionAreIngestedTwice() throws IOException {
        // one connection carries one record per sample; no msgId, no assembler state in between
        byte[] sample = sampleRecord();

        send(sample);
        send(sample);

        ArgumentCaptor<IngestRequest> captor = ArgumentCaptor.forClass(IngestRequest.class);
        verify(ingestService, times(2)).ingest(captor.capture(), eq(Protocol.TCP), any(IngressContext.class));
        assertEquals(2, captor.getAllValues().size());
        assertEquals("屏幕测试6", captor.getAllValues().get(1).getDeviceId());
        assertEquals("dnk", captor.getAllValues().get(1).getSiteId());
    }

    /** Real single-sample message rewritten into the gateway envelope; fits one TCP frame. */
    private byte[] sampleRecord() throws IOException {
        String body;
        try (InputStream in = getClass().getResourceAsStream("/tcp/sample-result-screen.json")) {
            assertNotNull(in, "sample result resource is missing");
            body = new String(in.readAllBytes(), StandardCharsets.UTF_8).trim();
        }

        return ("{\"deviceId\":\"屏幕测试6\",\"type\":\"SAMPLE\","
                + "\"occurredAt\":\"2026-11-04T11:21:29Z\",\"siteId\":\"dnk\",\"payload\":"
                + body + "}").getBytes(StandardCharsets.UTF_8);
    }
}
