package com.danah.instrument.gateway.tools;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simulates one analyser client sending the two real samples to the gateway over TCP.
 *
 * <p>Wire protocol (must match {@code LengthPrefixTcpFrameDecoder}): a frame is a 2-byte
 * big-endian payload length followed by that many UTF-8 bytes, and the whole frame must stay
 * below {@code gateway.tcp.max-frame-bytes} (1024), so one payload carries at most 1021 bytes.
 *
 * <p>A record that does not fit is sliced into {@code {"msgId","seq","total","data"}} envelopes
 * (740 raw bytes per slice by default) and reassembled by the gateway. {@code data} carries the
 * slice as escaped JSON text, not base64: a text dump stays readable and no 33% base64 inflation is
 * paid, at the cost of about 24% from JSON escaping. Slices are cut on UTF-8 character boundaries
 * so a multi-byte character is never split across two frames.
 *
 * <p>A frame must stay under 1024 B, so the slice size is not fixed: when escaping would push an
 * envelope past {@link #MAX_PAYLOAD}, that slice is cut shorter. A quote-heavy record therefore
 * sends more, smaller frames instead of failing.
 *
 * <p>The gateway does not answer: success is confirmed by its log line
 * {@code ingested ... type=REPORT} and by the Kafka consumer, not by the socket.
 *
 * <p>Lives under {@code src/test/java} on purpose: it is compiled by {@code mvn test-compile}
 * (so it cannot rot) but never packaged into the jar. Run it from the project root:
 * <pre>
 *   java src/test/java/com/danah/instrument/gateway/tools/TcpSampleSender.java --dry-run
 *   java -cp target/test-classes com.danah.instrument.gateway.tools.TcpSampleSender --dry-run
 * </pre>
 */
public final class TcpSampleSender {

    /** {@code gateway.tcp.max-frame-bytes}. */
    private static final int MAX_FRAME_BYTES = 1024;
    /** Entire frame = 2 + payload must be &lt; 1024, so payload &lt;= 1021. */
    private static final int MAX_PAYLOAD = MAX_FRAME_BYTES - 3;
    /**
     * Target raw bytes per slice. Real report JSON escapes at roughly 1.24x (every quote and
     * backslash doubles), so 740 raw bytes land near 918 on the wire; with a 60-byte envelope and
     * the 2-byte header that is 980 of the 1024 a frame may use. This is a target, not a limit:
     * slices shrink on their own when escaping would overflow a frame. Package-private so
     * {@code TcpSampleSenderTest} asserts against the real default.
     */
    static final int DEFAULT_CHUNK = 740;
    /** {@code gateway.tcp.assembly.max-fragments}: the gateway drops a record split into more. */
    private static final int MAX_FRAGMENTS = 128;
    /**
     * Fixed msgId width. A constant id length, together with {@code seq}/{@code total} never
     * exceeding {@link #MAX_FRAGMENTS}'s three digits, is what makes every envelope a known size —
     * and therefore every slice's {@code data} a known maximum.
     */
    static final int MSG_ID_LENGTH = 16;
    /** Widest envelope: 16-char msgId, 3-digit seq and total, empty data. */
    static final int MAX_ENVELOPE_BYTES = envelopeBytes("0".repeat(MSG_ID_LENGTH));
    /** Escaped bytes one slice may carry: the payload ceiling minus the fixed envelope budget. */
    static final int MAX_DATA_BYTES = MAX_PAYLOAD - MAX_ENVELOPE_BYTES;

    /** Seeded from the clock so ids differ between runs; the width is what matters. */
    private static final AtomicLong MSG_ID_COUNTER = new AtomicLong(System.currentTimeMillis());

    public static void main(String[] args) throws Exception {
        Options opt;
        try {
            opt = Options.parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage());
            System.exit(2);
            return;
        }
        if (opt.help) {
            Options.printUsage();
            return;
        }

        try {
            run(opt);
        } catch (IllegalArgumentException | IllegalStateException e) {
            System.err.println("error: " + e.getMessage());
            System.exit(2);
        } catch (IOException e) {
            System.err.println("connection failed: " + e.getMessage());
            System.err.println("is the gateway running? check gateway.tcp.port=9100 and --token");
            System.exit(3);
        }
    }

    private static void run(Options opt) throws IOException {
        Path dataDir = Path.of(opt.dataDir);
        String reportJson = readJson(dataDir.resolve("sample-report-gm.json"));
        String sampleJson = readJson(dataDir.resolve("sample-result-screen.json"));

        List<IngestRecord> records = List.of(
                new IngestRecord("REPORT  GM report, 77 rows", "REPORT",
                        "100C200017-20261103232734", reportJson),
                // iccid rather than content.deviceId: ASCII, stable across firmware locales
                new IngestRecord("SAMPLE  single result, 1 row", "SAMPLE",
                        "89860625790032599518-20261104112129", sampleJson));

        System.out.printf("target   %s:%d  (%s)%n", opt.host, opt.port, opt.dryRun ? "dry run" : "sending");
        System.out.printf("protocol frame = 2-byte big-endian length + UTF-8 JSON, frame < %d B, payload <= %d B%n%n",
                MAX_FRAME_BYTES, MAX_PAYLOAD);

        long wireBytes = 0;
        int frameCount = 0;
        try (Socket socket = opt.dryRun ? null : connect(opt)) {
            OutputStream out = socket == null ? null : socket.getOutputStream();

            int index = 1;
            for (IngestRecord entry : records) {
                byte[] record = entry.toJson(opt.token).getBytes(StandardCharsets.UTF_8);
                // fixed 16 hex chars: the envelope size must not depend on the type name
                String msgId = nextMsgId();
                List<byte[]> frames = frames(record, msgId, opt.chunk);

                System.out.printf("[%d/%d] %s%n", index++, records.size(), entry.label());
                System.out.printf("       type=%s  idempotencyKey=%s%n",
                        entry.type(), entry.idempotencyKey());
                System.out.printf("       record=%d B  frames=%d%s%n",
                        record.length, frames.size(),
                        frames.size() == 1 ? " (single frame, no fragmentation)" : "");

                for (int i = 0; i < frames.size(); i++) {
                    byte[] payload = frames.get(i);
                    wireBytes += 2L + payload.length;
                    frameCount++;

                    if (i == 2 && frames.size() > 3) {
                        System.out.printf("       ... %d more frame(s)%n", frames.size() - 3);
                    }
                    if (i < 2 || i == frames.size() - 1) {
                        System.out.printf("       frame %2d/%d  len=0x%04X  wire=%4d B  head=%s%n",
                                i + 1, frames.size(), payload.length, 2 + payload.length,
                                hex(payload, 12));
                    }
                    if (out != null) {
                        writeFrame(out, payload);
                    }
                }
                if (out != null) {
                    out.flush();
                }
                System.out.println();
            }
        }

        System.out.printf("%s: %d record(s), %d frame(s), %d bytes on the wire%n",
                opt.dryRun ? "dry run complete" : "sent", records.size(), frameCount, wireBytes);
        if (!opt.dryRun) {
            System.out.println("no ACK in this protocol: check the gateway log and the Kafka consumer");
        }
    }

    /**
     * Splits one JSON record into frame payloads.
     *
     * <p>A record that fits {@link #MAX_PAYLOAD} goes out as-is, which keeps the legacy
     * single-frame format. Otherwise every slice becomes a fragment envelope; {@code data} holds
     * the slice as escaped JSON text, {@code seq} is 1-based, {@code seq == total} marks the end.
     *
     * <p>{@code chunk} is a target, not a promise. Each slice is cut so that its escaped envelope
     * stays at or below {@link #MAX_PAYLOAD}: quote-heavy text costs two bytes per quote and a
     * control character costs six, so such a record yields more, smaller slices. The frame limit is
     * the constraint that must hold; the slice size is only how we get there.
     *
     * <p>The envelope has a fixed budget: {@code msgId} is exactly {@link #MSG_ID_LENGTH} characters
     * and {@code seq}/{@code total} fit in three digits ({@link #MAX_FRAGMENTS} is 128), so a slice
     * carries at most {@link #MAX_DATA_BYTES} escaped bytes and no frame can outgrow a payload.
     *
     * <p>Slices are cut on UTF-8 character boundaries: cutting mid-character would put an invalid
     * byte sequence in a fragment, and the reassembled record would no longer parse.
     *
     * <p>Package-private so {@code TcpSampleSenderTest} can guard the wire format.
     */
    static List<byte[]> frames(byte[] record, String msgId, int chunk) {
        if (chunk < 1) {
            throw new IllegalArgumentException("chunk must be at least 1, got " + chunk);
        }
        if (record.length <= MAX_PAYLOAD) {
            return List.of(record);
        }
        if (msgId.length() != MSG_ID_LENGTH) {
            throw new IllegalArgumentException("msgId must be " + MSG_ID_LENGTH
                    + " characters so the envelope keeps a known size, got " + msgId.length());
        }

        List<String> slices = slices(new String(record, StandardCharsets.UTF_8), chunk);
        if (slices.size() > MAX_FRAGMENTS) {
            throw new IllegalArgumentException("record needs " + slices.size()
                    + " fragments, the gateway accepts at most " + MAX_FRAGMENTS
                    + "; raise --chunk above " + chunk);
        }

        int total = slices.size();
        List<byte[]> frames = new ArrayList<>(total);
        for (int i = 0; i < total; i++) {
            String envelope = "{\"msgId\":\"" + msgId + "\",\"seq\":" + (i + 1)
                    + ",\"total\":" + total + ",\"data\":\"" + escapeJson(slices.get(i)) + "\"}";
            byte[] payload = envelope.getBytes(StandardCharsets.UTF_8);
            if (payload.length > MAX_PAYLOAD) {
                // unreachable: slices are cut against MAX_DATA_BYTES, a worst-case envelope. Kept so
                // a future change to either constant fails here instead of on the wire.
                throw new IllegalStateException("envelope " + payload.length
                        + " B exceeds the " + MAX_PAYLOAD + " B payload ceiling");
            }
            frames.add(payload);
        }
        return frames;
    }

    /**
     * Next record id: exactly {@link #MSG_ID_LENGTH} lowercase hex characters. Fixed width on
     * purpose — the envelope size must not depend on the id, and the event type is printed next to
     * it anyway.
     */
    static String nextMsgId() {
        return String.format(Locale.ROOT, "%0" + MSG_ID_LENGTH + "x", MSG_ID_COUNTER.incrementAndGet());
    }

    /**
     * Greedy slices: each at most {@code chunk} raw bytes and at most {@link #MAX_DATA_BYTES}
     * escaped bytes, so every slice fits the fixed envelope budget.
     */
    private static List<String> slices(String text, int chunk) {
        int capacity = MAX_DATA_BYTES;

        List<String> slices = new ArrayList<>();
        int from = 0;
        while (from < text.length()) {
            int escaped = 0;
            int raw = 0;
            int i = from;
            while (i < text.length()) {
                int cp = text.codePointAt(i);
                int escapedBytes = escapedLength(cp);
                int rawBytes = utf8Length(cp);
                if (escaped + escapedBytes > capacity || raw + rawBytes > chunk) {
                    break;
                }
                escaped += escapedBytes;
                raw += rawBytes;
                i += Character.charCount(cp);
            }
            if (i == from) {
                // only reachable through the raw budget: one character is wider than chunk. Take it
                // anyway — a one-character slice still fits a frame, an empty one would loop forever
                i += Character.charCount(text.codePointAt(from));
            }
            slices.add(text.substring(from, i));
            from = i;
        }
        return slices;
    }

    /** Bytes one code point costs inside an escaped JSON string; 6 is the worst case ({@code \\u00XX}). */
    private static int escapedLength(int cp) {
        return switch (cp) {
            case '"', '\\', '\n', '\r', '\t', '\b', '\f' -> 2;
            default -> cp < 0x20 ? 6 : utf8Length(cp);
        };
    }

    /** UTF-8 length of one code point, without allocating a byte array. */
    private static int utf8Length(int cp) {
        if (cp < 0x80) {
            return 1;
        }
        if (cp < 0x800) {
            return 2;
        }
        return cp < 0x10000 ? 3 : 4;
    }

    /** Envelope bytes with an empty data field and the widest seq/total the gateway accepts. */
    private static int envelopeBytes(String msgId) {
        String widest = "9".repeat(String.valueOf(MAX_FRAGMENTS).length());
        String envelope = "{\"msgId\":\"" + msgId + "\",\"seq\":" + widest
                + ",\"total\":" + widest + ",\"data\":\"\"}";
        return envelope.getBytes(StandardCharsets.UTF_8).length;
    }

    /** JSON string escaping; control characters become {@code \\uXXXX}. */
    static String escapeJson(String text) {
        StringBuilder out = new StringBuilder(text.length() + 16);
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                case '\b' -> out.append("\\b");
                case '\f' -> out.append("\\f");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.toString();
    }

    private static void writeFrame(OutputStream out, byte[] payload) throws IOException {
        out.write((payload.length >>> 8) & 0xFF);
        out.write(payload.length & 0xFF);
        out.write(payload);
    }

    private static Socket connect(Options opt) throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(opt.host, opt.port), 3000);
        socket.setTcpNoDelay(true);
        return socket;
    }

    private static String readJson(Path file) throws IOException {
        if (!Files.isRegularFile(file)) {
            throw new IOException("sample not found: " + file.toAbsolutePath());
        }
        return Files.readString(file, StandardCharsets.UTF_8).strip();
    }

    private static String hex(byte[] data, int limit) {
        int shown = Math.min(limit, data.length);
        StringBuilder sb = new StringBuilder(shown * 3);
        for (int i = 0; i < shown; i++) {
            if (i > 0) {
                sb.append(' ');
            }
            sb.append(String.format(Locale.ROOT, "%02x", data[i] & 0xFF));
        }
        if (data.length > shown) {
            sb.append(" ...");
        }
        return sb.toString();
    }

    private static final class Options {

        String host = env("GATEWAY_TCP_HOST", "127.0.0.1");
        int port = Integer.parseInt(env("GATEWAY_TCP_PORT", "9100"));
        String token = env("GATEWAY_API_KEY", "change-me-local-only");
        int chunk = DEFAULT_CHUNK;
        String dataDir = "src/test/resources/tcp";
        boolean dryRun;
        boolean help;

        static Options parse(String[] args) {
            Options o = new Options();
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                int eq = arg.indexOf('=');
                String name = eq > 0 ? arg.substring(0, eq) : arg;

                if ("--dry-run".equals(name)) {
                    o.dryRun = true;
                    continue;
                }
                if ("--help".equals(name) || "-h".equals(name)) {
                    o.help = true;
                    continue;
                }

                String value = eq > 0 ? arg.substring(eq + 1) : null;
                if (value == null && i + 1 < args.length && !args[i + 1].startsWith("--")) {
                    value = args[++i];
                }
                if (value == null) {
                    throw new IllegalArgumentException("missing value for " + name);
                }

                switch (name) {
                    case "--host" -> o.host = value;
                    case "--port" -> o.port = Integer.parseInt(value);
                    case "--token" -> o.token = value;
                    case "--chunk" -> o.chunk = Integer.parseInt(value);
                    case "--data-dir" -> o.dataDir = value;
                    default -> throw new IllegalArgumentException("unknown option: " + name + " (try --help)");
                }
            }
            return o;
        }

        private static String env(String key, String fallback) {
            String v = System.getenv(key);
            return v == null || v.isBlank() ? fallback : v.trim();
        }

        static void printUsage() {
            System.out.println("""
                    Usage: java src/test/java/com/danah/instrument/gateway/tools/TcpSampleSender.java [options]
                       or: java -cp target/test-classes com.danah.instrument.gateway.tools.TcpSampleSender [options]

                      --host HOST      gateway TCP host    (env GATEWAY_TCP_HOST, default 127.0.0.1)
                      --port PORT      gateway TCP port    (env GATEWAY_TCP_PORT, default 9100)
                      --token KEY      gateway API key     (env GATEWAY_API_KEY, default change-me-local-only)
                      --chunk BYTES    target raw bytes per slice (default 740; slices shrink to fit)
                      --data-dir DIR   sample directory    (default src/test/resources/tcp)
                      --dry-run        print frames only, open no socket
                      --help           this text

                    Run it from the project root so the default sample directory resolves.
                    Both "--token KEY" and "--token=KEY" are accepted.
                    """);
        }
    }
}

/**
 * One complete ingest record: the analyser payload wrapped in the ingress envelope.
 *
 * <p>This is the client-side twin of {@code IngestRequest}, the mutable DTO the gateway
 * deserialises into. Same JSON, two ends: that one receives, this one produces.
 *
 * <p>Deliberately not called {@code TcpRecord}. The envelope is identical for TCP, HTTP and MQTT —
 * all three end up in {@code IngestService.ingest(request, protocol, context)} — so a
 * protocol-specific name would force three copies that drift apart. It is a top-level type in this
 * file rather than a nested one so an HTTP or MQTT sender can reuse it as-is: {@code java
 * TcpSampleSender.java} only compiles the file it is given, and a second file would break that.
 *
 * <p>The envelope is exactly four fields:
 *
 * <ul>
 *   <li>{@code type} — selects the payload validator on the gateway side</li>
 *   <li>{@code token} — the credential; the gateway strips it before the event reaches Kafka</li>
 *   <li>{@code idempotencyKey} — lets the downstream drop a resend</li>
 *   <li>{@code payload} — the analyser's own JSON, forwarded untouched</li>
 * </ul>
 *
 * <p>{@code token} is a {@link #toJson(String)} argument rather than a field, so the same record
 * can be replayed after a key rotation without touching the sample data.
 *
 * <p>{@code deviceId}, {@code siteId} and {@code occurredAt} stay out on purpose: the gateway
 * derives identity from the connection, forwards site as null and defaults the timestamp.
 *
 * @param label console-only description, never serialised into the record
 * @param type  event type; must match a gateway {@code EventType}
 * @param idempotencyKey stable across resends, conventionally {@code {stable-id}-{yyyyMMddHHmmss}}
 * @param payload raw JSON object, kept byte-for-byte
 */
record IngestRecord(String label, String type, String idempotencyKey, String payload) {

    String toJson(String token) {
        return "{\"type\":\"" + type + "\""
                + ",\"token\":\"" + token + "\""
                + ",\"idempotencyKey\":\"" + idempotencyKey + "\""
                + ",\"payload\":" + payload + "}";
    }
}
