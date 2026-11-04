package com.danah.instrument.gateway.inbound.tcp;

import com.danah.instrument.gateway.config.TcpProperties;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Merges length-prefix fragments back into one JSON record.
 * <p>
 * Envelope fields: {@code msgId} (same for all fragments), {@code seq} (1-based),
 * {@code total} (fragment count), {@code data} (the slice as escaped JSON text — plain text, not
 * base64; the client cuts on UTF-8 character boundaries so the concatenation is lossless).
 * {@code seq == total} is the last fragment; the record is complete when all
 * {@code total} distinct fragments arrived, so arrival order does not matter.
 * <p>
 * State is per-connection ({@link Channel#attr(AttributeKey)}) because the inbound handler is sharable.
 */
@Component
public class TcpFragmentAssembler {

    private static final Logger log = LoggerFactory.getLogger(TcpFragmentAssembler.class);

    private static final AttributeKey<Map<String, Pending>> PENDING =
            AttributeKey.valueOf("gw.tcp.pending");

    private final TcpProperties properties;

    public TcpFragmentAssembler(TcpProperties properties) {
        this.properties = properties;
    }

    /**
     * @return the complete record once the last missing fragment arrives, {@code null} while waiting
     */
    public byte[] accept(Channel channel, String msgId, int seq, int total, byte[] data) {
        TcpProperties.Assembly cfg = properties.getAssembly();
        if (msgId == null || msgId.isBlank()
                || seq < 1 || seq > total || total < 1 || total > cfg.getMaxFragments()) {
            log.warn("TCP fragment rejected msgId={} seq={} total={} from {}",
                    msgId, seq, total, channel.remoteAddress());
            return null;
        }

        Map<String, Pending> pending = channel.attr(PENDING).get();
        if (pending == null) {
            pending = new HashMap<>();
            channel.attr(PENDING).set(pending);
        }
        evictExpired(pending, cfg.getTimeoutMs());

        Pending record = pending.get(msgId);
        if (record == null) {
            if (pending.size() >= cfg.getMaxPendingPerChannel()) {
                log.warn("TCP pending records full ({}) on {} — dropping msgId={}",
                        cfg.getMaxPendingPerChannel(), channel.remoteAddress(), msgId);
                return null;
            }
            record = new Pending(total);
            pending.put(msgId, record);
        } else if (record.total != total) {
            log.warn("TCP fragment total mismatch msgId={} expected={} got={} — dropped",
                    msgId, record.total, total);
            pending.remove(msgId);
            return null;
        }

        if (record.parts[seq - 1] == null) {
            record.parts[seq - 1] = data;
            record.bytes += data.length;
        } else {
            log.debug("TCP duplicate fragment msgId={} seq={} — kept first", msgId, seq);
        }

        if (record.bytes > cfg.getMaxBytes()) {
            log.warn("TCP assembled record too large msgId={} bytes={} > max {} — dropped",
                    msgId, record.bytes, cfg.getMaxBytes());
            pending.remove(msgId);
            return null;
        }

        if (!record.isComplete()) {
            log.debug("TCP fragment msgId={} {}/{} bytes={}", msgId, record.received(), total, record.bytes);
            return null;
        }

        pending.remove(msgId);
        ByteArrayOutputStream out = new ByteArrayOutputStream(record.bytes);
        for (byte[] part : record.parts) {
            if (part == null) {
                log.warn("TCP fragment hole msgId={} — dropped", msgId);
                return null;
            }
            out.writeBytes(part);
        }
        log.debug("TCP record reassembled msgId={} fragments={} bytes={}", msgId, total, record.bytes);
        return out.toByteArray();
    }

    /** Drop everything still waiting for fragments on this connection (disconnect / too long). */
    public void discard(Channel channel) {
        Map<String, Pending> pending = channel.attr(PENDING).getAndSet(null);
        if (pending != null && !pending.isEmpty()) {
            log.warn("TCP discarding {} unfinished record(s) on {}", pending.size(), channel.remoteAddress());
        }
    }

    private void evictExpired(Map<String, Pending> pending, int timeoutMs) {
        long now = System.currentTimeMillis();
        pending.entrySet().removeIf(entry -> {
            Pending p = entry.getValue();
            boolean expired = now - p.createdAt > timeoutMs;
            if (expired) {
                log.warn("TCP fragment assembly timeout msgId={} received={}/{} — dropped",
                        entry.getKey(), p.received(), p.total);
            }
            return expired;
        });
    }

    private static final class Pending {

        private final int total;
        private final byte[][] parts;
        private final long createdAt = System.currentTimeMillis();
        private int bytes;

        private Pending(int total) {
            this.total = total;
            this.parts = new byte[total][];
        }

        private boolean isComplete() {
            return received() == total;
        }

        private int received() {
            int count = 0;
            for (byte[] part : parts) {
                if (part != null) {
                    count++;
                }
            }
            return count;
        }
    }
}
