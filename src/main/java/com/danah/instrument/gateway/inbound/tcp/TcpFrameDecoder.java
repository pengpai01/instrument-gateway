package com.danah.instrument.gateway.inbound.tcp;

import io.netty.channel.ChannelHandler;

/**
 * Strategy for vendor-specific TCP framing.
 * Default implementation: 2-byte big-endian unsigned length + UTF-8 JSON.
 * Vendors with different framing should provide another {@link TcpFrameDecoder} bean
 * (or replace the pipeline factory) without changing ingest semantics.
 */
public interface TcpFrameDecoder {

    /**
     * @param maxFrameBytes entire frame including length header must be &lt; this
     * @return a new (non-sharable) Netty decoder handler for the pipeline
     */
    ChannelHandler newDecoder(int maxFrameBytes);

    String name();
}
