package com.danah.instrument.gateway.inbound.tcp;

import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.springframework.stereotype.Component;

/**
 * Default frame: 2-byte big-endian unsigned length + UTF-8 JSON payload.
 * Entire frame (header + payload) must be &lt; maxFrameBytes (default 1024).
 * <p>
 * lengthFieldOffset=0, lengthFieldLength=2, lengthAdjustment=0, initialBytesToStrip=2.
 * maxFrameLength is set to maxFrameBytes - 2 (payload max) so Netty rejects oversized payloads;
 * an additional guard in the handler checks total frame size.
 */
@Component
public class LengthPrefixTcpFrameDecoder implements TcpFrameDecoder {

    @Override
    public ChannelHandler newDecoder(int maxFrameBytes) {
        // Payload max = maxFrameBytes - 2 (header). Netty maxFrameLength counts from length field start
        // through end of frame BEFORE stripping; with lengthFieldLength=2 and lengthAdjustment=0,
        // maxFrameLength is the max of (2 + payload). Use maxFrameBytes - 1 to enforce entire frame < maxFrameBytes.
        int maxFrameLength = Math.max(1, maxFrameBytes - 1);
        return new LengthFieldBasedFrameDecoder(
                maxFrameLength,
                0,
                2,
                0,
                2
        );
    }

    @Override
    public String name() {
        return "length-prefix-be16";
    }
}
