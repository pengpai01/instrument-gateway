package com.danah.instrument.gateway.tcp;

import com.danah.instrument.gateway.inbound.tcp.LengthPrefixTcpFrameDecoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.TooLongFrameException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class LengthPrefixTcpFrameDecoderTest {

    private static final int MAX_FRAME = 1024;

    @Test
    void decodesValidFrame() {
        LengthPrefixTcpFrameDecoder strategy = new LengthPrefixTcpFrameDecoder();
        EmbeddedChannel ch = new EmbeddedChannel(strategy.newDecoder(MAX_FRAME));
        byte[] json = "{\"a\":1}".getBytes(StandardCharsets.UTF_8);
        ByteBuf frame = Unpooled.buffer();
        frame.writeShort(json.length);
        frame.writeBytes(json);
        assertTrue(ch.writeInbound(frame));
        ByteBuf payload = ch.readInbound();
        assertNotNull(payload);
        assertEquals("{\"a\":1}", payload.toString(StandardCharsets.UTF_8));
        payload.release();
        assertFalse(ch.finish());
    }

    @Test
    void rejectsOversizedFrame() {
        LengthPrefixTcpFrameDecoder strategy = new LengthPrefixTcpFrameDecoder();
        EmbeddedChannel ch = new EmbeddedChannel(strategy.newDecoder(MAX_FRAME));
        // Claim payload length that makes entire frame >= 1024
        int payloadLen = 1022; // 2 + 1022 = 1024 >= max → should fail (maxFrameLength = 1023)
        ByteBuf frame = Unpooled.buffer();
        frame.writeShort(payloadLen);
        frame.writeBytes(new byte[payloadLen]);
        assertThrows(TooLongFrameException.class, () -> ch.writeInbound(frame));
        ch.finish();
    }

    @Test
    void entireFrameUnderLimitAccepted() {
        LengthPrefixTcpFrameDecoder strategy = new LengthPrefixTcpFrameDecoder();
        EmbeddedChannel ch = new EmbeddedChannel(strategy.newDecoder(MAX_FRAME));
        // entire = 2 + 1021 = 1023 < 1024
        int payloadLen = 1021;
        ByteBuf frame = Unpooled.buffer();
        frame.writeShort(payloadLen);
        frame.writeBytes(new byte[payloadLen]);
        assertTrue(ch.writeInbound(frame));
        ByteBuf payload = ch.readInbound();
        assertEquals(payloadLen, payload.readableBytes());
        payload.release();
        ch.finish();
    }
}
