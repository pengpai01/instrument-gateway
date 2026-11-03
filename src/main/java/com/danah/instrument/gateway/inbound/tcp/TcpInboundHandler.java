package com.danah.instrument.gateway.inbound.tcp;

import com.danah.instrument.gateway.auth.ApiKeyAuthenticator;
import com.danah.instrument.gateway.config.TcpProperties;
import com.danah.instrument.gateway.inbound.IngestException;
import com.danah.instrument.gateway.inbound.IngestService;
import com.danah.instrument.gateway.inbound.model.IngestRequest;
import com.danah.instrument.gateway.inbound.model.Protocol;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@ChannelHandler.Sharable
@Component
public class TcpInboundHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private static final Logger log = LoggerFactory.getLogger(TcpInboundHandler.class);

    private final IngestService ingestService;
    private final ApiKeyAuthenticator authenticator;
    private final ObjectMapper objectMapper;
    private final TcpProperties tcpProperties;

    public TcpInboundHandler(IngestService ingestService,
                             ApiKeyAuthenticator authenticator,
                             ObjectMapper objectMapper,
                             TcpProperties tcpProperties) {
        this.ingestService = ingestService;
        this.authenticator = authenticator;
        this.objectMapper = objectMapper;
        this.tcpProperties = tcpProperties;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
        // After LengthFieldBasedFrameDecoder with initialBytesToStrip=2, msg is payload only.
        // Entire frame = 2 + payloadBytes; enforce < maxFrameBytes.
        int payloadBytes = msg.readableBytes();
        int entireFrame = 2 + payloadBytes;
        if (entireFrame >= tcpProperties.getMaxFrameBytes()) {
            log.warn("TCP frame discarded: entire frame {} bytes >= max {}", entireFrame, tcpProperties.getMaxFrameBytes());
            return;
        }

        String json = msg.toString(StandardCharsets.UTF_8);
        IngestRequest request;
        try {
            request = objectMapper.readValue(json, IngestRequest.class);
        } catch (Exception e) {
            log.warn("TCP bad JSON from {}: {} — discarded", ctx.channel().remoteAddress(), e.getMessage());
            return;
        }

        String token = request.getToken();
        request.setToken(null); // strip — never into Kafka
        if (!authenticator.matches(token)) {
            log.warn("TCP auth failed from {} — discarded", ctx.channel().remoteAddress());
            return;
        }

        try {
            ingestService.ingest(request, Protocol.TCP, null);
        } catch (IngestException e) {
            log.warn("TCP ingest rejected: {} — discarded", e.getMessage());
        } catch (Exception e) {
            log.error("TCP ingest error: {}", e.toString());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("TCP channel error {}: {}", ctx.channel().remoteAddress(), cause.toString());
        ctx.close();
    }
}
