package com.danah.instrument.gateway.inbound.tcp;

import com.danah.instrument.gateway.auth.ApiKeyAuthenticator;
import com.danah.instrument.gateway.config.TcpProperties;
import com.danah.instrument.gateway.inbound.IngestException;
import com.danah.instrument.gateway.inbound.IngestService;
import com.danah.instrument.gateway.inbound.IngressContext;
import com.danah.instrument.gateway.inbound.model.IngestRequest;
import com.danah.instrument.gateway.inbound.model.Protocol;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;

@ChannelHandler.Sharable
@Component
public class TcpInboundHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private static final Logger log = LoggerFactory.getLogger(TcpInboundHandler.class);

    private final IngestService ingestService;
    private final ApiKeyAuthenticator authenticator;
    private final ObjectMapper objectMapper;
    private final TcpProperties tcpProperties;
    private final TcpFragmentAssembler fragmentAssembler;

    public TcpInboundHandler(IngestService ingestService,
                             ApiKeyAuthenticator authenticator,
                             ObjectMapper objectMapper,
                             TcpProperties tcpProperties,
                             TcpFragmentAssembler fragmentAssembler) {
        this.ingestService = ingestService;
        this.authenticator = authenticator;
        this.objectMapper = objectMapper;
        this.tcpProperties = tcpProperties;
        this.fragmentAssembler = fragmentAssembler;
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

        byte[] record;
        try {
            record = recordBytes(ctx.channel(), msg);
        } catch (Exception e) {
            log.warn("TCP bad frame from {}: {} — discarded", ctx.channel().remoteAddress(), e.getMessage());
            return;
        }
        if (record == null) {
            return; // fragment stored; the rest of this record is still in flight
        }

        IngestRequest request;
        try {
            request = objectMapper.readValue(record, IngestRequest.class);
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
            // peer IP is the last-resort identity for a client that sends neither deviceId nor
            // sourceId; the bound clientId (if the key came from gateway.clients) outranks it
            IngressContext ingress = new IngressContext(
                    null, authenticator.clientIdOf(token), null, remoteHost(ctx.channel()));
            ingestService.ingest(request, Protocol.TCP, ingress);
        } catch (IngestException e) {
            log.warn("TCP ingest rejected: {} — discarded", e.getMessage());
        } catch (Exception e) {
            log.error("TCP ingest error: {}", e.toString());
        }
    }

    /** @return the peer IP without the ephemeral port, or null when the address is not IP-based. */
    static String remoteHost(Channel channel) {
        SocketAddress address = channel.remoteAddress();
        if (address instanceof InetSocketAddress inet && inet.getAddress() != null) {
            return inet.getAddress().getHostAddress();
        }
        return null;
    }

    /**
     * Frames carrying msgId/seq/total are fragments of one JSON record; merge them before parsing.
     * Frames without those fields are parsed as a single-frame record (legacy format).
     *
     * @return the complete record bytes, or {@code null} while more fragments are needed
     */
    private byte[] recordBytes(Channel channel, ByteBuf msg) throws Exception {
        byte[] raw = new byte[msg.readableBytes()];
        msg.readBytes(raw);

        JsonNode node = objectMapper.readTree(raw);
        if (node == null
                || !node.hasNonNull("msgId")
                || !node.hasNonNull("seq")
                || !node.hasNonNull("total")) {
            return raw;
        }
        if (!tcpProperties.getAssembly().isEnabled()) {
            log.warn("TCP fragment received but gateway.tcp.assembly.enabled=false from {} — discarded",
                    channel.remoteAddress());
            return null;
        }

        // data carries the slice as escaped JSON text; Jackson already unescaped it for us, so the
        // bytes here are exactly the bytes the client cut. base64 is no longer part of the contract.
        JsonNode dataNode = node.get("data");
        byte[] chunk = dataNode == null || dataNode.isNull()
                ? new byte[0]
                : dataNode.asText().getBytes(StandardCharsets.UTF_8);

        return fragmentAssembler.accept(channel,
                node.get("msgId").asText(),
                node.get("seq").asInt(),
                node.get("total").asInt(),
                chunk);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        fragmentAssembler.discard(ctx.channel());
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("TCP channel error {}: {}", ctx.channel().remoteAddress(), cause.toString());
        ctx.close();
    }
}
