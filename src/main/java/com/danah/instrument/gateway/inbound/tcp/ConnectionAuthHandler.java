package com.danah.instrument.gateway.inbound.tcp;

import com.danah.instrument.gateway.auth.ApiKeyAuthenticator;
import com.danah.instrument.gateway.config.TcpProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.ReadTimeoutHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Connection-level authentication: one AUTH frame at the start of a connection replaces a token in
 * every record.
 * <p>
 * The handshake frame is {@code {"type":"AUTH","token":"<key>"}}. It is consumed here — never
 * ingested and never written to Kafka — and the resulting {@link TcpAuthPrincipal} is kept on the
 * channel, so {@link TcpInboundHandler} accepts later frames without a token.
 * <p>
 * The handshake is answered: {@code {"type":"AUTH_OK"}} once the key is accepted, or
 * {@code {"type":"AUTH_FAIL","reason":"..."}} on rejection. A client is expected to wait for
 * AUTH_OK before it sends a record, so a bad key costs no records at all; the gateway itself never
 * buffers them. Business frames stay unanswered — this protocol has no ACK.
 * <p>
 * AUTH is mandatory: a first frame that is not a valid handshake is a protocol violation, counted
 * like a failed credential and answered with AUTH_FAIL. A connection that never authenticates is
 * closed either by the handshake timeout or by the failure limit.
 * <p>
 * Sharable: every piece of per-connection state lives in {@link TcpAuthState} on the channel.
 */
@ChannelHandler.Sharable
@Component
public class ConnectionAuthHandler extends SimpleChannelInboundHandler<ByteBuf> {

    /** {@code type} of the handshake frame; not an {@code EventType}, so it must never reach ingest. */
    public static final String AUTH_TYPE = "AUTH";
    /** Answer to an accepted handshake. */
    public static final String AUTH_OK_TYPE = "AUTH_OK";
    /** Answer to a rejected handshake, always carrying a {@code reason}. */
    public static final String AUTH_FAIL_TYPE = "AUTH_FAIL";

    private static final Logger log = LoggerFactory.getLogger(ConnectionAuthHandler.class);

    private final ApiKeyAuthenticator authenticator;
    private final ObjectMapper objectMapper;
    private final TcpProperties properties;

    public ConnectionAuthHandler(ApiKeyAuthenticator authenticator,
                                 ObjectMapper objectMapper,
                                 TcpProperties properties) {
        this.authenticator = authenticator;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
        TcpProperties.Auth cfg = properties.getAuth();
        TcpAuthPrincipal principal = TcpAuthState.principal(ctx.channel());

        if (principal != null) {
            if (expired(principal, cfg.getSessionTtlSeconds())) {
                TcpAuthState.clear(ctx.channel());
                log.info("TCP session expired from {} — closing, client must reconnect and AUTH",
                        ctx.channel().remoteAddress());
                ctx.close();
                return;
            }
            forward(ctx, msg);
            return;
        }

        JsonNode node;
        try {
            // copy the readable bytes: readTree moves the buffer position, readerIndex must not move
            node = objectMapper.readTree(ByteBufUtil.getBytes(msg, msg.readerIndex(), msg.readableBytes(), true));
        } catch (Exception e) {
            log.warn("TCP unreadable first frame from {}: {}", ctx.channel().remoteAddress(), e.getMessage());
            reject(ctx, "unreadable first frame");
            return;
        }

        if (!isAuthFrame(node)) {
            reject(ctx, "first frame is not AUTH");
            return;
        }

        String token = node.path("token").asText(null);
        String rejection = TcpAuthState.check(token, authenticator, cfg);
        if (rejection != null) {
            reject(ctx, rejection);
            return;
        }

        TcpAuthState.authenticated(ctx.channel(), new TcpAuthPrincipal(
                authenticator.clientIdOf(token), TcpInboundHandler.remoteHost(ctx.channel()), Instant.now()));
        // the client sends its records only after this reply: the handshake is the one place with
        // an answer, so a bad key fails before a single record is put on the wire. A write that
        // never lands still leaves a valid session, so it is warned about rather than ignored
        ctx.writeAndFlush(reply(AUTH_OK_TYPE, null)).addListener(f -> {
            if (!f.isSuccess()) {
                log.warn("AUTH_OK not delivered to {}: {}",
                        ctx.channel().remoteAddress(), String.valueOf(f.cause()));
            }
        });
        dropHandshakeTimeout(ctx);
        log.info("TCP connection authenticated from {} clientId={}",
                ctx.channel().remoteAddress(), TcpAuthState.principal(ctx.channel()).clientId());
        // the AUTH frame is consumed here: it carries no business payload and must not reach ingest
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("TCP channel error before auth {}: {}", ctx.channel().remoteAddress(), cause.toString());
        ctx.close();
    }

    /**
     * Rejects the handshake: answer the client first, then count the failure. Once the limit is
     * reached the channel is closed only after AUTH_FAIL has been flushed, so a rejected client
     * always learns why instead of seeing a bare disconnect.
     */
    private void reject(ChannelHandlerContext ctx, String reason) {
        if (!ctx.channel().isActive()) {
            // the limit already closed this connection: a frame still in flight must not earn a
            // reply or push the counter past the limit
            return;
        }
        log.warn("TCP handshake required from {}: {}", ctx.channel().remoteAddress(), reason);
        int failures = TcpAuthState.recordFailure(ctx.channel());
        // the failure that reaches this count closes the connection; below 1 acts like 1
        int max = Math.max(1, properties.getAuth().getMaxFailuresPerConnection());

        ChannelFuture reply = ctx.writeAndFlush(reply(AUTH_FAIL_TYPE, reason));
        if (failures >= max) {
            log.warn("TCP auth failed from {}: {} — {}/{} failures, closing",
                    ctx.channel().remoteAddress(), reason, failures, max);
            reply.addListener(ChannelFutureListener.CLOSE);
        } else {
            log.warn("TCP auth failed from {}: {} — discarded, {}/{} failures",
                    ctx.channel().remoteAddress(), reason, failures, max);
        }
    }

    /** One length-prefixed JSON frame — the same framing the client uses. */
    private ByteBuf reply(String type, String reason) {
        ObjectNode body = objectMapper.createObjectNode().put("type", type);
        if (reason != null) {
            body.put("reason", reason);
        }
        byte[] payload;
        try {
            payload = objectMapper.writeValueAsBytes(body);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("cannot encode handshake reply " + type, e);
        }
        ByteBuf frame = Unpooled.buffer(payload.length + 2);
        frame.writeShort(payload.length);
        frame.writeBytes(payload);
        return frame;
    }

    private boolean expired(TcpAuthPrincipal principal, int sessionTtlSeconds) {
        if (sessionTtlSeconds <= 0) {
            return false;
        }
        return !Instant.now().isBefore(principal.authenticatedAt().plusSeconds(sessionTtlSeconds));
    }

    private static boolean isAuthFrame(JsonNode node) {
        return node != null && AUTH_TYPE.equalsIgnoreCase(node.path("type").asText(""));
    }

    /** The timeout only bounds the handshake; an authenticated connection is allowed to idle. */
    private static void dropHandshakeTimeout(ChannelHandlerContext ctx) {
        if (ctx.pipeline().get(ReadTimeoutHandler.class) != null) {
            ctx.pipeline().remove(ReadTimeoutHandler.class);
        }
    }

    private static void forward(ChannelHandlerContext ctx, ByteBuf msg) {
        // SimpleChannelInboundHandler releases msg after this method: retain once for the next handler
        ctx.fireChannelRead(msg.retain());
    }
}
