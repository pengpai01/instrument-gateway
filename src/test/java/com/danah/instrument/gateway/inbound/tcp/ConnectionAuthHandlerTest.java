package com.danah.instrument.gateway.inbound.tcp;

import com.danah.instrument.gateway.auth.ApiKeyAuthenticator;
import com.danah.instrument.gateway.config.GatewayProperties;
import com.danah.instrument.gateway.config.TcpProperties;
import com.danah.instrument.gateway.inbound.IngestService;
import com.danah.instrument.gateway.inbound.IngressContext;
import com.danah.instrument.gateway.inbound.model.IngestRequest;
import com.danah.instrument.gateway.inbound.model.Protocol;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.timeout.ReadTimeoutHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Real decoder + real handshake + real inbound handler, ingest mocked: proves a connection needs
 * the AUTH handshake before any frame is accepted, and needs no token afterwards.
 */
class ConnectionAuthHandlerTest {

    private static final String SHARED_KEY = "shared-key";
    private static final String BOUND_KEY = "lab-key-01";
    private static final String BOUND_CLIENT = "lab-01";

    private static final String POWER_WITHOUT_TOKEN =
            "{\"type\":\"POWER\",\"payload\":{\"state\":\"ON\"}}";
    private static final String POWER_WITH_TOKEN =
            "{\"type\":\"POWER\",\"token\":\"" + SHARED_KEY + "\",\"payload\":{\"state\":\"ON\"}}";

    private final ObjectMapper mapper = new ObjectMapper();

    private IngestService ingestService;
    private ApiKeyAuthenticator authenticator;
    private TcpProperties properties;
    private EmbeddedChannel channel;

    @BeforeEach
    void setUp() {
        ingestService = mock(IngestService.class);
        GatewayProperties gateway = new GatewayProperties();
        gateway.setApiKey(SHARED_KEY);
        gateway.setClients(Map.of(BOUND_KEY, BOUND_CLIENT));
        authenticator = new ApiKeyAuthenticator(gateway);

        properties = new TcpProperties();
        channel = newChannel();
    }

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    @Test
    void authFrameIsConsumedAndLaterFramesCarryNoToken() {
        send(auth(BOUND_KEY));
        verify(ingestService, never()).ingest(any(), any(), any());

        send(POWER_WITHOUT_TOKEN);

        ArgumentCaptor<IngestRequest> request = ArgumentCaptor.forClass(IngestRequest.class);
        ArgumentCaptor<IngressContext> context = ArgumentCaptor.forClass(IngressContext.class);
        verify(ingestService).ingest(request.capture(), eq(Protocol.TCP), context.capture());
        assertEquals("POWER", request.getValue().getType());
        assertNull(request.getValue().getToken(), "a token must never leak into the record");
        assertEquals(BOUND_CLIENT, context.getValue().credentialClientId(),
                "clientId bound at handshake must survive without a per-record token");
        assertNotNull(TcpAuthState.principal(channel), "connection must stay authenticated");
    }

    @Test
    void sharedKeyAuthenticatesWithoutBindingAClient() {
        send(auth(SHARED_KEY));
        send(POWER_WITHOUT_TOKEN);

        ArgumentCaptor<IngressContext> context = ArgumentCaptor.forClass(IngressContext.class);
        verify(ingestService).ingest(any(IngestRequest.class), eq(Protocol.TCP), context.capture());
        assertNull(context.getValue().credentialClientId(), "the shared key names no client");
    }

    @Test
    void handshakeRemovesTheTimeoutThatBoundsIt() {
        assertNotNull(channel.pipeline().get(ReadTimeoutHandler.class));

        send(auth(SHARED_KEY));

        assertNull(channel.pipeline().get(ReadTimeoutHandler.class),
                "an authenticated connection may idle: the handshake timeout must be gone");
    }

    @Test
    void businessFrameBeforeHandshakeIsRejected() {
        send(POWER_WITH_TOKEN); // a body token grants nothing: only the AUTH frame counts

        verify(ingestService, never()).ingest(any(), any(), any());
        assertTrue(channel.isActive(), "one rejected frame must leave room to retry the handshake");
    }

    @Test
    void firstFrameThatIsNotAuthIsCountedAsAFailure() {
        properties.getAuth().setMaxFailuresPerConnection(1);

        send(POWER_WITHOUT_TOKEN);

        verify(ingestService, never()).ingest(any(), any(), any());
        assertFalse(channel.isActive(), "the failure limit must also cover protocol violations");
    }

    @Test
    void acceptedHandshakeIsAnsweredWithAuthOk() throws IOException {
        send(auth(BOUND_KEY));

        JsonNode reply = readReply();
        assertEquals(ConnectionAuthHandler.AUTH_OK_TYPE, reply.get("type").asText());
        assertNull(reply.get("reason"), "success needs no explanation");
        verify(ingestService, never()).ingest(any(), any(), any());
    }

    @Test
    void rejectedHandshakeIsAnsweredWithTheReason() throws IOException {
        send(auth("wrong-key"));

        JsonNode reply = readReply();
        assertEquals(ConnectionAuthHandler.AUTH_FAIL_TYPE, reply.get("type").asText());
        assertEquals("invalid token", reply.get("reason").asText());
        assertTrue(channel.isActive(), "the client may retry with the correct key");
        verify(ingestService, never()).ingest(any(), any(), any());
    }

    @Test
    void retryAfterARejectedHandshakeSucceeds() throws IOException {
        send(auth("wrong-key"));
        assertEquals(ConnectionAuthHandler.AUTH_FAIL_TYPE, readReply().get("type").asText());

        send(auth(SHARED_KEY));

        assertEquals(ConnectionAuthHandler.AUTH_OK_TYPE, readReply().get("type").asText());
        assertNotNull(TcpAuthState.principal(channel), "the retry must authenticate the connection");
    }

    @Test
    void failureLimitSendsAuthFailBeforeClosing() throws IOException {
        properties.getAuth().setMaxFailuresPerConnection(1);

        send(auth("wrong-key"));

        assertFalse(channel.isActive());
        JsonNode reply = readReply();
        assertEquals(ConnectionAuthHandler.AUTH_FAIL_TYPE, reply.get("type").asText());
        assertEquals("invalid token", reply.get("reason").asText(),
                "the client must learn why it was dropped, not just see a broken pipe");
    }

    @Test
    void invalidAuthClosesOnlyAfterTheConfiguredAttempts() {
        properties.getAuth().setMaxFailuresPerConnection(2);

        send(auth("wrong-key"));
        assertTrue(channel.isActive(), "one failed attempt must not drop the connection");

        send(auth("wrong-key"));
        assertFalse(channel.isActive());
    }

    @Test
    void aChannelClosedByTheLimitIsNotAnsweredNorCountedAgain() {
        // state after the limit fires: the channel is already inactive when a last frame is
        // handled. Closing destroys the pipeline, so the frame is driven through a channel that
        // only reports itself as closed; the guard must skip both the reply and the count
        EmbeddedChannel closing = new EmbeddedChannel(
                new LengthPrefixTcpFrameDecoder().newDecoder(properties.getMaxFrameBytes()),
                new ConnectionAuthHandler(authenticator, mapper, properties)) {
            @Override
            public boolean isActive() {
                return false;
            }
        };
        try {
            byte[] payload = auth("wrong-key").getBytes(StandardCharsets.UTF_8);
            ByteBuf frame = Unpooled.buffer();
            frame.writeShort(payload.length);
            frame.writeBytes(payload);

            closing.writeInbound(frame);

            assertEquals(0, TcpAuthState.failures(closing),
                    "a frame the gateway cannot answer is not counted");
            assertNull(closing.readOutbound(), "an inactive connection must not answer");
        } finally {
            closing.finishAndReleaseAll();
        }
    }

    @Test
    void tokenLongerThanTheLimitIsRejectedEvenWhenItMatches() {
        properties.getAuth().setMaxFailuresPerConnection(1);
        properties.getAuth().setMaxTokenLength(SHARED_KEY.length() - 1);

        send(auth(SHARED_KEY)); // the right key, but longer than the configured bound

        assertFalse(channel.isActive());
        assertNull(TcpAuthState.principal(channel));
    }

    @Test
    void unreadableFirstFrameIsRejected() {
        properties.getAuth().setMaxFailuresPerConnection(1);

        send("{not-json");

        verify(ingestService, never()).ingest(any(), any(), any());
        assertFalse(channel.isActive());
    }

    @Test
    void authFrameAfterHandshakeIsIgnoredInsteadOfIngested() {
        send(auth(SHARED_KEY));
        send(auth(SHARED_KEY));
        send(POWER_WITHOUT_TOKEN);

        verify(ingestService, times(1)).ingest(any(), any(), any());
    }

    @Test
    void bodyTokenAfterHandshakeIsIgnoredButStillStripped() {
        send(auth(BOUND_KEY));
        send(POWER_WITH_TOKEN); // the shared key in the body must not override the handshake identity

        ArgumentCaptor<IngestRequest> request = ArgumentCaptor.forClass(IngestRequest.class);
        ArgumentCaptor<IngressContext> context = ArgumentCaptor.forClass(IngressContext.class);
        verify(ingestService).ingest(request.capture(), eq(Protocol.TCP), context.capture());
        assertNull(request.getValue().getToken(), "a body token must never reach ingest");
        assertEquals(BOUND_CLIENT, context.getValue().credentialClientId(),
                "identity comes from the handshake, not from the body");
    }

    @Test
    void sessionTtlClosesTheConnectionSoTheClientMustReauthenticate() {
        properties.getAuth().setSessionTtlSeconds(60);

        send(auth(SHARED_KEY));
        send(POWER_WITHOUT_TOKEN);
        verify(ingestService, times(1)).ingest(any(), any(), any());

        // rewind the handshake timestamp instead of waiting: 61s > the 60s TTL
        TcpAuthState.authenticated(channel, new TcpAuthPrincipal(
                null, null, Instant.now().minusSeconds(properties.getAuth().getSessionTtlSeconds() + 1)));
        send(POWER_WITHOUT_TOKEN);

        assertFalse(channel.isActive(), "an expired session must be closed");
        verify(ingestService, times(1)).ingest(any(), any(), any());
    }

    private EmbeddedChannel newChannel() {
        TcpInboundHandler inbound = new TcpInboundHandler(ingestService, mapper,
                properties, new TcpFragmentAssembler(properties));
        return new EmbeddedChannel(
                new ReadTimeoutHandler(5),
                new LengthPrefixTcpFrameDecoder().newDecoder(properties.getMaxFrameBytes()),
                new ConnectionAuthHandler(authenticator, mapper, properties),
                inbound);
    }

    /** Reads the one length-prefixed reply the gateway must send for every handshake attempt. */
    private JsonNode readReply() throws IOException {
        ByteBuf frame = channel.readOutbound();
        assertNotNull(frame, "every handshake attempt must be answered");
        try {
            int length = frame.readUnsignedShort();
            byte[] payload = new byte[length];
            frame.readBytes(payload);
            return mapper.readTree(payload);
        } finally {
            frame.release();
        }
    }

    private String auth(String token) {
        return "{\"type\":\"AUTH\",\"token\":\"" + token + "\"}";
    }

    private void send(String json) {
        byte[] payload = json.getBytes(StandardCharsets.UTF_8);
        ByteBuf frame = Unpooled.buffer();
        frame.writeShort(payload.length);
        frame.writeBytes(payload);
        channel.writeInbound(frame);
    }
}
