package com.danah.instrument.gateway.inbound.tcp;

import com.danah.instrument.gateway.auth.ApiKeyAuthenticator;
import com.danah.instrument.gateway.config.TcpProperties;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

/**
 * Per-connection auth state. The TCP handlers are sharable singletons, so the principal and the
 * failure counter live on the channel rather than on the handler.
 */
final class TcpAuthState {

    private static final AttributeKey<TcpAuthPrincipal> PRINCIPAL =
            AttributeKey.valueOf("gw.tcp.principal");
    private static final AttributeKey<Integer> FAILURES =
            AttributeKey.valueOf("gw.tcp.authFailures");

    private TcpAuthState() {
    }

    /** @return the handshake result for this connection, or {@code null} when it never completed one */
    static TcpAuthPrincipal principal(Channel channel) {
        return channel.attr(PRINCIPAL).get();
    }

    static void authenticated(Channel channel, TcpAuthPrincipal principal) {
        channel.attr(PRINCIPAL).set(principal);
        channel.attr(FAILURES).set(0);
    }

    /** Forgets the principal and the failure count; used when a session expires. */
    static void clear(Channel channel) {
        channel.attr(PRINCIPAL).set(null);
        channel.attr(FAILURES).set(0);
    }

    /**
     * Token policy for the handshake: bound the length, then check the credential. Pure: the
     * caller decides how to answer and when the failure limit closes the connection.
     *
     * @return {@code null} when the token is acceptable, otherwise the reason to report back
     */
    static String check(String token, ApiKeyAuthenticator authenticator, TcpProperties.Auth cfg) {
        if (token == null || token.isBlank()) {
            return "missing token";
        }
        if (token.length() > cfg.getMaxTokenLength()) {
            return "token longer than " + cfg.getMaxTokenLength();
        }
        return authenticator.matches(token) ? null : "invalid token";
    }

    /** Rejected handshakes counted so far on this connection; 0 before the first rejection. */
    static int failures(Channel channel) {
        Integer count = channel.attr(FAILURES).get();
        return count != null ? count : 0;
    }

    /** Counts one rejected handshake and returns the running total for this connection. */
    static int recordFailure(Channel channel) {
        int failures = failures(channel) + 1;
        channel.attr(FAILURES).set(failures);
        return failures;
    }
}
