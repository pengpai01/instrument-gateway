package com.danah.instrument.gateway.inbound.tcp;

import java.time.Instant;

/**
 * A connection that completed the AUTH handshake.
 *
 * @param clientId        clientId bound to the presented key in {@code gateway.clients}, or
 *                        {@code null} when the shared key was used
 * @param remoteHost      peer address captured at handshake time; the last-resort identity when
 *                        the body carries neither sourceId nor deviceId
 * @param authenticatedAt when the handshake was accepted; drives the session TTL
 */
public record TcpAuthPrincipal(String clientId, String remoteHost, Instant authenticatedAt) {
}
