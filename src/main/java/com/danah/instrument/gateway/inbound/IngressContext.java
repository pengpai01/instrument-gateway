package com.danah.instrument.gateway.inbound;

/**
 * Where an inbound message came from.
 *
 * <p>{@code deviceId} is optional on the wire: many analysers either do not know their own
 * serial number or report a free-form value that changes between firmware versions. The gateway
 * still needs one stable string per source to write logs, to pick a Kafka key and to keep one
 * source's events ordered, so {@link #resolveSource(String)} picks the first usable hint from a
 * fixed order and never returns null.
 *
 * <p>Hints are not a security boundary — all of them can be spoofed by the client. The token is
 * what authenticates a message; the hints only decide how the message is labelled.
 */
public record IngressContext(String declaredSourceId,
                             String credentialClientId,
                             String channelId,
                             String remoteHost) {

    /** Returned when every hint is blank, so {@code sourceId} is never null. */
    public static final String ANONYMOUS = "anonymous";

    /** No hint available (unit tests, unknown transports). */
    public static final IngressContext NONE = new IngressContext(null, null, null, null);

    /**
     * Derivation order, strongest first:
     * <ol>
     *   <li>{@code bodySourceId} — the client said so inside the message</li>
     *   <li>{@code declaredSourceId} — the client said so out of band (HTTP {@code X-Client-Id})</li>
     *   <li>{@code credentialClientId} — the operator bound this API key to a name in
     *       {@code gateway.clients}; the only hint a zero-capability device can still carry</li>
     *   <li>{@code channelId} — the MQTT topic segment, set by the broker route</li>
     *   <li>{@code deviceId} — what the instrument put in the body</li>
     *   <li>{@code remoteHost} — the TCP/HTTP peer address; last resort because DHCP moves it</li>
     * </ol>
     *
     * @param bodySourceId the optional {@code sourceId} from the request body
     * @param deviceId     the optional {@code deviceId} from the request body
     * @return trimmed, never blank, never null
     */
    public String resolveSource(String bodySourceId, String deviceId) {
        String source = firstText(bodySourceId, declaredSourceId, credentialClientId,
                channelId, deviceId, remoteHost);
        return source != null ? source : ANONYMOUS;
    }

    /** Same order, without a body-level declaration. */
    public String resolveSource(String deviceId) {
        return resolveSource(null, deviceId);
    }

    private static String firstText(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value.trim();
            }
        }
        return null;
    }
}
