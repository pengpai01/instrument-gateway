package com.danah.instrument.gateway.auth;

import com.danah.instrument.gateway.config.GatewayProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.Map;

@Component
public class ApiKeyAuthenticator {

    private final GatewayProperties properties;

    public ApiKeyAuthenticator(GatewayProperties properties) {
        this.properties = properties;
    }

    /** Accepts the shared api-key and any key bound to a clientId in {@code gateway.clients}. */
    public boolean matches(String presented) {
        if (!StringUtils.hasText(presented)) {
            return false;
        }
        String expected = properties.getApiKey();
        if (StringUtils.hasText(expected) && expected.equals(presented)) {
            return true;
        }
        return clientIds().containsKey(presented);
    }

    /**
     * @return the clientId bound to this key, or null when the key is unbound (shared key) or
     *         invalid. Call {@link #matches(String)} first; this method does not authenticate.
     */
    public String clientIdOf(String presented) {
        if (!StringUtils.hasText(presented)) {
            return null;
        }
        String clientId = clientIds().get(presented);
        return StringUtils.hasText(clientId) ? clientId.trim() : null;
    }

    public boolean isConfigured() {
        return StringUtils.hasText(properties.getApiKey()) || !clientIds().isEmpty();
    }

    private Map<String, String> clientIds() {
        return properties.getClients();
    }
}
