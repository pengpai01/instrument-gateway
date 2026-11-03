package com.danah.instrument.gateway.auth;

import com.danah.instrument.gateway.config.GatewayProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
public class ApiKeyAuthenticator {

    private final GatewayProperties properties;

    public ApiKeyAuthenticator(GatewayProperties properties) {
        this.properties = properties;
    }

    public boolean matches(String presented) {
        String expected = properties.getApiKey();
        if (!StringUtils.hasText(expected)) {
            return false;
        }
        return expected.equals(presented);
    }

    public boolean isConfigured() {
        return StringUtils.hasText(properties.getApiKey());
    }
}
