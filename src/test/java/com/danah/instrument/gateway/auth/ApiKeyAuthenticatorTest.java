package com.danah.instrument.gateway.auth;

import com.danah.instrument.gateway.config.GatewayProperties;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ApiKeyAuthenticatorTest {

    @Test
    void sharedKeyAuthenticatesWithoutIdentity() {
        ApiKeyAuthenticator auth = authenticator("shared", Map.of());
        assertTrue(auth.matches("shared"));
        assertNull(auth.clientIdOf("shared"), "the shared key names no client");
    }

    @Test
    void boundKeyCarriesItsClientId() {
        ApiKeyAuthenticator auth = authenticator("", Map.of("key-a", "lab-01"));
        assertTrue(auth.matches("key-a"));
        assertEquals("lab-01", auth.clientIdOf("key-a"));
    }

    @Test
    void invalidKeyRejectedEvenWhenClientsConfigured() {
        ApiKeyAuthenticator auth = authenticator("shared", Map.of("key-a", "lab-01"));
        assertFalse(auth.matches("nope"));
        assertNull(auth.clientIdOf("nope"));
    }

    @Test
    void blankKeyRejected() {
        ApiKeyAuthenticator auth = authenticator("", Map.of("key-a", "lab-01"));
        assertFalse(auth.matches(null));
        assertFalse(auth.matches("   "));
    }

    @Test
    void boundKeysCountAsConfiguration() {
        assertTrue(authenticator("", Map.of("key-a", "lab-01")).isConfigured());
        assertFalse(authenticator("", Map.of()).isConfigured());
    }

    private static ApiKeyAuthenticator authenticator(String apiKey, Map<String, String> clients) {
        GatewayProperties properties = new GatewayProperties();
        properties.setApiKey(apiKey);
        properties.setClients(clients);
        return new ApiKeyAuthenticator(properties);
    }
}
