package com.danah.instrument.gateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "gateway")
public class GatewayProperties {

    /**
     * Global API key. Prefer env GATEWAY_API_KEY. Never commit real production secrets.
     */
    private String apiKey = "";

    /**
     * Optional API key to clientId map. This is the only identity an instrument with no
     * serial number and no configuration surface can still carry: the installer picks the key,
     * so the key itself names the source. A key that matches here authenticates even when
     * {@link #apiKey} is empty.
     */
    private Map<String, String> clients = new LinkedHashMap<>();

    public String getApiKey() {
        return apiKey;
    }

    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
    }

    public Map<String, String> getClients() {
        return clients;
    }

    public void setClients(Map<String, String> clients) {
        this.clients = clients != null ? clients : new LinkedHashMap<>();
    }
}
