package com.danah.instrument.gateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "gateway.mqtt")
public class MqttProperties {

    /**
     * Broker URL e.g. tcp://localhost:1883. Empty = MQTT adapter disabled; process still starts.
     */
    private String brokerUrl = "";
    private String username = "";
    private String password = "";
    private String topicFilter = "instr/+/up";
    private int qos = 1;
    private String clientIdPrefix = "gw-";

    public String getBrokerUrl() {
        return brokerUrl;
    }

    public void setBrokerUrl(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getTopicFilter() {
        return topicFilter;
    }

    public void setTopicFilter(String topicFilter) {
        this.topicFilter = topicFilter;
    }

    public int getQos() {
        return qos;
    }

    public void setQos(int qos) {
        this.qos = qos;
    }

    public String getClientIdPrefix() {
        return clientIdPrefix;
    }

    public void setClientIdPrefix(String clientIdPrefix) {
        this.clientIdPrefix = clientIdPrefix;
    }

    public boolean isConfigured() {
        return brokerUrl != null && !brokerUrl.isBlank();
    }
}
