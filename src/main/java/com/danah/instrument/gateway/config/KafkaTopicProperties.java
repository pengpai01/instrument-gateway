package com.danah.instrument.gateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "gateway.kafka")
public class KafkaTopicProperties {

    private String topic = "instr.event.v1";
    private boolean autoCreateTopic = true;
    private int partitions = 1;
    private short replicationFactor = 1;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public boolean isAutoCreateTopic() {
        return autoCreateTopic;
    }

    public void setAutoCreateTopic(boolean autoCreateTopic) {
        this.autoCreateTopic = autoCreateTopic;
    }

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    public short getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(short replicationFactor) {
        this.replicationFactor = replicationFactor;
    }
}
