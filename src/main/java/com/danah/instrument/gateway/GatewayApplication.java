package com.danah.instrument.gateway;

import com.danah.instrument.gateway.config.GatewayProperties;
import com.danah.instrument.gateway.config.KafkaTopicProperties;
import com.danah.instrument.gateway.config.MqttProperties;
import com.danah.instrument.gateway.config.TcpProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({
    GatewayProperties.class,
    KafkaTopicProperties.class,
    TcpProperties.class,
    MqttProperties.class
})
public class GatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(GatewayApplication.class, args);
    }
}
