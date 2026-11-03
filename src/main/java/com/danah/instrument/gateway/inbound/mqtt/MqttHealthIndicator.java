package com.danah.instrument.gateway.inbound.mqtt;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicReference;

@Component("mqtt")
public class MqttHealthIndicator implements HealthIndicator {

    public enum State { UP, DOWN, DISABLED, UNKNOWN }

    private final AtomicReference<State> state = new AtomicReference<>(State.UNKNOWN);
    private final AtomicReference<String> detail = new AtomicReference<>("initializing");

    public void setState(State state, String detail) {
        this.state.set(state);
        this.detail.set(detail != null ? detail : "");
    }

    @Override
    public Health health() {
        State s = state.get();
        String d = detail.get();
        return switch (s) {
            case UP -> Health.up().withDetail("mqtt", d).build();
            case DISABLED -> Health.unknown().withDetail("mqtt", d).build();
            case DOWN -> Health.down().withDetail("mqtt", d).build();
            default -> Health.unknown().withDetail("mqtt", d).build();
        };
    }
}
