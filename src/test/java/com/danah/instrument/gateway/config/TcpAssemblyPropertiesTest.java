package com.danah.instrument.gateway.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Locks {@code gateway.tcp.assembly.*} binding so a typo in application.yml cannot silently
 * fall back to defaults (and so the nested Assembly class stays bindable).
 */
class TcpAssemblyPropertiesTest {

    @Test
    void applicationYmlBindsAssemblyBlock() throws IOException {
        List<PropertySource<?>> loaded = new YamlPropertySourceLoader()
                .load("application.yml", new ClassPathResource("application.yml"));
        assertEquals(1, loaded.size(), "application.yml");

        TcpProperties props = new Binder(ConfigurationPropertySources.from(loaded))
                .bind("gateway.tcp", Bindable.of(TcpProperties.class))
                .orElseThrow(() -> new IllegalStateException("gateway.tcp not bound"));

        assertTrue(props.isEnabled());
        assertEquals(9100, props.getPort());
        assertEquals(1024, props.getMaxFrameBytes());

        TcpProperties.Assembly assembly = props.getAssembly();
        assertNotNull(assembly);
        assertTrue(assembly.isEnabled());
        assertEquals(65536, assembly.getMaxBytes());
        assertEquals(128, assembly.getMaxFragments());
        assertEquals(10000, assembly.getTimeoutMs());
        assertEquals(8, assembly.getMaxPendingPerChannel());
    }
}
