package com.apex.customer.support;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.test.context.DynamicPropertyRegistry;

/** Test-only: discard inherited deployment overrides before creating client beans. */
public final class IsolatedTestEnvironment implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    @Override
    public void initialize(ConfigurableApplicationContext context) {
        var sources = context.getEnvironment().getPropertySources();
        sources.remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
        var system = sources.get(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
        Map<String, Object> safe = new LinkedHashMap<>();
        if (system instanceof MapPropertySource properties) {
            properties.getSource().forEach((key, value) -> {
                if (!key.matches("(?i)^(spring|apex|cell|kafka|server)\\..*")
                        || key.equals("spring.embedded.kafka.brokers")) safe.put(key, value);
            });
            sources.replace(system.getName(), new MapPropertySource(system.getName(), safe));
        }
    }

    public static void kafka(DynamicPropertyRegistry properties) {
        for (String key : new String[]{"spring.kafka.bootstrap-servers", "spring.kafka.producer.bootstrap-servers",
                "spring.kafka.consumer.bootstrap-servers", "spring.kafka.properties.bootstrap.servers",
                "spring.kafka.producer.properties.bootstrap.servers", "spring.kafka.consumer.properties.bootstrap.servers",
                "spring.kafka.admin.properties.bootstrap.servers"}) {
            properties.add(key, () -> {
                String brokers = System.getProperty("spring.embedded.kafka.brokers");
                if (brokers == null || brokers.isBlank()) throw new IllegalStateException("Embedded Kafka is required");
                return brokers;
            });
        }
    }
}
