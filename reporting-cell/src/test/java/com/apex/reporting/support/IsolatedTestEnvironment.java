package com.apex.reporting.support;

import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.test.context.DynamicPropertyRegistry;

/** No deployment environment/system properties participate in isolated Reporting tests. */
public final class IsolatedTestEnvironment implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    @Override
    public void initialize(ConfigurableApplicationContext context) {
        context.getEnvironment().getPropertySources().remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
        context.getEnvironment().getPropertySources().remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
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
