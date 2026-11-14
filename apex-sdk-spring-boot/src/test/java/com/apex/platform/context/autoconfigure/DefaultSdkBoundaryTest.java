package com.apex.platform.context.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import com.apex.platform.core.context.ExecutionContextProvider;
import com.apex.platform.events.api.CellEvents;
import com.apex.platform.grpc.api.QueryGateway;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.ClassUtils;

class DefaultSdkBoundaryTest {
    @Test
    void publicContractsAreAvailableWithoutTheirRuntime() {
        assertThat(CellEvents.class.isInterface()).isTrue();
        assertThat(QueryGateway.class.isInterface()).isTrue();
        for (String name : List.of("org.apache.kafka.clients.producer.KafkaProducer",
                "org.springframework.jdbc.core.JdbcTemplate", "io.grpc.ManagedChannel",
                "jakarta.servlet.Servlet", "com.netflix.discovery.EurekaClient",
                "io.fabric8.kubernetes.client.KubernetesClient", "org.springframework.security.web.FilterChainProxy",
                "com.apex.identity.IdentityApplication", "com.apex.gateway.GatewayApplication")) {
            assertThat(ClassUtils.isPresent(name, getClass().getClassLoader())).as(name).isFalse();
        }
    }

    @Test
    void actualBootStartupDiscoversOneProviderWithoutInfrastructureSettings() {
        SpringApplication app = new SpringApplication(Consumer.class);
        app.setWebApplicationType(WebApplicationType.NONE);
        try (var context = app.run("--spring.config.location=optional:classpath:/absent-sdk.properties",
                "--spring.main.banner-mode=off")) {
            assertThat(context.getBeansOfType(ExecutionContextProvider.class)).hasSize(1);
            assertThat(context.getBeansOfType(CellEvents.class)).isEmpty();
            assertThat(context.getBeansOfType(QueryGateway.class)).isEmpty();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class Consumer {}
}
