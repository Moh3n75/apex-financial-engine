package example;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.apex.platform.core.context.ExecutionContext;
import com.apex.platform.core.context.ExecutionContextProvider;
import com.apex.platform.core.context.ThreadLocalExecutionContextProvider;
import com.apex.platform.events.api.CellEvents;
import com.apex.platform.grpc.api.QueryGateway;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.annotation.Bean;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.util.ClassUtils;

class ConsumerTest {
    private SpringApplication app(Class<?>... classes) {
        var app = new SpringApplication(classes);
        app.setWebApplicationType(WebApplicationType.NONE);
        return app;
    }

    private final String[] options = {"--spring.config.location=optional:classpath:/absent.properties",
            "--spring.main.banner-mode=off"};

    @Test
    void bootsInstalledSdkWithNoInfrastructureAndNoApplicationSettings() {
        try (var context = app(ConsumerApplication.class).run(options)) {
            assertThat(context.getBeansOfType(ExecutionContextProvider.class)).hasSize(1);
            assertThat(context.getBeansOfType(CellEvents.class)).isEmpty();
            assertThat(context.getBeansOfType(QueryGateway.class)).isEmpty();
        }
    }

    @Test
    void preservesCustomExecutionContextProvider() {
        try (var context = app(ConsumerApplication.class, Custom.class).run(options)) {
            assertThat(context.getBeansOfType(ExecutionContextProvider.class)).hasSize(1);
            assertThat(context.getBean(ExecutionContextProvider.class)).isSameAs(context.getBean("customProvider"));
        }
    }

    @Test
    void executionScopeCleansUpAndJavaContractsAreTyped() {
        var provider = new ThreadLocalExecutionContextProvider();
        var execution = ExecutionContext.root("external", "external");
        try (var ignored = provider.open(execution)) {
            assertThat(provider.requireCurrent()).isEqualTo(execution);
        }
        assertThatThrownBy(provider::requireCurrent).isInstanceOf(IllegalStateException.class);
        assertThat(ConsumerApplication.Inspect.class.getAnnotation(com.apex.platform.grpc.api.CellQuery.class)
                .response()).isEqualTo(ConsumerApplication.Answer.class);
    }

    @Test
    void hasNoExecutableCellOrExternalRuntimeDependencies() {
        for (String name : List.of("org.apache.kafka.clients.consumer.KafkaConsumer",
                "org.springframework.jdbc.core.JdbcTemplate", "io.grpc.ManagedChannel",
                "com.netflix.discovery.EurekaClient", "io.fabric8.kubernetes.client.KubernetesClient",
                "org.springframework.security.oauth2.server.authorization.JdbcOAuth2AuthorizationService",
                "com.apex.identity.IdentityApplication", "com.apex.order.OrderCellApplication",
                "com.apex.inventory.InventoryCellApplication", "com.apex.payment.PaymentCellApplication")) {
            assertThat(ClassUtils.isPresent(name, getClass().getClassLoader())).as(name).isFalse();
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class Custom {
        @Bean ExecutionContextProvider customProvider() { return new ThreadLocalExecutionContextProvider(); }
    }
}
