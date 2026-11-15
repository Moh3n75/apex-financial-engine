package example;

import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.util.ClassUtils;
import static org.assertj.core.api.Assertions.*;

class DiscoveryIsolationTest {
    @Test void exactlyOneProviderCanBeInstalledWithoutOtherTransportsOrImplicitRegistration() {
        var loader=getClass().getClassLoader();
        boolean eureka=ClassUtils.isPresent("com.netflix.discovery.EurekaClient",loader);
        boolean kubernetes=ClassUtils.isPresent("io.fabric8.kubernetes.client.KubernetesClient",loader);
        assertThat(eureka ^ kubernetes).isTrue();
        for(String type:new String[]{"org.springframework.kafka.core.KafkaTemplate","org.springframework.jdbc.core.JdbcTemplate",
                "io.grpc.ManagedChannel","org.springframework.web.servlet.DispatcherServlet"})
            assertThat(ClassUtils.isPresent(type,loader)).as(type).isFalse();
        var app=new SpringApplication(ConsumerApplication.class);app.setWebApplicationType(WebApplicationType.NONE);
        try(var context=app.run("--spring.config.location=optional:classpath:/absent.properties",
                "--apex.discovery.provider=none","--eureka.client.enabled=false")) {
            assertThat(context.containsBean("apexCellServiceDirectory")).isFalse();
            assertThat(context.getBeansOfType(com.apex.platform.discovery.CellServiceDirectory.class)).isEmpty();
            assertThat(context.getBean(com.apex.platform.core.context.ExecutionContextProvider.class)).isNotNull();
        }
        assertThatThrownBy(()->new com.apex.platform.discovery.DiscoveryProperties(){{setProvider("typo");}}.validate())
                .isInstanceOf(IllegalStateException.class);
    }
}
