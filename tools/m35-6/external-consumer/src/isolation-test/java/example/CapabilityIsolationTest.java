package example;

import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.util.ClassUtils;
import static org.assertj.core.api.Assertions.assertThat;

class CapabilityIsolationTest {
  @Test void selectedRoleBootsWithoutUnselectedCapabilities() {
    var loader = getClass().getClassLoader();
    for (String type : new String[] {"org.apache.kafka.clients.consumer.KafkaConsumer",
        "org.springframework.jdbc.core.JdbcTemplate", "org.postgresql.Driver",
        "org.springframework.web.servlet.DispatcherServlet", "io.fabric8.kubernetes.client.KubernetesClient"}) {
      assertThat(ClassUtils.isPresent(type, loader)).as(type).isFalse();
    }
    boolean client = ClassUtils.isPresent("org.springframework.boot.grpc.client.autoconfigure.GrpcClientProperties", loader);
    boolean server = ClassUtils.isPresent("org.springframework.boot.grpc.server.autoconfigure.GrpcServerProperties", loader);
    assertThat(client && server).isFalse();
    var application = new SpringApplication(ConsumerApplication.class);
    application.setWebApplicationType(WebApplicationType.NONE);
    try (var context = application.run("--spring.config.location=optional:classpath:/absent.properties",
        "--spring.grpc.server.enabled=false", "--apex.contract-discovery.enabled=true")) {
      assertThat(context.containsBean("contractDiscoveryProvider")).isFalse();
      assertThat(context.containsBean("apexCellGrpcService")).isFalse();
    }
  }
}
