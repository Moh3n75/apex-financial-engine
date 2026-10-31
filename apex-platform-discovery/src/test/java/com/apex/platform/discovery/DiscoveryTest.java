package com.apex.platform.discovery;

import static org.assertj.core.api.Assertions.*;

import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.core.env.*;

@Tag("unit")
class DiscoveryTest {
  @Test
  void fetchOnlyEurekaDoesNotStartWebAutoRegistrationAndRegisteredReadinessIncludesDb() {
    var values = new HashMap<String, Object>();
    values.put("apex.discovery.provider", "eureka");
    values.put("eureka.client.service-url.defaultZone", "http://127.0.0.1:8761/eureka/");
    values.put("apex.discovery.insecure-loopback-test-only", true);
    var client = env(values);
    client.setActiveProfiles("eureka");
    new DiscoveryModeGuard().postProcessEnvironment(client, null);
    assertThat(
            client.getProperty(
                "spring.cloud.service-registry.auto-registration.enabled", Boolean.class))
        .isFalse();
    values.put("apex.discovery.registration.enabled", true);
    var server = env(values);
    server.setActiveProfiles("eureka");
    new DiscoveryModeGuard().postProcessEnvironment(server, null);
    assertThat(
            server.getProperty(
                "spring.cloud.service-registry.auto-registration.enabled", Boolean.class))
        .isTrue();
    assertThat(server.getProperty("management.endpoint.health.group.readiness.include"))
        .isEqualTo("*");
  }

  final List<ServiceInstance> live = new ArrayList<>();
  final DiscoveryClient provider =
      new DiscoveryClient() {
        public String description() {
          return "controlled provider";
        }

        public List<ServiceInstance> getInstances(String id) {
          return List.copyOf(live);
        }

        public List<String> getServices() {
          return List.of("order-cell");
        }
      };
  final DiscoveryProperties policy = new DiscoveryProperties();

  DiscoveryTest() {
    policy.setTargets(Map.of("order-cell", new DiscoveryProperties.Target("order.apex.test")));
  }

  DefaultServiceInstance instance(String id, int port) {
    return new DefaultServiceInstance(
        id,
        "order-cell",
        "127.0.0.1",
        port,
        false,
        new HashMap<>(
            Map.of(
                "apex.cell-id",
                "order-cell",
                "apex.grpc.port",
                Integer.toString(port + 1),
                "apex.grpc.authority",
                "order.apex.test")));
  }

  @Test
  void separatesPortsAndReflectsReplicaAdditionAndRemoval() {
    var directory = new CloudCellServiceDirectory(provider, policy, () -> true);
    live.add(instance("b", 8082));
    live.add(instance("a", 8080));
    assertThat(directory.endpoints("order-cell"))
        .extracting(CellEndpoint::instanceId)
        .containsExactly("a", "b");
    assertThat(directory.endpoints("order-cell").getFirst().httpPort()).isEqualTo(8080);
    assertThat(directory.endpoints("order-cell").getFirst().grpcPort()).isEqualTo(8081);
    live.removeFirst();
    assertThat(directory.endpoints("order-cell")).hasSize(1);
    live.clear();
    assertThat(directory.endpoints("order-cell")).isEmpty();
  }

  @Test
  void refusesUnknownRouteAndExpiredSnapshot() {
    assertThatThrownBy(
            () ->
                new CloudCellServiceDirectory(provider, policy, () -> true)
                    .endpoints("payment-cell"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                new CloudCellServiceDirectory(provider, policy, () -> false)
                    .endpoints("order-cell"))
        .hasMessageContaining("stale");
  }

  @Test
  void refusesDuplicateIdsAndSockets() {
    var directory = new CloudCellServiceDirectory(provider, policy, () -> true);
    live.add(instance("same", 8080));
    live.add(instance("same", 8082));
    assertThatThrownBy(() -> directory.endpoints("order-cell")).hasMessageContaining("Duplicate");
    live.clear();
    live.add(instance("a", 8080));
    live.add(instance("b", 8080));
    assertThatThrownBy(() -> directory.endpoints("order-cell")).hasMessageContaining("Duplicate");
  }

  @Test
  void rejectsForgedAuthorityCellAndIncompatiblePorts() {
    var directory = new CloudCellServiceDirectory(provider, policy, () -> true);
    var i = instance("a", 8080);
    live.add(i);
    i.getMetadata().clear();
    i.getMetadata().putAll(Map.of("apex.cell-id", "payment-cell"));
    assertThatThrownBy(() -> directory.endpoints("order-cell")).hasMessageContaining("mismatch");
    i.getMetadata().clear();
    i.getMetadata()
        .putAll(Map.of("apex.cell-id", "order-cell", "apex.grpc.authority", "wrong.apex.test"));
    assertThatThrownBy(() -> directory.endpoints("order-cell")).hasMessageContaining("mismatch");
    i.getMetadata().clear();
    i.getMetadata()
        .putAll(
            Map.of(
                "apex.cell-id",
                "order-cell",
                "apex.grpc.authority",
                "order.apex.test",
                "apex.grpc.port",
                "8080"));
    assertThatThrownBy(() -> directory.endpoints("order-cell"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void disabledModeTurnsOffBothNetworkProvidersBeforeBeans() {
    var env = env(Map.of());
    new DiscoveryModeGuard().postProcessEnvironment(env, null);
    assertThat(env.getProperty("eureka.client.enabled", Boolean.class)).isFalse();
    assertThat(env.getProperty("spring.cloud.kubernetes.enabled", Boolean.class)).isFalse();
  }

  @Test
  void rejectsMultipleModesWrongProfileAndUnscopedKubernetes() {
    var env = env(Map.of("apex.discovery.provider", "eureka"));
    assertThatThrownBy(() -> new DiscoveryModeGuard().postProcessEnvironment(env, null))
        .isInstanceOf(IllegalStateException.class);
    env.setActiveProfiles("eureka", "kubernetes");
    assertThatThrownBy(() -> new DiscoveryModeGuard().postProcessEnvironment(env, null))
        .isInstanceOf(IllegalStateException.class);
    var kube =
        env(
            Map.of(
                "apex.discovery.provider",
                "kubernetes",
                "spring.cloud.kubernetes.discovery.all-namespaces",
                true));
    kube.setActiveProfiles("kubernetes");
    assertThatThrownBy(() -> new DiscoveryModeGuard().postProcessEnvironment(kube, null))
        .hasMessageContaining("namespace");
  }

  @Test
  void kubernetesSelectsOnlyReadyEndpointSlicesAndDisablesEureka() {
    var env =
        env(
            Map.of(
                "apex.discovery.provider",
                "kubernetes",
                "spring.cloud.kubernetes.client.namespace",
                "apex-test"));
    env.setActiveProfiles("kubernetes");
    new DiscoveryModeGuard().postProcessEnvironment(env, null);
    assertThat(env.getProperty("eureka.client.enabled", Boolean.class)).isFalse();
    assertThat(
            env.getProperty("spring.cloud.kubernetes.discovery.use-endpoint-slices", Boolean.class))
        .isTrue();
    assertThat(
            env.getProperty(
                "spring.cloud.kubernetes.discovery.include-not-ready-addresses", Boolean.class))
        .isFalse();
  }

  static StandardEnvironment env(Map<String, Object> values) {
    var e = new StandardEnvironment();
    e.getPropertySources().remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
    e.getPropertySources().remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
    e.getPropertySources().addFirst(new MapPropertySource("test", values));
    return e;
  }
}
