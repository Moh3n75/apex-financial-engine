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


  static StandardEnvironment env(Map<String, Object> values) {
    var e = new StandardEnvironment();
    e.getPropertySources().remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
    e.getPropertySources().remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
    e.getPropertySources().addFirst(new MapPropertySource("test", values));
    return e;
  }
}
