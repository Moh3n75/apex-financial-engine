package com.apex.discovery.kubernetes;

import static org.assertj.core.api.Assertions.*;

import com.apex.platform.discovery.*;
import com.sun.net.httpserver.HttpServer;
import io.fabric8.kubernetes.client.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import org.springframework.core.env.*;

/** Real Fabric8 HTTP calls to an owned controlled API; this is not a deployed cluster. */
@Tag("integration")
class EndpointSliceApiTest {
  HttpServer api;
  KubernetesClient client;
  EndpointSliceDiscoveryClient discovery;
  final AtomicReference<String> slice = new AtomicReference<>();
  final AtomicBoolean fail = new AtomicBoolean();
  final AtomicLong nano = new AtomicLong();
  final List<String> requests = new CopyOnWriteArrayList<>();
  final DiscoveryProperties policy = new DiscoveryProperties();
  String service =
      """
      {"apiVersion":"v1","kind":"Service","metadata":{"name":"order-cell","namespace":"apex-test",
       "labels":{"apex.cell-id":"order-cell"},"annotations":{"apex.grpc.authority":"order.apex.test"}},
       "spec":{"clusterIP":"10.96.0.99","ports":[{"name":"http","port":8083},{"name":"grpc","port":9091}]}}
      """;

  @BeforeEach
  void start() throws Exception {
    policy.setProvider("kubernetes");
    policy.setRefreshInterval(Duration.ofMillis(250));
    policy.setTargets(Map.of("order-cell", new DiscoveryProperties.Target("order.apex.test")));
    slice.set(slices(true, false, "127.0.0.2", 9091));
    api = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    api.createContext(
        "/",
        exchange -> {
          requests.add(exchange.getRequestURI().toString());
          String path = exchange.getRequestURI().getPath();
          String body;
          int code = fail.get() ? 500 : 200;
          if (path.equals("/api/v1/namespaces/apex-test/services/order-cell")) body = service;
          else if (path.equals("/apis/discovery.k8s.io/v1/namespaces/apex-test/endpointslices"))
            body = slice.get();
          else {
            code = 403;
            body = "{}";
          }
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(code, bytes.length);
          try (var out = exchange.getResponseBody()) {
            out.write(bytes);
          }
        });
    api.start();
    client =
        new KubernetesClientBuilder()
            .withConfig(
                new ConfigBuilder(Config.empty())
                    .withMasterUrl("http://127.0.0.1:" + api.getAddress().getPort())
                    .withNamespace("apex-test")
                    .withConnectionTimeout(1000)
                    .withRequestTimeout(1000)
                    .withRequestRetryBackoffLimit(0)
                    .build())
            .build();
    discovery =
        new EndpointSliceDiscoveryClient(
            client, "apex-test", policy, Duration.ofSeconds(2), nano::get, false);
  }

  @AfterEach
  void stop() {
    if (discovery != null) discovery.close();
    if (client != null) client.close();
    if (api != null) api.stop(0);
  }

  @Test
  void resolvesPodEndpointNotClusterIpWithScopedRequests() {
    var result =
        new CloudCellServiceDirectory(discovery, policy, discovery::fresh).endpoints("order-cell");
    assertThat(result).hasSize(1);
    assertThat(result.getFirst().host()).isEqualTo("127.0.0.2").isNotEqualTo("10.96.0.99");
    assertThat(result.getFirst().httpPort()).isEqualTo(8083);
    assertThat(result.getFirst().grpcPort()).isEqualTo(9091);
    assertThat(requests).allMatch(p -> p.contains("/namespaces/apex-test/"));
    assertThat(requests).anyMatch(p -> p.contains("labelSelector="));
    assertThat(requests).noneMatch(p -> p.contains("secrets") || p.contains("/pods"));
  }

  @Test
  void excludesUnreadyAndTerminatingAndReflectsEndpointReplacement() {
    slice.set(slices(false, false, "127.0.0.2", 9091));
    discovery.refresh();
    assertThat(discovery.getInstances("order-cell")).isEmpty();
    slice.set(slices(true, true, "127.0.0.2", 9091));
    discovery.refresh();
    assertThat(discovery.getInstances("order-cell")).isEmpty();
    slice.set(slices(true, false, "127.0.0.3", 9092));
    discovery.refresh();
    var result =
        new CloudCellServiceDirectory(discovery, policy, discovery::fresh).endpoints("order-cell");
    assertThat(result.getFirst().host()).isEqualTo("127.0.0.3");
    assertThat(result.getFirst().grpcPort()).isEqualTo(9092);
  }

  @Test
  void apiFailureCannotRefreshExpiredEndpoints() {
    fail.set(true);
    assertThatThrownBy(discovery::refresh).isInstanceOf(RuntimeException.class);
    nano.set(Duration.ofSeconds(3).toNanos());
    assertThat(discovery.fresh()).isFalse();
    assertThatThrownBy(() -> discovery.getInstances("order-cell")).hasMessageContaining("expired");
    assertThatThrownBy(
            () ->
                new CloudCellServiceDirectory(discovery, policy, discovery::fresh)
                    .endpoints("order-cell"))
        .hasMessageContaining("stale");
  }

  @Test
  void wrongAuthorityNamespaceAndPortsFailClosed() {
    service = service.replace("order.apex.test", "forged.apex.test");
    assertThatThrownBy(discovery::refresh).hasMessageContaining("authority");
    service =
        service
            .replace("forged.apex.test", "order.apex.test")
            .replace("apex-test", "other-namespace");
    assertThatThrownBy(discovery::refresh).hasMessageContaining("identity");
    service = service.replace("other-namespace", "apex-test");
    slice.set(slices(true, false, "127.0.0.2", 8083));
    assertThatThrownBy(discovery::refresh).isInstanceOf(IllegalArgumentException.class);
  }

  static String slices(boolean ready, boolean terminating, String ip, int grpc) {
    return """
    {"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSliceList","metadata":{"resourceVersion":"1"},"items":[
    {"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSlice","metadata":{"name":"order-1","namespace":"apex-test",
    "labels":{"kubernetes.io/service-name":"order-cell"}},"addressType":"IPv4",
    "ports":[{"name":"http","protocol":"TCP","port":8083},{"name":"grpc","protocol":"TCP","port":%d}],
    "endpoints":[{"addresses":["%s"],"conditions":{"ready":%s,"terminating":%s},
    "targetRef":{"kind":"Pod","namespace":"apex-test","name":"order-1","uid":"00000000-0000-4000-8000-000000000001"}}]}]}
    """
        .formatted(grpc, ip, ready, terminating);
  }
}
