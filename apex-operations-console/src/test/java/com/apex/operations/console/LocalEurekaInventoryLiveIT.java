package com.apex.operations.console;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;

/** Explicit live acceptance. The probe is a real, isolated Console registration, not a fake registry response. */
class LocalEurekaInventoryLiveIT {
  final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
  final ObjectMapper json = new ObjectMapper();
  final String origin = "http://127.0.0.1:9190";
  final String probe = "apex-operations-console-local-probe";

  JsonNode topology() throws Exception {
    var response = http.send(HttpRequest.newBuilder(URI.create(origin + "/ops/v1/topology?schemaVersion=2"))
        .timeout(Duration.ofSeconds(4)).GET().build(), HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(200);
    return json.readTree(response.body());
  }
  JsonNode await(boolean present) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(100).toNanos();
    do {
      var graph = topology(); boolean found = false;
      for (var node : graph.path("nodes")) if (node.path("name").asText().equals(probe)) found = true;
      boolean available = false;
      for (var provider : graph.path("providers"))
        if (provider.path("name").asText().equals("eureka-local-inventory") && provider.path("status").asText().equals("AVAILABLE")) available = true;
      if (available && found == present) return graph;
      Thread.sleep(1000);
    } while (System.nanoTime() < deadline);
    throw new AssertionError("Live registry did not converge; no fixture fallback");
  }

  @Test void realRegistrationAndGracefulRemovalPropagateThroughExistingConsole() throws Exception {
    assumeTrue(Boolean.getBoolean("apex.local.inventory.live"), "Requires explicitly running local Console and Eureka");
    var baseline = await(false);
    assertThat(baseline.path("nodes").size()).isGreaterThan(0);
    JsonNode added;
    try (var context = new SpringApplicationBuilder(ConsoleApplication.class).run(
        "--spring.profiles.active=local", "--spring.application.name="+probe, "--server.port=0", "--server.address=127.0.0.1",
        "--apex.ops.m36a.enabled=false", "--apex.ops03.owners=", "--apex.ops.contract-discovery.owners=",
        "--apex.ops.membership.enabled=false", "--apex.grpc.server.enabled=false",
        "--eureka.client.enabled=true", "--eureka.client.register-with-eureka=true",
        "--eureka.client.service-url.defaultZone=http://127.0.0.1:8761/eureka/",
        "--eureka.instance.instance-id="+probe+":owned-live-test",
        "--eureka.client.initial-instance-info-replication-interval-seconds=1")) {
      added = await(true);
      for (var node : added.path("nodes")) if (node.path("name").asText().equals(probe)) {
        assertThat(node.path("health").asText()).isEqualTo("UNKNOWN");
        assertThat(node.path("epoch").isNull()).isTrue();
        assertThat(node.path("evidence").get(0).path("source").asText()).isEqualTo("EUREKA");
      }
      assertThat(added.path("connections")).isEqualTo(baseline.path("connections"));
      assertThat(added.path("memberships")).isEqualTo(baseline.path("memberships"));
    }
    var removed = await(false);
    String receipt = System.getProperty("apex.local.inventory.receipt");
    if (receipt != null) json.writerWithDefaultPrettyPrinter().writeValue(Path.of(receipt).toFile(), Map.of(
        "source", "LIVE_REAL_CONSOLE_REGISTRATION_AND_REMOVAL", "probeService", probe,
        "baseline", baseline, "registered", added, "removed", removed));
    System.out.println("LIVE_LOCAL_INVENTORY: real Console probe registered and removed; final services=" + removed.path("nodes").size());
  }
}
