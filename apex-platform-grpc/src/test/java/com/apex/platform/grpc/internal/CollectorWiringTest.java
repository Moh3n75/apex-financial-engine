package com.apex.platform.grpc.internal;

import static org.assertj.core.api.Assertions.*;

import com.apex.platform.grpc.api.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.*;

@Tag("integration")
class CollectorWiringTest {
  @Test
  void defaultOffAndEnabledWithoutAGatewayFailsClearly() {
    try (var off =
        GrpcRuntimeTest.boot(GrpcRuntimeTest.Client.class, "--spring.application.name=off")) {
      assertThat(off.getBeansOfType(MultiResponderGateway.class)).isEmpty();
    }
    assertThatThrownBy(
            () ->
                GrpcRuntimeTest.boot(
                    GrpcRuntimeTest.Client.class, "--apex.grpc.collector.enabled=true"))
        .hasStackTraceContaining("QueryGateway");
  }

  @Test
  void bootDiscoverySupportsAuthorizedExplicitLocalQueriesWithoutNetwork() throws Exception {
    try (var app =
        GrpcRuntimeTest.boot(
            GrpcRuntimeTest.App.class,
            "--apex.grpc.client.enabled=true",
            "--apex.grpc.cell-id=local",
            "--apex.grpc.destinations.local.local=true",
            "--apex.grpc.collector.enabled=true",
            "--apex.grpc.collector.allowed-responders=local")) {
      var response =
          app.getBean(MultiResponderGateway.class)
              .collectQueries(
                  new GrpcRuntimeTest.ProbeQuery("local", 0),
                  GrpcRuntimeTest.ProbeReply.class,
                  new CollectionOptions(List.of("local"), Duration.ofSeconds(1), false))
              .toCompletableFuture()
              .get(2, TimeUnit.SECONDS);
      assertThat(response.completeSuccess()).isTrue();
      assertThat(response.responders().getFirst().response().cell()).isEqualTo("local");
      assertThat(app.getBeansOfType(io.grpc.BindableService.class)).isEmpty();
      assertThat(
              ((java.util.concurrent.ScheduledThreadPoolExecutor) app.getBean("grpcDeadlines"))
                  .getRemoveOnCancelPolicy())
          .isTrue();
    }
  }

  @Test
  void invalidAllowlistAndBoundedPolicyFailStartup() {
    assertThatThrownBy(
            () ->
                GrpcRuntimeTest.boot(
                    GrpcRuntimeTest.App.class,
                    "--apex.grpc.client.enabled=true",
                    "--apex.grpc.cell-id=local",
                    "--apex.grpc.destinations.local.local=true",
                    "--apex.grpc.collector.enabled=true",
                    "--apex.grpc.collector.allowed-responders=unknown"))
        .hasStackTraceContaining("explicit responder allowlist");
    var p = new GrpcProperties();
    p.setDestinations(Map.of("local", new GrpcProperties.Destination(true, null)));
    var c = new CollectorProperties();
    c.setAllowedResponders(List.of("local"));
    c.setMaxResponders(17);
    assertThatThrownBy(() -> c.validate(p)).isInstanceOf(IllegalStateException.class);
    var peers = new PeerSecurityProperties();
    peers.setTenant("alpha");
    peers.setPeers(
        Map.of(
            "a",
            new PeerSecurityProperties.Grant(
                "spiffe://apex.test/tenant/alpha/cell/client", List.of("QUERY:VIEW:1")),
            "b",
            new PeerSecurityProperties.Grant(
                "spiffe://apex.test/tenant/alpha/cell/client", List.of("QUERY:VIEW:1"))));
    assertThatThrownBy(peers::validate).hasMessageContaining("duplicate");
  }
}
