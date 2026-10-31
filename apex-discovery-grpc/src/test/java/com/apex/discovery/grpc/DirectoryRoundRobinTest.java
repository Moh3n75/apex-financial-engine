package com.apex.discovery.grpc;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import com.apex.platform.discovery.*;
import com.apex.platform.grpc.internal.wire.*;
import io.grpc.*;
import io.grpc.netty.*;
import io.grpc.stub.StreamObserver;
import java.net.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.*;

/** Genuine reused HTTP/2 channel and native servers; packaged mTLS is a separate acceptance. */
@Tag("integration")
class DirectoryRoundRobinTest {
  @Test
  void oneChannelBalancesAndRefreshesExactReplicaAddresses() throws Exception {
    Server a = null, b = null;
    ManagedChannel channel = null;
    var scheduler = Executors.newSingleThreadScheduledExecutor();
    try {
      a = server("a");
      b = server("b");
      var endpoints =
          new AtomicReference<>(List.of(endpoint("a", a.getPort()), endpoint("b", b.getPort())));
      CellServiceDirectory directory =
          logical -> {
            assertThat(logical).isEqualTo("order-cell");
            return endpoints.get();
          };
      var factory =
          new DirectoryNameResolverFactory(
              directory, "order-cell", "localhost", scheduler, Duration.ofMillis(250));
      channel =
          NettyChannelBuilder.forTarget("dns:///localhost:443")
              .nameResolverFactory(factory)
              .intercept(new DiscoveryFreshnessInterceptor(directory, "order-cell"))
              .defaultLoadBalancingPolicy("round_robin")
              .disableRetry()
              .usePlaintext()
              .build(); // owned loopback transport test only
      var stub = CellGatewayGrpc.newBlockingStub(channel);
      var observed = new HashSet<String>();
      await()
          .atMost(Duration.ofSeconds(8))
          .pollInterval(Duration.ofMillis(25))
          .untilAsserted(
              () -> {
                observed.add(query(stub));
                assertThat(observed).containsExactlyInAnyOrder("a", "b");
              });
      var counts = new HashMap<String, Integer>();
      for (int i = 0; i < 40; i++) counts.merge(query(stub), 1, Integer::sum);
      assertThat(counts.get("a")).isBetween(15, 25);
      assertThat(counts.get("b")).isBetween(15, 25);
      endpoints.set(List.of(endpoint("a", a.getPort())));
      b.shutdownNow();
      assertThat(b.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
      await()
          .atMost(Duration.ofSeconds(8))
          .untilAsserted(
              () -> {
                for (int i = 0; i < 5; i++) assertThat(query(stub)).isEqualTo("a");
              });
      endpoints.set(List.of());
      // Even a still-alive former endpoint must not receive a new call after discovery is empty.
      assertThatThrownBy(() -> query(stub)).isInstanceOf(StatusRuntimeException.class);
      assertThat(a.isShutdown()).isFalse();
    } finally {
      if (channel != null) {
        channel.shutdownNow();
        assertThat(channel.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
      }
      for (Server server : Arrays.asList(a, b))
        if (server != null) {
          server.shutdownNow();
          assertThat(server.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
      scheduler.shutdownNow();
      assertThat(scheduler.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void rejectsUnboundedRefreshAndUntrustedAuthoritySyntax() {
    var timer = Executors.newSingleThreadScheduledExecutor();
    try {
      assertThatThrownBy(
              () ->
                  new DirectoryNameResolverFactory(
                      id -> List.of(), "order-cell", "bad/authority", timer, Duration.ofSeconds(1)))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(
              () ->
                  new DirectoryNameResolverFactory(
                      id -> List.of(), "order-cell", "localhost", timer, Duration.ofMillis(1)))
          .isInstanceOf(IllegalArgumentException.class);
    } finally {
      timer.shutdownNow();
    }
  }

  static CellEndpoint endpoint(String name, int port) {
    return new CellEndpoint(
        "order-cell", name, "127.0.0.1", port == 8080 ? 8081 : 8080, port, false, "localhost");
  }

  static String query(CellGatewayGrpc.CellGatewayBlockingStub stub) {
    return stub.withDeadlineAfter(1, TimeUnit.SECONDS)
        .invokeQuery(Invocation.getDefaultInstance())
        .getResponderCell();
  }

  static Server server(String name) throws Exception {
    return NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
        .addService(
            new CellGatewayGrpc.CellGatewayImplBase() {
              @Override
              public void invokeQuery(Invocation input, StreamObserver<Reply> result) {
                result.onNext(Reply.newBuilder().setResponderCell(name).build());
                result.onCompleted();
              }
            })
        .build()
        .start();
  }
}
