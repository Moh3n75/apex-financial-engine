package com.apex.platform.grpc.internal;

import static org.assertj.core.api.Assertions.*;

import com.apex.platform.core.context.*;
import com.apex.platform.grpc.api.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.*;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.grpc.server.lifecycle.GrpcServerStartedEvent;

@Tag("integration")
class GrpcRuntimeTest {
  @CellQuery(type = "PROBE", version = 1, response = ProbeReply.class)
  public record ProbeQuery(String value, int delay) {}

  public record ProbeReply(
      String value, UUID correlation, UUID cause, String cell, UUID incomingParent) {}

  public static class Queries {
    private final ExecutionContextProvider provider;

    public Queries(ExecutionContextProvider p) {
      provider = p;
    }

    @CellQueryHandler(type = "PROBE", version = 1)
    public ProbeReply probe(ProbeQuery q, RequestContext metadata) throws InterruptedException {
      if (q.value().equals("bad"))
        throw new IllegalArgumentException("sensitive business value must never reach wire");
      if (q.value().equals("null")) return null;
      if (q.delay() > 0) Thread.sleep(q.delay());
      var ctx = provider.requireCurrent();
      return new ProbeReply(
          q.value(), ctx.correlationId(), ctx.causationId(), ctx.cellId(), metadata.causationId());
    }
  }

  @Configuration(proxyBeanMethods = false)
  @EnableAutoConfiguration
  @Import(Queries.class)
  static class App {
    static final java.util.concurrent.atomic.AtomicInteger PORT =
        new java.util.concurrent.atomic.AtomicInteger();

    @Bean
    org.springframework.context.ApplicationListener<GrpcServerStartedEvent> port() {
      return event -> PORT.set(event.getPort());
    }
  }

  static ConfigurableApplicationContext boot(Class<?> app, String... arguments) {
    var env = new StandardEnvironment();
    env.getPropertySources().remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
    env.getPropertySources().remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
    var args =
        new ArrayList<>(
            List.of(
                "--spring.config.location=optional:classpath:/no-external-m31.yml",
                "--spring.main.banner-mode=off"));
    args.addAll(List.of(arguments));
    return new SpringApplicationBuilder(app)
        .environment(env)
        .web(WebApplicationType.NONE)
        .run(args.toArray(String[]::new));
  }

  @Test
  void disabledStarterDoesNotStartServerOrGateway() {
    try (var context = boot(App.class, "--spring.application.name=off")) {
      assertThat(context.getBeansOfType(io.grpc.BindableService.class)).isEmpty();
      assertThat(context.getBeansOfType(org.springframework.grpc.server.GrpcServerFactory.class))
          .isEmpty();
      assertThat(context.getBeansOfType(QueryGateway.class)).isEmpty();
    }
  }

  @Test
  void bootDiscoveryRealGrpcAndLocalQueriesRespectContextDeadlinesAndCleanup() throws Exception {
    try (var server =
            boot(
                App.class,
                "--apex.grpc.server.enabled=true",
                "--apex.grpc.cell-id=m31-server",
                "--apex.grpc.destinations.m31-server.local=true",
                "--apex.grpc.insecure-loopback-test-only=true",
                "--apex.grpc.test-clients[0]=m31-client",
                "--spring.grpc.server.address=127.0.0.1",
                "--spring.grpc.server.port=0",
                "--spring.grpc.server.ssl.enabled=false",
                "--spring.grpc.server.reflection.enabled=false",
                "--spring.grpc.server.health.enabled=false");
        var client =
            boot(
                Client.class,
                "--apex.grpc.client.enabled=true",
                "--apex.grpc.cell-id=m31-client",
                "--apex.grpc.insecure-loopback-test-only=true",
                "--apex.grpc.destinations.m31-server.channel=server",
                "--spring.grpc.client.channel.server.target=static://127.0.0.1:" + App.PORT.get(),
                "--spring.grpc.client.channel.server.ssl.enabled=false")) {
      assertThat(App.PORT.get()).isPositive();
      assertRawWireGuards(client);
      var gateway = client.getBean(QueryGateway.class);
      var provider = client.getBean(ExecutionContextProvider.class);
      UUID correlation = UUID.randomUUID();
      UUID parent = UUID.randomUUID();
      var options = QueryOptions.defaults();
      try (var scope =
          provider.open(new ExecutionContext(correlation, parent, "m31-client", "m31-client"))) {
        var reply = gateway.query("m31-server", new ProbeQuery("ok", 0), ProbeReply.class, options);
        assertThat(reply)
            .isEqualTo(
                new ProbeReply("ok", correlation, options.requestId(), "m31-server", parent));
        assertThat(provider.requireCurrent().correlationId()).isEqualTo(correlation);
        assertThat(provider.requireCurrent().causationId()).isEqualTo(parent);
      }
      assertThatThrownBy(provider::requireCurrent).isInstanceOf(IllegalStateException.class);
      org.springframework.transaction.support.TransactionSynchronizationManager
          .setActualTransactionActive(true);
      try {
        assertThatThrownBy(
                () ->
                    client
                        .getBean(RequestReplyGateway.class)
                        .request(
                            "m31-server",
                            new WriteRequest("no-network"),
                            WriteResponse.class,
                            RequestOptions.withIdempotencyKey("tx-rejected")))
            .isInstanceOfSatisfying(
                GatewayException.class,
                e -> assertThat(e.code()).isEqualTo(GatewayException.Code.FAILED_PRECONDITION));
      } finally {
        org.springframework.transaction.support.TransactionSynchronizationManager
            .setActualTransactionActive(false);
      }
      var tasks = new ArrayList<CompletableFuture<ProbeReply>>();
      var correlations = new ArrayList<UUID>();
      for (int i = 0; i < 12; i++) {
        UUID corr = UUID.randomUUID();
        correlations.add(corr);
        try (var scope =
            provider.open(new ExecutionContext(corr, null, "m31-client", "m31-client"))) {
          tasks.add(
              gateway
                  .queryAsync(
                      "m31-server",
                      new ProbeQuery("parallel", 0),
                      ProbeReply.class,
                      QueryOptions.defaults())
                  .toCompletableFuture());
        }
      }
      for (int i = 0; i < tasks.size(); i++)
        assertThat(tasks.get(i).get(5, TimeUnit.SECONDS).correlation())
            .isEqualTo(correlations.get(i));
      assertThatThrownBy(
              () -> gateway.query("m31-server", new ProbeQuery("bad", 0), ProbeReply.class))
          .isInstanceOfSatisfying(
              GatewayException.class,
              e -> assertThat(e.code()).isEqualTo(GatewayException.Code.INVALID_ARGUMENT))
          .hasMessage("INVALID_ARGUMENT");
      assertThatThrownBy(() -> gateway.query("missing", new ProbeQuery("ok", 0), ProbeReply.class))
          .isInstanceOfSatisfying(
              GatewayException.class,
              e -> assertThat(e.code()).isEqualTo(GatewayException.Code.NOT_FOUND));
      assertThatThrownBy(
              () -> gateway.query("m31-server", new ProbeQuery("null", 0), ProbeReply.class))
          .isInstanceOfSatisfying(
              GatewayException.class,
              e -> {
                assertThat(e.code()).isEqualTo(GatewayException.Code.INTERNAL);
                assertThat(e.outcomeUnknown()).isFalse();
              });
      assertThatThrownBy(
              () ->
                  gateway.query(
                      "m31-server",
                      new ProbeQuery("slow", 200),
                      ProbeReply.class,
                      new QueryOptions(UUID.randomUUID(), Duration.ofMillis(40))))
          .isInstanceOfSatisfying(
              GatewayException.class,
              e -> {
                assertThat(e.code()).isEqualTo(GatewayException.Code.DEADLINE_EXCEEDED);
                assertThat(e.outcomeUnknown()).isFalse();
              });
      var local = server.getBean(QueryGateway.class);
      assertThat(
              local
                  .query("m31-server", new ProbeQuery("explicit-local-route", 0), ProbeReply.class)
                  .cell())
          .isEqualTo("m31-server");
      assertThat(
              local
                  .queryLocal(new ProbeQuery("local", 0), ProbeReply.class, QueryOptions.defaults())
                  .cell())
          .isEqualTo("m31-server");
      var serverProvider = server.getBean(ExecutionContextProvider.class);
      var workers = server.getBean("grpcServerWorkers", ThreadPoolExecutor.class);
      var ready = new CountDownLatch(8);
      var release = new CountDownLatch(1);
      var checks = new ArrayList<Future<Boolean>>();
      for (int i = 0; i < 8; i++)
        checks.add(
            workers.submit(
                () -> {
                  ready.countDown();
                  release.await(5, TimeUnit.SECONDS);
                  try {
                    serverProvider.requireCurrent();
                    return false;
                  } catch (IllegalStateException clean) {
                    return true;
                  }
                }));
      assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
      release.countDown();
      for (var check : checks) assertThat(check.get(5, TimeUnit.SECONDS)).isTrue();
    }
  }

  private static void assertRawWireGuards(ConfigurableApplicationContext client) {
    var channel =
        client
            .getBean(org.springframework.grpc.client.GrpcChannelFactory.class)
            .createChannel("server");
    try {
      UUID request = UUID.randomUUID(), correlation = UUID.randomUUID();
      var headers = new io.grpc.Metadata();
      headers.put(RpcServerAuth.CELL, "m31-client");
      headers.put(RpcServerAuth.REQUEST, request.toString());
      headers.put(RpcServerAuth.CORRELATION, correlation.toString());
      var stub =
          com.apex.platform.grpc.internal.wire.CellGatewayGrpc.newBlockingStub(channel)
              .withInterceptors(io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(headers));
      var wire =
          com.apex.platform.grpc.internal.wire.Invocation.newBuilder()
              .setContractType("PROBE")
              .setContractVersion(1)
              .setContentType("application/json")
              .setPayload(com.google.protobuf.ByteString.copyFromUtf8("{broken"))
              .setRequestId(request.toString())
              .setCorrelationId(correlation.toString())
              .setOriginCell("m31-client")
              .setTargetCell("m31-server");
      assertThatThrownBy(
              () -> stub.withDeadlineAfter(2, TimeUnit.SECONDS).invokeQuery(wire.build()))
          .isInstanceOfSatisfying(
              io.grpc.StatusRuntimeException.class,
              e ->
                  assertThat(e.getStatus().getCode())
                      .isEqualTo(io.grpc.Status.Code.INVALID_ARGUMENT));
      wire.setPayload(com.google.protobuf.ByteString.copyFrom(new byte[65537]));
      assertThatThrownBy(
              () -> stub.withDeadlineAfter(2, TimeUnit.SECONDS).invokeQuery(wire.build()))
          .isInstanceOfSatisfying(
              io.grpc.StatusRuntimeException.class,
              e ->
                  assertThat(e.getStatus().getCode())
                      .isEqualTo(io.grpc.Status.Code.RESOURCE_EXHAUSTED));
      wire.setPayload(
          com.google.protobuf.ByteString.copyFromUtf8("{\"value\":\"ok\",\"delay\":0}"));
      wire.setContractVersion(99);
      assertThatThrownBy(
              () -> stub.withDeadlineAfter(2, TimeUnit.SECONDS).invokeQuery(wire.build()))
          .isInstanceOfSatisfying(
              io.grpc.StatusRuntimeException.class,
              e ->
                  assertThat(e.getStatus().getCode()).isEqualTo(io.grpc.Status.Code.UNIMPLEMENTED));
      wire.setContractVersion(1).setOriginCell("forged");
      assertThatThrownBy(
              () -> stub.withDeadlineAfter(2, TimeUnit.SECONDS).invokeQuery(wire.build()))
          .isInstanceOfSatisfying(
              io.grpc.StatusRuntimeException.class,
              e ->
                  assertThat(e.getStatus().getCode())
                      .isEqualTo(io.grpc.Status.Code.UNAUTHENTICATED));
      wire.setOriginCell("m31-client");
      for (int i = 0; i < 33; i++)
        headers.put(
            io.grpc.Metadata.Key.of("x-extra", io.grpc.Metadata.ASCII_STRING_MARSHALLER), "v");
      assertThatThrownBy(
              () -> stub.withDeadlineAfter(2, TimeUnit.SECONDS).invokeQuery(wire.build()))
          .isInstanceOfSatisfying(
              io.grpc.StatusRuntimeException.class,
              e ->
                  assertThat(e.getStatus().getCode())
                      .isEqualTo(io.grpc.Status.Code.RESOURCE_EXHAUSTED));
    } finally {
      channel.shutdownNow();
    }
  }

  @CellRequest(type = "WRITE", version = 1, response = WriteResponse.class)
  public record WriteRequest(String value) {}

  public record WriteResponse(String value) {}

  public static class Writes {
    @CellRequestHandler(type = "WRITE", version = 1)
    public WriteResponse write(WriteRequest q) {
      return new WriteResponse(q.value());
    }
  }

  @Configuration(proxyBeanMethods = false)
  @EnableAutoConfiguration
  @Import(Writes.class)
  static class WriteApp {}

  @Test
  void missingPersistentStrategyFailsStartupInsteadOfInventingIdempotency() {
    assertThatThrownBy(
            () ->
                boot(
                    WriteApp.class,
                    "--apex.grpc.server.enabled=true",
                    "--apex.grpc.cell-id=m31-server",
                    "--apex.grpc.insecure-loopback-test-only=true",
                    "--apex.grpc.test-clients[0]=m31-client",
                    "--spring.grpc.server.address=127.0.0.1",
                    "--spring.grpc.server.port=0"))
        .hasStackTraceContaining("owning persistent TransactionalRequestExecutor");
  }

  @Test
  void productionWithoutTrustedMtlsVerifierFailsStartup() {
    assertThatThrownBy(
            () ->
                boot(
                    App.class, "--apex.grpc.server.enabled=true", "--apex.grpc.cell-id=m31-server"))
        .hasStackTraceContaining("trusted PeerIdentityVerifier");
  }

  @Test
  void unsafeOrMissingClientDestinationFailsStartup() {
    assertThatThrownBy(
            () ->
                boot(
                    Client.class,
                    "--apex.grpc.client.enabled=true",
                    "--apex.grpc.cell-id=m31-client"))
        .hasStackTraceContaining("requires at least one explicit destination");
    for (String target : List.of("static://127.0.0.1:99999", "static://example.invalid:443", "")) {
      assertThatThrownBy(
              () ->
                  boot(
                      Client.class,
                      "--apex.grpc.client.enabled=true",
                      "--apex.grpc.cell-id=m31-client",
                      "--apex.grpc.destinations.server.channel=server",
                      "--spring.grpc.client.channel.server.target=" + target,
                      "--spring.grpc.client.channel.server.ssl.enabled=false"))
          .hasStackTraceContaining("gRPC");
    }
  }

  @Configuration(proxyBeanMethods = false)
  @EnableAutoConfiguration
  static class Client {}
}
