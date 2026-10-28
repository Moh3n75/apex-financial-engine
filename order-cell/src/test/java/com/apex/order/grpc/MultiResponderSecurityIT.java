package com.apex.order.grpc;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.*;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Three packaged owning Cells and a separate Java-only client over real authenticated HTTP/2. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MultiResponderSecurityIT {
  static final JsonMapper JSON = JsonMapper.builder().build();
  static final List<String> TARGETS = List.of("inventory-cell", "order-cell", "payment-cell");
  static final String PRIMARY = "spiffe://apex.test/tenant/alpha/cell/m32-client";
  static final String OBSERVER = "spiffe://apex.test/tenant/alpha/cell/m32-observer";
  Fixture f;

  @BeforeAll
  void start() throws Exception {
    f = new Fixture();
    try {
      f.start();
    } catch (Exception | Error failure) {
      try {
        f.close();
      } catch (Exception cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  @AfterAll
  void close() throws Exception {
    if (f != null) f.close();
  }

  @AfterEach
  void queriesNeverMutateOwnerFactsOrPublishKafka() {
    assertThat(f.snapshot()).isEqualTo(f.baseline);
    assertThat(f.kafkaConnections.get()).isZero();
  }

  @Test
  void threeRealTlsTargetsReturnFaithfulFactsAndIsolatedParallelMetadata() throws Exception {
    var command = f.collection(f.orderId);
    var result = f.client.call(command);
    assertSuccess(result);
    verifyMetadata(
        result,
        command.get("correlationId").toString(),
        command.get("parentCausationId").toString());
    command.put("action", "concurrent-collections");
    command.put("count", 4);
    var collections = f.client.call(command);
    assertThat(collections.size()).isEqualTo(4);
    var correlations = new HashSet<String>();
    var ids = new HashSet<String>();
    for (var collection : collections) {
      String correlation = collection.path("correlationId").asText();
      correlations.add(correlation);
      assertSuccess(collection.path("collection"));
      verifyMetadata(
          collection.path("collection"), correlation, command.get("parentCausationId").toString());
      for (var r : collection.path("collection").path("responders"))
        assertThat(ids.add(r.path("invocationId").asText())).isTrue();
    }
    assertThat(correlations).hasSize(4);
    f.evidence.add(
        Map.of("case", "parallel-mtls", "collections", 4, "uniqueInvocationIds", ids.size()));
  }

  @Test
  void missingFactsAndUnsafeTargetsDoNotBecomeBusinessDenials() throws Exception {
    var command = f.collection(UUID.randomUUID());
    var missing = f.client.call(command);
    assertSlots(missing);
    for (var slot : missing.path("responders")) {
      assertThat(slot.path("status").asText()).isEqualTo("NOT_FOUND");
      assertThat(slot.path("answerUnknown").asBoolean()).isFalse();
    }
    command.put("allowPartial", false);
    var strict = f.client.call(command);
    assertThat(strict.path("error").asText()).isEqualTo("COLLECTION_INCOMPLETE");
    assertSlots(strict.path("collection"));
    command = f.collection(f.orderId);
    command.put("targets", List.of("order-cell", "not-allowlisted"));
    error(f.client.call(command), "INVALID_ARGUMENT");
    command.put("targets", List.of("order-cell", "order-cell"));
    error(f.client.call(command), "INVALID_ARGUMENT");
    error(
        f.client.call(
            Map.of("action", "perspective", "target", "unknown", "orderId", f.orderId.toString())),
        "NOT_FOUND");
    error(
        f.client.call(Map.of("action", "perspective99", "orderId", f.orderId.toString())),
        "PERMISSION_DENIED");
    f.evidence.add(
        Map.of("case", "absence-and-contracts", "missingSlots", 3, "unknownTargetsRejected", true));
  }

  @Test
  void oneUnavailableTargetRemainsAnExplicitUnknownSlot() throws Exception {
    Cell payment = f.cells.get("payment-cell");
    payment.stop();
    try {
      var result = f.client.call(f.collection(f.orderId));
      assertSlots(result);
      assertThat(slot(result, "payment-cell").path("status").asText()).isEqualTo("UNAVAILABLE");
      assertThat(slot(result, "payment-cell").path("answerUnknown").asBoolean()).isTrue();
      assertThat(slot(result, "order-cell").path("status").asText()).isEqualTo("SUCCESS");
      f.evidence.add(Map.of("case", "unavailable", "result", result));
    } finally {
      payment.start();
      f.awaitHealthy();
    }
  }

  @Test
  void slowTargetRespectsOneOverallDeadlinePartialStrictAndCancellation() throws Exception {
    try (var lock = f.cells.get("payment-cell").db.getPostgresDatabase().getConnection()) {
      lock.setAutoCommit(false);
      lock.createStatement().execute("LOCK TABLE payment_authorizations IN ACCESS EXCLUSIVE MODE");
      var command = f.collection(f.orderId);
      command.put("deadlineMs", 600);
      long start = System.nanoTime();
      var result = f.client.call(command);
      long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
      if (result.path("responders").size() != 3) {
        var dump =
            new ProcessBuilder(
                    Path.of(System.getProperty("java.home"), "bin", "jcmd").toString(),
                    Long.toString(f.client.process.pid()),
                    "Thread.print")
                .redirectErrorStream(true)
                .redirectOutput(f.output.resolve("deadline-failure-threads.log").toFile())
                .start();
        if (!dump.waitFor(5, TimeUnit.SECONDS)) {
          dump.destroyForcibly();
          dump.waitFor(5, TimeUnit.SECONDS);
        }
      }
      assertSlots(result);
      assertThat(elapsed).isLessThan(2000);
      assertThat(slot(result, "payment-cell").path("status").asText()).isEqualTo("TIMED_OUT");
      assertThat(slot(result, "inventory-cell").path("status").asText()).isEqualTo("SUCCESS");
      command.put("allowPartial", false);
      var strict = f.client.call(command);
      error(strict, "COLLECTION_INCOMPLETE");
      assertSlots(strict.path("collection"));
      command = f.collection(f.orderId);
      command.put("action", "cancel-collection");
      command.put("cancelAfterMs", 500);
      var cancelled = f.client.call(command);
      error(cancelled, "COLLECTION_CANCELLED");
      assertSlots(cancelled.path("collection"));
      assertThat(slot(cancelled.path("collection"), "payment-cell").path("status").asText())
          .isEqualTo("CANCELLED");
      assertThat(slot(cancelled.path("collection"), "order-cell").path("status").asText())
          .isEqualTo("SUCCESS");
      verifyMetadata(
          cancelled.path("collection"),
          command.get("correlationId").toString(),
          command.get("parentCausationId").toString());
      try (var limited =
          f.newClient(
              "limited",
              f.certs.primary,
              "m32-client",
              false,
              false,
              List.of(
                  "--apex.grpc.collector.max-collections=1",
                  "--apex.grpc.collector.max-in-flight=3"))) {
        var saturation = f.collection(f.orderId);
        saturation.put("action", "saturation");
        var rejected = limited.call(saturation);
        assertThat(rejected.path("secondError").asText()).isEqualTo("RESOURCE_EXHAUSTED");
        assertThat(rejected.path("firstCancelled").asBoolean()).isTrue();
      }
      f.evidence.add(
          Map.of(
              "case",
              "overall-deadline",
              "deadlineMs",
              600,
              "observedMs",
              elapsed,
              "slots",
              result));
      lock.rollback();
    }
    f.awaitHealthy();
  }

  @Test
  void handlerInfrastructureErrorIsFailedNotADeclineAndRollsBackReadTransaction() throws Exception {
    var payment = f.cells.get("payment-cell").jdbc;
    payment.execute("ALTER TABLE payment_authorizations RENAME TO m32_hidden_authorizations");
    try {
      var result = f.client.call(f.collection(f.orderId));
      assertSlots(result);
      assertThat(slot(result, "payment-cell").path("status").asText()).isEqualTo("FAILED");
      assertThat(slot(result, "payment-cell").path("errorCode").asText()).isEqualTo("INTERNAL");
      assertThat(slot(result, "payment-cell").path("answerUnknown").asBoolean()).isTrue();
      f.evidence.add(Map.of("case", "handler-error", "result", result));
    } finally {
      payment.execute("ALTER TABLE m32_hidden_authorizations RENAME TO payment_authorizations");
    }
    f.awaitHealthy();
  }

  @Test
  void actualHandshakeFailuresAndPostHandshakeIdentityAuthorizationFailClosed() throws Exception {
    int port = f.cells.get("order-cell").port;
    assertThat(tlsProbe(f.certs.context(f.certs.primary, f.certs.trust), port)).startsWith("TLS");
    for (Path key : Arrays.asList(null, f.certs.rogue)) {
      Throwable failure = catchThrowable(() -> tlsProbe(f.certs.context(key, f.certs.trust), port));
      assertThat(failure).isInstanceOf(IOException.class).isNotInstanceOf(ConnectException.class);
      assertThat(f.cells.get("order-cell").process.isAlive()).isTrue();
      f.evidence.add(
          Map.of(
              "case",
              key == null ? "handshake-missing-client" : "handshake-untrusted-client",
              "failureClass",
              failure.getClass().getName()));
    }
    Throwable badTrust =
        catchThrowable(() -> tlsProbe(f.certs.context(f.certs.primary, f.certs.rogue), port));
    assertThat(badTrust).isInstanceOf(IOException.class).isNotInstanceOf(ConnectException.class);
    f.evidence.add(
        Map.of(
            "case", "handshake-untrusted-server", "failureClass", badTrust.getClass().getName()));
    // A valid, trusted chain with a wrong workload SAN completes TLS but fails APEX authentication.
    assertThat(tlsProbe(f.certs.context(f.certs.wrongSan, f.certs.trust), port)).startsWith("TLS");
    record Negative(
        String label,
        Path key,
        String identity,
        boolean plaintext,
        boolean wrongTrust,
        String expected) {}
    var cases =
        List.of(
            new Negative("missing-certificate", null, "m32-client", false, false, "UNAVAILABLE"),
            new Negative("untrusted-ca", f.certs.rogue, "m32-client", false, false, "UNAVAILABLE"),
            new Negative(
                "untrusted-server", f.certs.primary, "m32-client", false, true, "UNAVAILABLE"),
            new Negative("plaintext", null, "m32-client", true, false, "UNAVAILABLE"),
            new Negative(
                "wrong-san", f.certs.wrongSan, "m32-client", false, false, "UNAUTHENTICATED"),
            new Negative(
                "forged-origin", f.certs.observer, "m32-client", false, false, "UNAUTHENTICATED"),
            new Negative(
                "foreign-tenant",
                f.certs.foreign,
                "m32-foreign",
                false,
                false,
                "PERMISSION_DENIED"));
    for (var c : cases)
      try (var client =
          f.newClient(c.label(), c.key(), c.identity(), c.plaintext(), c.wrongTrust(), List.of())) {
        var request = GrpcOrderIT.command("create");
        error(client.call(request), c.expected());
        f.evidence.add(
            Map.of(
                "case",
                c.label(),
                "error",
                c.expected(),
                "pid",
                client.process.pid(),
                "businessEffect",
                false));
      }
    try (var observer =
        f.newClient("observer", f.certs.observer, "m32-observer", false, false, List.of())) {
      var result = observer.call(f.collection(f.orderId));
      assertSlots(result);
      assertThat(slot(result, "order-cell").path("status").asText()).isEqualTo("SUCCESS");
      for (String target : List.of("inventory-cell", "payment-cell")) {
        assertThat(slot(result, target).path("status").asText()).isEqualTo("FAILED");
        assertThat(slot(result, target).path("errorCode").asText()).isEqualTo("PERMISSION_DENIED");
      }
      error(observer.call(f.createCommand), "ALREADY_EXISTS");
      f.evidence.add(
          Map.of("case", "target-acl", "result", result, "observerPid", observer.process.pid()));
    }
  }

  @Test
  void persistentReceiptsDoNotExpireOrSilentlyCrossContractVersions() throws Exception {
    var jdbc = f.cells.get("order-cell").jdbc;
    String key = f.createCommand.get("key").toString();
    var original =
        jdbc.queryForMap(
            "SELECT response,created_at FROM order_request_receipt WHERE idempotency_key=?", key);
    try {
      jdbc.update(
          "UPDATE order_request_receipt SET created_at=TIMESTAMPTZ '2020-01-01 00:00:00Z' WHERE"
              + " idempotency_key=?",
          key);
      assertThat(f.client.call(f.createCommand)).isEqualTo(f.created);
      var version = new LinkedHashMap<>(f.createCommand);
      version.put("action", "version2");
      error(f.client.call(version), "UNIMPLEMENTED");
      var conflict = new LinkedHashMap<>(f.createCommand);
      conflict.put("quantity", 2);
      error(f.client.call(conflict), "ALREADY_EXISTS");
      jdbc.update(
          "UPDATE order_request_receipt SET response=? WHERE idempotency_key=?",
          "{\"incompatible\":1}".getBytes(StandardCharsets.UTF_8),
          key);
      error(f.client.call(f.createCommand), "FAILED_PRECONDITION");
      assertThat(GrpcOrderIT.total(jdbc, "orders")).isEqualTo(1);
      assertThat(GrpcOrderIT.total(jdbc, "outbox_event")).isEqualTo(1);
      assertThat(GrpcOrderIT.total(jdbc, "order_request_receipt")).isEqualTo(1);
      f.evidence.add(
          Map.of(
              "case",
              "receipt-lifecycle",
              "retainedSince",
              "2020-01-01",
              "version2",
              "UNIMPLEMENTED",
              "incompatibleBytes",
              "FAILED_PRECONDITION"));
    } finally {
      jdbc.update(
          "UPDATE order_request_receipt SET response=?,created_at=? WHERE idempotency_key=?",
          original.get("response"),
          original.get("created_at"),
          key);
    }
    assertThat(f.client.call(f.createCommand)).isEqualTo(f.created);
  }

  static String tlsProbe(SSLContext context, int port) throws Exception {
    try (var socket = (SSLSocket) context.getSocketFactory().createSocket("127.0.0.1", port)) {
      socket.setSoTimeout(4000);
      socket.setEnabledProtocols(new String[] {"TLSv1.2"});
      socket.startHandshake();
      return socket.getSession().getProtocol();
    }
  }

  static JsonNode slot(JsonNode collection, String target) {
    for (var r : collection.path("responders"))
      if (r.path("responder").asText().equals(target)) return r;
    throw new AssertionError("Missing named response: " + target);
  }

  static void error(JsonNode result, String code) {
    assertThat(result.path("error").asText()).isEqualTo(code);
  }

  static void assertSlots(JsonNode result) {
    assertThat(result.path("responders").size()).isEqualTo(3);
    var names = new ArrayList<String>();
    result.path("responders").forEach(r -> names.add(r.path("responder").asText()));
    assertThat(names).containsExactlyElementsOf(TARGETS);
  }

  static void assertSuccess(JsonNode result) {
    assertSlots(result);
    for (var r : result.path("responders")) {
      assertThat(r.path("status").asText()).isEqualTo("SUCCESS");
      assertThat(r.path("response").path("owner").asText()).isEqualTo(r.path("responder").asText());
    }
    assertThat(slot(result, "order-cell").path("response").path("state").asText())
        .isEqualTo("PENDING_INVENTORY");
    assertThat(slot(result, "inventory-cell").path("response").path("state").asText())
        .isEqualTo("RESERVED");
    assertThat(slot(result, "payment-cell").path("response").path("state").asText())
        .isEqualTo("AUTHORIZED");
  }

  void verifyMetadata(JsonNode result, String correlation, String parent) {
    for (var r : result.path("responders")) {
      String id = r.path("invocationId").asText();
      String log = GrpcOrderIT.read(f.cells.get(r.path("responder").asText()).log);
      assertThat(
              log.lines()
                  .anyMatch(
                      l ->
                          l.contains("requestId=" + id)
                              && l.contains("correlationId=" + correlation)
                              && l.contains("parentCausationId=" + parent)))
          .isTrue();
    }
  }

  final class Fixture implements AutoCloseable {
    final Path root =
        Path.of(System.getProperty("grpc.repositoryRoot")).toAbsolutePath().normalize();
    final Path output = root.resolve("order-cell/target/m32-evidence");
    final Map<String, Cell> cells = new LinkedHashMap<>();
    final List<Map<String, Object>> evidence = new ArrayList<>();
    final AtomicInteger kafkaConnections = new AtomicInteger();
    final List<Long> childPids = new ArrayList<>();
    final List<Process> children = new ArrayList<>();
    boolean closed;
    EphemeralCertificates certs;
    Client client;
    ServerSocket kafkaTrap;
    Thread trapThread;
    UUID orderId;
    JsonNode created;
    Map<String, Object> createCommand;
    Map<String, String> baseline;

    void start() throws Exception {
      Files.createDirectories(output);
      verifyArtifacts();
      certs = new EphemeralCertificates();
      kafkaTrap = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
      trapThread =
          Thread.ofPlatform()
              .daemon()
              .name("m32-no-kafka-connection-trap")
              .start(
                  () -> {
                    while (!kafkaTrap.isClosed())
                      try (var ignored = kafkaTrap.accept()) {
                        kafkaConnections.incrementAndGet();
                      } catch (IOException stopped) {
                      }
                  });
      for (String target : List.of("order-cell", "inventory-cell", "payment-cell")) {
        var cell = new Cell(this, target);
        cells.put(target, cell);
        cell.start();
      }
      client = newClient("primary", certs.primary, "m32-client", false, false, List.of());
      createCommand = GrpcOrderIT.command("create");
      createCommand.put("deadlineMs", 5000);
      created = client.call(createCommand);
      assertThat(created.path("status").asText()).isEqualTo("PENDING_INVENTORY");
      orderId = UUID.fromString(created.path("orderId").asText());
      GrpcOrderIT.assertCommitted(cells.get("order-cell").jdbc, createCommand, orderId);
      // Fixture-only separate owning connections seed durable facts. This is not a synthetic Saga
      // completion claim.
      cells
          .get("inventory-cell")
          .jdbc
          .update(
              "INSERT INTO inventory_reservations(order_id,status,created_at) VALUES"
                  + " (?,'RESERVED',clock_timestamp())",
              orderId);
      cells
          .get("payment-cell")
          .jdbc
          .update(
              "INSERT INTO payment_authorizations(order_id,status) VALUES (?,'AUTHORIZED')",
              orderId);
      awaitHealthy();
      baseline = snapshot();
      evidence.add(
          Map.of(
              "case",
              "topology",
              "orderId",
              orderId,
              "cells",
              cells.values().stream()
                  .map(
                      c ->
                          Map.of(
                              "cell",
                              c.id,
                              "pid",
                              c.process.pid(),
                              "grpcPort",
                              c.port,
                              "databasePort",
                              c.db.getPort()))
                  .toList(),
              "clientPid",
              client.process.pid(),
              "primarySan",
              PRIMARY,
              "tls",
              "real Boot Netty TLS/mTLS HTTP2",
              "kafkaBrokerStarted",
              false));
    }

    void verifyArtifacts() throws Exception {
      Path receipt = root.resolve("out/m32-build-receipt.json");
      assertThat(receipt).isRegularFile();
      for (var item : JSON.readTree(Files.readAllBytes(receipt)).path("files")) {
        Path path = root.resolve(item.path("path").asText());
        assertThat(sha(path)).isEqualTo(item.path("sha256").asText());
      }
    }

    LinkedHashMap<String, Object> collection(UUID id) {
      var cmd = new LinkedHashMap<String, Object>();
      cmd.put("action", "collect");
      cmd.put("targets", TARGETS);
      cmd.put("orderId", id.toString());
      cmd.put("deadlineMs", 3000);
      cmd.put("allowPartial", true);
      cmd.put("correlationId", UUID.randomUUID().toString());
      cmd.put("parentCausationId", UUID.randomUUID().toString());
      return cmd;
    }

    void awaitHealthy() throws Exception {
      await()
          .atMost(Duration.ofSeconds(20))
          .untilAsserted(() -> assertSuccess(client.call(collection(orderId))));
    }

    Client newClient(
        String label,
        Path keys,
        String identity,
        boolean plaintext,
        boolean wrongTrust,
        List<String> extra)
        throws Exception {
      var args =
          new ArrayList<>(
              List.of(
                  "--spring.main.web-application-type=none",
                  "--spring.config.location=optional:classpath:/m32-no-deployment.yml",
                  "--apex.grpc.cell-id=" + identity,
                  "--apex.grpc.client.enabled=true",
                  "--apex.grpc.collector.enabled=true",
                  "--apex.grpc.collector.max-responders=3",
                  "--apex.grpc.collector.allowed-responders=" + String.join(",", TARGETS)));
      for (var c : cells.values()) {
        args.add("--apex.grpc.destinations." + c.id + ".channel=" + c.id);
        args.add("--spring.grpc.client.channel." + c.id + ".target=static://127.0.0.1:" + c.port);
        args.add("--spring.grpc.client.channel." + c.id + ".ssl.enabled=" + !plaintext);
        if (!plaintext) args.add("--spring.grpc.client.channel." + c.id + ".ssl.bundle=m32");
      }
      if (plaintext) args.add("--apex.grpc.insecure-loopback-test-only=true");
      args.addAll(certs.bundle(keys));
      if (wrongTrust) {
        args.removeIf(a -> a.startsWith("--spring.ssl.bundle.jks.m32.truststore.location="));
        args.add("--spring.ssl.bundle.jks.m32.truststore.location=" + certs.rogue.toUri());
      }
      for (var option : extra) {
        String key = option.substring(0, option.indexOf('=') + 1);
        args.removeIf(a -> a.startsWith(key));
        args.add(option);
      }
      return new Client(
          root.resolve("sample-grpc-client/target/sample-grpc-client.jar"),
          output.resolve(label + ".log"),
          args,
          this);
    }

    Map<String, String> snapshot() {
      var state = new TreeMap<String, String>();
      cells.forEach(
          (id, c) -> {
            var tables = new ArrayList<>(List.of("outbox_event", "processed_event"));
            tables.addAll(
                switch (id) {
                  case "order-cell" -> List.of("orders", "order_items", "order_request_receipt");
                  case "inventory-cell" ->
                      List.of(
                          "inventory_stock",
                          "inventory_reservations",
                          "inventory_reservation_items");
                  default -> List.of("payment_authorizations", "payment_simulation_policy");
                });
            for (String table : tables)
              state.put(
                  id + "/" + table,
                  c.jdbc.queryForObject(
                      "SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY"
                          + " to_jsonb(x)::text)::text,'[]') FROM "
                          + table
                          + " x",
                      String.class));
          });
      return state;
    }

    @Override
    public void close() throws Exception {
      if (closed) return;
      closed = true;
      var failures = new ArrayList<Exception>();
      if (client != null)
        try {
          client.close();
        } catch (Exception e) {
          failures.add(e);
        }
      for (var c : cells.values())
        try {
          c.close();
        } catch (Exception e) {
          failures.add(e);
        }
      if (kafkaTrap != null)
        try {
          kafkaTrap.close();
          if (trapThread != null) trapThread.join(2000);
        } catch (Exception e) {
          failures.add(e);
        }
      if (certs != null)
        try {
          Path dir = certs.directory;
          certs.close();
          assertThat(Files.exists(dir)).isFalse();
        } catch (Exception e) {
          failures.add(e);
        }
      boolean allStopped = children.stream().noneMatch(Process::isAlive);
      evidence.add(
          Map.of(
              "case",
              "teardown",
              "childPids",
              childPids,
              "allChildrenStopped",
              allStopped,
              "kafkaConnections",
              kafkaConnections.get(),
              "cleanupFailures",
              failures.size()));
      Files.writeString(
          output.resolve("acceptance.json"),
          JSON.writerWithDefaultPrettyPrinter().writeValueAsString(evidence));
      if (!failures.isEmpty()) {
        var failure = failures.getFirst();
        failures.stream().skip(1).forEach(failure::addSuppressed);
        throw failure;
      }
      assertThat(allStopped).isTrue();
    }
  }

  static String sha(Path path) throws Exception {
    return HexFormat.of()
        .formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path)));
  }

  static final class Cell implements AutoCloseable {
    final Fixture fixture;
    final String id;
    final EmbeddedPostgres db;
    final JdbcTemplate jdbc;
    final int port;
    final Path log;
    Process process;

    Cell(Fixture fixture, String id) throws Exception {
      this.fixture = fixture;
      this.id = id;
      port = GrpcOrderIT.freePort();
      log = fixture.output.resolve(id + ".log");
      db =
          EmbeddedPostgres.builder()
              .setServerConfig("fsync", "on")
              .setServerConfig("synchronous_commit", "on")
              .start();
      jdbc = new JdbcTemplate(db.getPostgresDatabase());
    }

    void start() throws Exception {
      var args =
          new ArrayList<>(
              List.of(
                  "--spring.profiles.active="
                      + (id.equals("payment-cell") ? "simulation,grpc" : "grpc"),
                  "--spring.config.location=classpath:application.yml",
                  "--spring.config.additional-location=",
                  "--spring.config.import=",
                  "--server.port=0",
                  "--spring.datasource.url=" + db.getJdbcUrl("postgres", "postgres"),
                  "--spring.datasource.username=postgres",
                  "--spring.datasource.password=",
                  "--spring.kafka.bootstrap-servers=127.0.0.1:" + fixture.kafkaTrap.getLocalPort(),
                  "--spring.kafka.listener.auto-startup=false",
                  "--spring.kafka.consumer.group-id=m32-" + id + "-" + UUID.randomUUID(),
                  "--apex.cell.id=" + id,
                  "--apex.messaging.cell.enabled=true",
                  "--apex.messaging.consumer.enabled=" + id.equals("payment-cell"),
                  "--apex.messaging.consumer.reliability.enabled=false",
                  "--apex.messaging.outbox.publisher.enabled=false",
                  "--apex.grpc.server.enabled=true",
                  "--apex.grpc.cell-id=" + id,
                  "--spring.grpc.server.address=127.0.0.1",
                  "--spring.grpc.server.port=" + port,
                  "--spring.grpc.server.ssl.bundle=m32",
                  "--spring.grpc.server.ssl.client-auth=require",
                  "--spring.grpc.server.ssl.enabled=true",
                  "--spring.grpc.server.health.enabled=false",
                  "--spring.grpc.server.reflection.enabled=false",
                  "--apex.grpc.security.enabled=true",
                  "--apex.grpc.security.tenant=alpha",
                  "--logging.level.com.apex.platform.grpc.internal.RpcDispatcher=DEBUG"));
      args.addAll(fixture.certs.bundle(fixture.certs.server));
      args.add("--apex.grpc.security.peers.primary.uri=" + PRIMARY);
      String grants =
          "QUERY:GET_ORDER_PERSPECTIVE:1"
              + (id.equals("order-cell")
                  ? ",QUERY:GET_ORDER:1,REQUEST:CREATE_ORDER:1,REQUEST:CREATE_ORDER:2"
                  : "");
      args.add("--apex.grpc.security.peers.primary.contracts=" + grants);
      args.add("--apex.grpc.security.peers.observer.uri=" + OBSERVER);
      args.add(
          "--apex.grpc.security.peers.observer.contracts="
              + (id.equals("order-cell") ? grants : ""));
      args.add(
          "--apex.grpc.security.peers.foreign.uri=spiffe://apex.test/tenant/beta/cell/m32-foreign");
      args.add("--apex.grpc.security.peers.foreign.contracts=" + grants);
      process = GrpcOrderIT.launch(fixture.root.resolve(id + "/target/" + id + ".jar"), log, args);
      fixture.childPids.add(process.pid());
      fixture.children.add(process);
      try {
        await()
            .atMost(Duration.ofSeconds(45))
            .untilAsserted(
                () -> {
                  assertThat(process.isAlive())
                      .withFailMessage("Startup failed; inspect %s", log)
                      .isTrue();
                  assertThat(GrpcOrderIT.read(log)).contains("gRPC Server started");
                });
      } catch (Exception | Error failure) {
        stop();
        throw failure;
      }
    }

    void stop() throws Exception {
      GrpcOrderIT.stop(process);
    }

    @Override
    public void close() throws Exception {
      try {
        stop();
      } finally {
        db.close();
      }
    }
  }

  static final class Client implements AutoCloseable {
    final Process process;
    final Path log;
    final BufferedWriter input;
    int commands;

    Client(Path jar, Path log, List<String> args, Fixture fixture) throws Exception {
      this.log = log;
      process = GrpcOrderIT.launch(jar, log, args);
      fixture.childPids.add(process.pid());
      fixture.children.add(process);
      input =
          new BufferedWriter(
              new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));
      try {
        await()
            .atMost(Duration.ofSeconds(30))
            .untilAsserted(
                () -> {
                  assertThat(process.isAlive())
                      .withFailMessage("Client startup failed; inspect %s", log)
                      .isTrue();
                  assertThat(GrpcOrderIT.read(log)).contains("M31_CLIENT_READY");
                });
      } catch (Exception | Error failure) {
        try {
          close();
        } catch (Exception cleanup) {
          failure.addSuppressed(cleanup);
        }
        throw failure;
      }
    }

    JsonNode call(Map<String, Object> command) throws IOException {
      int index = commands++;
      input.write(JSON.writeValueAsString(command));
      input.newLine();
      input.flush();
      await()
          .atMost(Duration.ofSeconds(12))
          .untilAsserted(
              () -> {
                assertThat(process.isAlive()).isTrue();
                assertThat(replies().size()).isGreaterThan(index);
              });
      var result = JSON.readTree(replies().get(index).substring("M31_REPLY ".length()));
      assertThat(result.path("contextCleared").asBoolean()).isTrue();
      return result.path("result");
    }

    List<String> replies() {
      return GrpcOrderIT.read(log).lines().filter(l -> l.startsWith("M31_REPLY ")).toList();
    }

    @Override
    public void close() throws Exception {
      try {
        if (process.isAlive()) {
          input.write("EXIT\n");
          input.flush();
          process.waitFor(5, TimeUnit.SECONDS);
        }
      } finally {
        try {
          GrpcOrderIT.stop(process);
        } finally {
          input.close();
        }
      }
    }
  }
}
