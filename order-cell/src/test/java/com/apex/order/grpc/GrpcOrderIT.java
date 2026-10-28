package com.apex.order.grpc;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Real packaged business processes, real HTTP/2 gRPC, owned PostgreSQL/KRaft only. */
@Tag("integration")
class GrpcOrderIT {
  static final JsonMapper JSON = JsonMapper.builder().build();
  static final String TOPIC = "apex.order.events.v1";
  final List<Map<String, Object>> evidence = new ArrayList<>();

  @Test
  void packagedJavaClientCreatesAndQueriesCommittedOrdersWithSafeRetry() throws Exception {
    Path root = Path.of(System.getProperty("grpc.repositoryRoot")).toAbsolutePath().normalize();
    Path output = root.resolve("order-cell/target");
    Path serverJar = output.resolve("order-cell.jar");
    Path clientJar = root.resolve("sample-grpc-client/target/sample-grpc-client.jar");
    assertThat(serverJar).isRegularFile();
    assertThat(clientJar).isRegularFile();
    var broker = new EmbeddedKafkaKraftBroker(1, 1, TOPIC, TOPIC + "-dlt");
    broker.brokerProperty("auto.create.topics.enable", "false");
    broker.brokerListProperty("apex.m31.disposable.brokers");
    Process server = null;
    try (var db =
        EmbeddedPostgres.builder()
            .setServerConfig("fsync", "on")
            .setServerConfig("synchronous_commit", "on")
            .setServerConfig("full_page_writes", "on")
            .start()) {
      try {
        broker.afterPropertiesSet();
        int grpcPort = freePort(), httpPort = freePort();
        var jdbc = new JdbcTemplate(db.getPostgresDatabase());
        String group = "m31-owned-" + UUID.randomUUID();
        Path serverLog = output.resolve("grpc-order-process.log");
        List<String> config =
            List.of(
                "--spring.profiles.active=grpc",
                "--server.port=" + httpPort,
                "--spring.config.location=classpath:application.yml",
                "--spring.config.import=",
                "--spring.config.additional-location=",
                "--spring.datasource.url=" + db.getJdbcUrl("postgres", "postgres"),
                "--spring.datasource.username=postgres",
                "--spring.datasource.password=",
                "--spring.kafka.bootstrap-servers=" + broker.getBrokersAsString(),
                "--spring.kafka.consumer.group-id=" + group,
                "--spring.kafka.listener.auto-startup=false",
                "--apex.messaging.consumer.enabled=false",
                "--apex.messaging.cell.enabled=true",
                "--apex.messaging.publisher.topic=" + TOPIC,
                "--apex.messaging.outbox.publisher.enabled=true",
                "--apex.messaging.outbox.publisher.interval-ms=100",
                "--apex.grpc.cell-id=order-cell",
                "--apex.grpc.server.enabled=true",
                "--spring.grpc.server.address=127.0.0.1",
                "--spring.grpc.server.port=" + grpcPort,
                "--spring.grpc.server.ssl.enabled=false",
                "--spring.grpc.server.reflection.enabled=false",
                "--apex.grpc.insecure-loopback-test-only=true",
                "--apex.grpc.test-clients[0]=m31-client",
                "--apex.grpc.test-clients[1]=m31-other");
        server = launch(serverJar, serverLog, config);
        ready(server, serverLog);
        long firstServerPid = server.pid();
        try (var proxy = new ReplyLossProxy(grpcPort);
            var client =
                new Client(
                    clientJar,
                    output.resolve("grpc-client-process.log"),
                    proxy.port(),
                    "m31-client");
            var other =
                new Client(
                    clientJar, output.resolve("grpc-other-process.log"), grpcPort, "m31-other");
            var admin = Admin.create(Map.of("bootstrap.servers", broker.getBrokersAsString()))) {
          evidence.add(
              Map.of(
                  "stage",
                  "processes",
                  "serverPid",
                  server.pid(),
                  "clientPid",
                  client.process.pid(),
                  "otherClientPid",
                  other.process.pid(),
                  "grpcPort",
                  grpcPort,
                  "httpPort",
                  httpPort,
                  "databasePort",
                  db.getPort(),
                  "broker",
                  broker.getBrokersAsString(),
                  "group",
                  group));
          Map<String, Object> create = command("create");
          JsonNode created = client.call(create);
          UUID order = UUID.fromString(created.path("orderId").asText());
          assertThat(created.path("status").asText()).isEqualTo("PENDING_INVENTORY");
          assertCommitted(jdbc, create, order);
          assertThat(client.call(create)).isEqualTo(created);
          var newId = new LinkedHashMap<>(create);
          newId.put("requestId", UUID.randomUUID().toString());
          newId.put("action", "async");
          assertThat(client.call(newId)).isEqualTo(created);
          var changed = new LinkedHashMap<>(create);
          changed.put("quantity", 2);
          error(client.call(changed), "ALREADY_EXISTS", false);
          error(other.call(create), "ALREADY_EXISTS", false);
          var changedKey = new LinkedHashMap<>(create);
          changedKey.put("key", "other-key-" + UUID.randomUUID());
          changedKey.put("externalReference", "other-reference-" + UUID.randomUUID());
          error(client.call(changedKey), "ALREADY_EXISTS", false);
          assertThat(
                  count(jdbc, "orders", "external_reference", changedKey.get("externalReference")))
              .isZero();
          assertThat(count(jdbc, "order_request_receipt", "idempotency_key", changedKey.get("key")))
              .isZero();
          assertThat(
                  jdbc.queryForObject(
                      "SELECT count(*) FROM outbox_event WHERE causation_id=?",
                      Long.class,
                      UUID.fromString(create.get("requestId").toString())))
              .isEqualTo(1);
          var concurrent = command("concurrent");
          var pair = client.call(concurrent);
          assertThat(pair.size()).isEqualTo(2);
          assertThat(pair.get(0)).isEqualTo(pair.get(1));
          assertCommitted(jdbc, concurrent, UUID.fromString(pair.get(0).path("orderId").asText()));
          for (String action : List.of("version99", "mismatch")) {
            var invalid = command(action);
            error(
                client.call(invalid),
                action.equals("version99") ? "UNIMPLEMENTED" : "INVALID_ARGUMENT",
                false);
            assertThat(
                    count(jdbc, "orders", "external_reference", invalid.get("externalReference")))
                .isZero();
          }
          await()
              .atMost(Duration.ofSeconds(20))
              .untilAsserted(
                  () ->
                      assertThat(
                              jdbc.queryForObject(
                                  "SELECT count(*) FROM outbox_event WHERE published=false",
                                  Long.class))
                          .isZero());
          long outbox = total(jdbc, "outbox_event"),
              receipts = total(jdbc, "order_request_receipt"),
              end = end(admin);
          var query = Map.<String, Object>of("action", "query", "orderId", order.toString());
          assertThat(client.call(query)).isEqualTo(created);
          error(
              client.call(Map.of("action", "query", "orderId", UUID.randomUUID().toString())),
              "NOT_FOUND",
              false);
          error(
              client.call(Map.of("action", "query99", "orderId", order.toString())),
              "UNIMPLEMENTED",
              false);
          assertThat(total(jdbc, "outbox_event")).isEqualTo(outbox);
          assertThat(total(jdbc, "order_request_receipt")).isEqualTo(receipts);
          assertThat(total(jdbc, "processed_event")).isZero();
          assertThat(end(admin)).isEqualTo(end);

          // Genuine PostgreSQL deferred COMMIT failure: callback has returned, outer commit must
          // fail.
          jdbc.execute(
              "CREATE TABLE rpc_test_fault(key text PRIMARY KEY, delay_ms integer NOT NULL)");
          jdbc.execute(
              """
              CREATE FUNCTION rpc_test_commit() RETURNS trigger LANGUAGE plpgsql AS $$
              DECLARE delay integer; BEGIN
                SELECT delay_ms INTO delay FROM rpc_test_fault WHERE key=NEW.idempotency_key;
                IF FOUND THEN
                  IF delay < 0 THEN RAISE EXCEPTION 'M31 isolated deferred commit fault'; END IF;
                  PERFORM pg_sleep(delay/1000.0);
                END IF; RETURN NEW;
              END $$
              """);
          jdbc.execute(
              "CREATE CONSTRAINT TRIGGER rpc_test_commit AFTER INSERT ON order_request_receipt"
                  + " DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION"
                  + " rpc_test_commit()");
          var commitFailure = command("create");
          jdbc.update("INSERT INTO rpc_test_fault VALUES (?,-1)", commitFailure.get("key"));
          error(client.call(commitFailure), "INTERNAL", true);
          assertRolledBack(jdbc, commitFailure);
          jdbc.update("DELETE FROM rpc_test_fault WHERE key=?", commitFailure.get("key"));
          var recovered = client.call(commitFailure);
          assertCommitted(jdbc, commitFailure, UUID.fromString(recovered.path("orderId").asText()));
          evidence.add(
              Map.of(
                  "stage",
                  "deferred-commit",
                  "rollback",
                  true,
                  "retryOrderId",
                  recovered.path("orderId").asText()));

          // Query deadlines are not mutation outcomes. A test-owned SQL lock forces an actual
          // remote timeout.
          try (var lock = db.getPostgresDatabase().getConnection()) {
            lock.setAutoCommit(false);
            lock.createStatement().execute("LOCK TABLE orders IN ACCESS EXCLUSIVE MODE");
            error(
                client.call(
                    Map.of("action", "query", "orderId", order.toString(), "deadlineMs", 50)),
                "DEADLINE_EXCEEDED",
                false);
            lock.rollback();
          }
          assertThat(client.call(query)).isEqualTo(created);
          var timeout = command("create");
          timeout.put("deadlineMs", 100);
          jdbc.update("INSERT INTO rpc_test_fault VALUES (?,750)", timeout.get("key"));
          error(client.call(timeout), "DEADLINE_EXCEEDED", true);
          jdbc.update("DELETE FROM rpc_test_fault WHERE key=?", timeout.get("key"));
          timeout.put("deadlineMs", 3000);
          var timeoutRetry = client.call(timeout);
          assertCommitted(jdbc, timeout, UUID.fromString(timeoutRetry.path("orderId").asText()));
          var cancel = command("cancel");
          error(client.call(cancel), "CANCELLED", true);
          cancel.put("action", "create");
          var cancelRetry = client.call(cancel);
          assertCommitted(jdbc, cancel, UUID.fromString(cancelRetry.path("orderId").asText()));

          // Drop only server-to-client TCP bytes after handshake; real request still commits on the
          // owner.
          assertThat(client.call(query)).isEqualTo(created);
          var lost = command("create");
          lost.put("deadlineMs", 5000);
          proxy.dropReplies = true;
          int pending = client.send(lost);
          await()
              .atMost(Duration.ofSeconds(10))
              .untilAsserted(
                  () ->
                      assertThat(
                              count(
                                  jdbc,
                                  "order_request_receipt",
                                  "idempotency_key",
                                  lost.get("key")))
                          .isEqualTo(1));
          UUID lostOrder =
              jdbc.queryForObject(
                  "SELECT id FROM orders WHERE external_reference=?",
                  UUID.class,
                  lost.get("externalReference"));
          assertCommitted(jdbc, lost, lostOrder);
          server.destroyForcibly();
          assertThat(server.waitFor(10, TimeUnit.SECONDS)).isTrue();
          proxy.disconnect();
          JsonNode lostReply = client.receive(pending);
          assertThat(lostReply.path("error").asText()).isIn("UNAVAILABLE", "DEADLINE_EXCEEDED");
          assertThat(lostReply.path("outcomeUnknown").asBoolean()).isTrue();
          proxy.dropReplies = false;
          server = launch(serverJar, output.resolve("grpc-order-restarted.log"), config);
          ready(server, output.resolve("grpc-order-restarted.log"));
          await()
              .atMost(Duration.ofSeconds(20))
              .untilAsserted(() -> assertThat(client.call(query)).isEqualTo(created));
          lost.put("deadlineMs", 3000);
          assertThat(client.call(lost).path("orderId").asText()).isEqualTo(lostOrder.toString());
          assertCommitted(jdbc, lost, lostOrder);
          evidence.add(
              Map.of(
                  "stage",
                  "commit-before-lost-reply-restart",
                  "firstPid",
                  firstServerPid,
                  "restartedPid",
                  server.pid(),
                  "requestId",
                  lost.get("requestId"),
                  "correlationId",
                  lost.get("correlationId"),
                  "sameOrderId",
                  lostOrder,
                  "unknown",
                  true));
          try (var unavailable =
              new Client(
                  clientJar,
                  output.resolve("grpc-unavailable-client.log"),
                  freePort(),
                  "m31-client")) {
            error(unavailable.call(command("create")), "UNAVAILABLE", true);
            error(unavailable.call(query), "UNAVAILABLE", false);
          }
          // Independent Kafka client proves the ordinary durable event, not a request/reply topic.
          try (var consumer =
              new KafkaConsumer<String, String>(
                  Map.of(
                      "bootstrap.servers",
                      broker.getBrokersAsString(),
                      "key.deserializer",
                      StringDeserializer.class,
                      "value.deserializer",
                      StringDeserializer.class,
                      "enable.auto.commit",
                      false))) {
            var tp = new TopicPartition(TOPIC, 0);
            consumer.assign(List.of(tp));
            consumer.seekToBeginning(List.of(tp));
            var found = new ArrayList<Map<String, Object>>();
            await()
                .atMost(Duration.ofSeconds(20))
                .until(
                    () -> {
                      for (var record : consumer.poll(Duration.ofMillis(100))) {
                        var wire = JSON.readTree(record.value());
                        if (record.key().equals("ORDER:" + order)) {
                          assertThat(wire.path("metadata").path("correlationId").asText())
                              .isEqualTo(create.get("correlationId"));
                          assertThat(wire.path("metadata").path("causationId").asText())
                              .isEqualTo(create.get("requestId"));
                          assertThat(wire.path("metadata").path("eventType").asText())
                              .isEqualTo("ORDER_PLACED");
                          found.add(
                              Map.of(
                                  "topic",
                                  record.topic(),
                                  "partition",
                                  record.partition(),
                                  "offset",
                                  record.offset(),
                                  "eventId",
                                  wire.path("metadata").path("eventId").asText(),
                                  "orderId",
                                  order));
                        }
                      }
                      return !found.isEmpty();
                    });
            assertThat(found).hasSize(1);
            evidence.addAll(found);
          }
          assertThat(total(jdbc, "processed_event")).isZero();
          assertThat(
                  jdbc.queryForObject(
                      "SELECT count(*) FROM order_request_receipt WHERE response IS NULL",
                      Long.class))
              .isZero();
          evidence.add(
              Map.of(
                  "stage",
                  "final",
                  "orders",
                  total(jdbc, "orders"),
                  "receipts",
                  total(jdbc, "order_request_receipt"),
                  "outbox",
                  total(jdbc, "outbox_event"),
                  "inbox",
                  total(jdbc, "processed_event"),
                  "queriesEmitKafka",
                  false));
        }
      } finally {
        stop(server);
        broker.destroy();
        Files.writeString(
            output.resolve("grpc-e2e-evidence.json"),
            JSON.writerWithDefaultPrettyPrinter().writeValueAsString(evidence));
      }
    }
  }

  static LinkedHashMap<String, Object> command(String action) {
    var c = new LinkedHashMap<String, Object>();
    c.put("action", action);
    c.put("key", "rpc-" + UUID.randomUUID());
    c.put("requestId", UUID.randomUUID().toString());
    c.put("correlationId", UUID.randomUUID().toString());
    c.put("externalReference", "grpc-" + UUID.randomUUID());
    c.put("productId", UUID.randomUUID().toString());
    c.put("quantity", 1);
    return c;
  }

  static void error(JsonNode reply, String code, boolean unknown) {
    assertThat(reply.path("error").asText()).isEqualTo(code);
    assertThat(reply.path("outcomeUnknown").asBoolean()).isEqualTo(unknown);
  }

  static long total(JdbcTemplate jdbc, String table) {
    return jdbc.queryForObject("SELECT count(*) FROM " + table, Long.class);
  }

  static long count(JdbcTemplate jdbc, String table, String field, Object id) {
    return jdbc.queryForObject(
        "SELECT count(*) FROM " + table + " WHERE " + field + "=?", Long.class, id);
  }

  static void assertCommitted(JdbcTemplate jdbc, Map<String, Object> cmd, UUID id) {
    assertThat(count(jdbc, "orders", "external_reference", cmd.get("externalReference")))
        .isEqualTo(1);
    assertThat(count(jdbc, "order_request_receipt", "idempotency_key", cmd.get("key")))
        .isEqualTo(1);
    assertThat(count(jdbc, "outbox_event", "aggregate_id", id)).isEqualTo(1);
    assertThat(
            jdbc.queryForObject(
                "SELECT completed_at IS NOT NULL FROM order_request_receipt WHERE"
                    + " idempotency_key=?",
                Boolean.class,
                cmd.get("key")))
        .isTrue();
  }

  static void assertRolledBack(JdbcTemplate jdbc, Map<String, Object> cmd) {
    assertThat(count(jdbc, "orders", "external_reference", cmd.get("externalReference"))).isZero();
    assertThat(count(jdbc, "order_request_receipt", "idempotency_key", cmd.get("key"))).isZero();
    assertThat(
            jdbc.queryForObject(
                "SELECT count(*) FROM outbox_event WHERE causation_id=?",
                Long.class,
                UUID.fromString(cmd.get("requestId").toString())))
        .isZero();
  }

  static long end(Admin admin) throws Exception {
    return admin
        .listOffsets(
            Map.of(
                new TopicPartition(TOPIC, 0), org.apache.kafka.clients.admin.OffsetSpec.latest()))
        .all()
        .get(5, TimeUnit.SECONDS)
        .values()
        .iterator()
        .next()
        .offset();
  }

  static int freePort() throws IOException {
    try (var s = new ServerSocket(0)) {
      return s.getLocalPort();
    }
  }

  static Process launch(Path jar, Path log, List<String> args) throws IOException {
    var cmd =
        new ArrayList<>(
            List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xms64m",
                "-Xmx256m",
                "-Dfile.encoding=UTF-8",
                "-jar",
                jar.toString()));
    cmd.addAll(args);
    var builder = new ProcessBuilder(cmd).redirectErrorStream(true).redirectOutput(log.toFile());
    builder
        .environment()
        .keySet()
        .removeIf(
            k ->
                k.matches(
                    "(?i)(SPRING_.*|APEX_.*|CELL_.*|ORDER_.*|KAFKA_.*|JAVA_TOOL_OPTIONS|JDK_JAVA_OPTIONS|_JAVA_OPTIONS)"));
    return builder.start();
  }

  static String read(Path file) {
    try {
      return Files.readString(file);
    } catch (IOException e) {
      return "";
    }
  }

  static void ready(Process p, Path log) {
    await()
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(
            () -> {
              assertThat(p.isAlive()).withFailMessage("Child failed; inspect %s", log).isTrue();
              assertThat(read(log)).contains("Started OrderCellApplication");
            });
  }

  static void stop(Process p) throws InterruptedException {
    if (p != null && p.isAlive()) {
      p.destroy();
      if (!p.waitFor(10, TimeUnit.SECONDS)) {
        p.destroyForcibly();
        assertThat(p.waitFor(10, TimeUnit.SECONDS)).isTrue();
      }
    }
  }

  static final class Client implements AutoCloseable {
    final Process process;
    final Path log;
    final BufferedWriter input;
    int commands;

    Client(Path jar, Path log, int port, String identity) throws Exception {
      this.log = log;
      process =
          launch(
              jar,
              log,
              List.of(
                  "--spring.main.web-application-type=none",
                  "--spring.config.location=optional:classpath:/no-external-m31.yml",
                  "--apex.grpc.client.enabled=true",
                  "--apex.grpc.cell-id=" + identity,
                  "--apex.grpc.insecure-loopback-test-only=true",
                  "--apex.grpc.destinations.order-cell.channel=order",
                  "--spring.grpc.client.channel.order.target=static://127.0.0.1:" + port,
                  "--spring.grpc.client.channel.order.ssl.enabled=false"));
      input =
          new BufferedWriter(
              new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));
      try {
        await()
            .atMost(Duration.ofSeconds(30))
            .untilAsserted(
                () -> {
                  assertThat(process.isAlive()).isTrue();
                  assertThat(read(log)).contains("M31_CLIENT_READY");
                });
      } catch (Exception | Error failure) {
        try {
          stop(process);
          input.close();
        } catch (Exception cleanup) {
          failure.addSuppressed(cleanup);
        }
        throw failure;
      }
    }

    int send(Map<String, Object> cmd) throws IOException {
      int index = commands++;
      input.write(JSON.writeValueAsString(cmd));
      input.newLine();
      input.flush();
      return index;
    }

    JsonNode receive(int index) {
      await()
          .atMost(Duration.ofSeconds(12))
          .untilAsserted(
              () -> {
                assertThat(process.isAlive()).isTrue();
                assertThat(replies()).hasSizeGreaterThan(index);
              });
      var result = JSON.readTree(replies().get(index).substring("M31_REPLY ".length()));
      assertThat(result.path("contextCleared").asBoolean()).isTrue();
      return result.path("result");
    }

    List<String> replies() {
      return read(log).lines().filter(l -> l.startsWith("M31_REPLY ")).toList();
    }

    JsonNode call(Map<String, Object> cmd) throws IOException {
      return receive(send(cmd));
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
          stop(process);
        } finally {
          input.close();
        }
      }
    }
  }

  /** Fault-only byte tunnel; never used as an application transport implementation. */
  static final class ReplyLossProxy implements AutoCloseable {
    final ServerSocket listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
    final ExecutorService threads =
        Executors.newCachedThreadPool(
            r -> {
              var t = new Thread(r, "m31-test-byte-proxy");
              t.setDaemon(true);
              return t;
            });
    final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
    final int target;
    volatile boolean dropReplies;

    ReplyLossProxy(int target) throws IOException {
      this.target = target;
      threads.submit(
          () -> {
            while (!listener.isClosed()) {
              Socket client = null;
              try {
                client = listener.accept();
                Socket server = new Socket("127.0.0.1", target);
                sockets.add(client);
                sockets.add(server);
                Socket accepted = client;
                threads.submit(() -> relay(accepted, server, false));
                threads.submit(() -> relay(server, accepted, true));
              } catch (IOException disconnected) {
                if (client != null)
                  try {
                    client.close();
                  } catch (IOException ignored) {
                  }
              }
            }
          });
    }

    int port() {
      return listener.getLocalPort();
    }

    void relay(Socket from, Socket to, boolean reply) {
      try {
        byte[] buffer = new byte[8192];
        int n;
        while ((n = from.getInputStream().read(buffer)) >= 0) {
          if (!(reply && dropReplies)) {
            to.getOutputStream().write(buffer, 0, n);
            to.getOutputStream().flush();
          }
        }
      } catch (IOException ignored) {
      } finally {
        try {
          from.close();
          to.close();
        } catch (IOException ignored) {
        }
        sockets.remove(from);
        sockets.remove(to);
      }
    }

    void disconnect() {
      for (var s : sockets)
        try {
          s.close();
        } catch (IOException ignored) {
        }
    }

    @Override
    public void close() throws IOException {
      listener.close();
      disconnect();
      threads.shutdownNow();
    }
  }
}
