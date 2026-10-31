package com.apex.order.grpc;

import static com.apex.order.grpc.GrpcOrderIT.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.*;
import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

/** Real registry/edge/client and 3+2+2 packaged Cell JVMs, exactly three owning databases. */
@Tag("integration")
class DualDiscoveryReplicaIT {
  static String sha(Path path) throws Exception {
    return HexFormat.of()
        .formatHex(
            java.security.MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path)));
  }

  static final List<String> SOURCES =
      List.of(
          "apex.order.events.v1",
          "apex.inventory.events.v1",
          "apex.order.payment-requests.v1",
          "apex.order.inventory-release-requests.v1",
          "apex.payment.events.v1",
          "apex.order.legacy.events.v1");
  static final String PRIMARY = "spiffe://apex.test/tenant/alpha/cell/m32-client";

  @Test
  void realEurekaHttpAndGrpcReplicasPreserveSagaAndRequestReceipts() throws Exception {
    try (var f = new Fleet()) {
      f.start();
      UUID happy = f.place("happy", 2, "AUTHORIZED");
      f.status(happy, "CONFIRMED");
      f.stock(happy, 8, 2);
      f.httpDistribution(happy);
      f.grpcDistribution(happy, 2);
      f.compensatesOnlyAfterRelease();
      f.rejectsStockWithoutPayment();
      f.sameBusinessDuplicates(happy);
      f.lostReplyRetriesOnOtherReplica(happy);
      f.addOrderReplica();
      f.grpcDistribution(happy, 3);
      f.publisherMarkFailureAndConcurrentClaims();
      f.concurrentReserveAndRelease();
      f.replicaStopAndRebalance();
      f.drain();
      f.assertInvariants();
      f.securityAndDiscoveryNegatives(happy);
      f.evidence.add(
          Map.of("stage", "business", "result", "PASS", "orders", total(f.orders, "orders")));
      f.databaseLossChangesReadinessNotLiveness();
    }
  }

  static final class Fleet implements AutoCloseable {
    final Path root =
        Path.of(System.getProperty("grpc.repositoryRoot")).toAbsolutePath().normalize();
    final Path output = root.resolve("order-cell/target/m34-evidence");
    final List<Map<String, Object>> evidence = new ArrayList<>();
    final List<Process> children = new ArrayList<>();
    final List<Node> nodes = new ArrayList<>();
    final Map<String, EmbeddedPostgres> databases = new LinkedHashMap<>();
    final Map<UUID, UUID> productForOrder = new HashMap<>();
    final Map<UUID, Integer> initialStock = new HashMap<>();
    final Map<String, String> groups =
        Map.of(
            "order-cell",
            "m34-order-" + UUID.randomUUID(),
            "inventory-cell",
            "m34-inventory-" + UUID.randomUUID(),
            "payment-cell",
            "m34-payment-" + UUID.randomUUID());
    EmbeddedKafkaKraftBroker broker;
    EphemeralCertificates certs;
    Admin admin;
    HttpClient http;
    JdbcTemplate orders, inventory, payments;
    Process registry, gateway;
    Client client;
    int registryPort, edgePort;
    String brokers;
    boolean closed;

    void start() throws Exception {
      Files.createDirectories(output);
      var receipt = JSON.readTree(Files.readAllBytes(root.resolve("out/m34-build-receipt.json")));
      for (var file : receipt.path("files"))
        assertThat(sha(root.resolve(file.path("path").asText())))
            .as(file.path("path").asText())
            .isEqualTo(file.path("sha256").asText());
      http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
      certs = new EphemeralCertificates();
      var topics = new ArrayList<String>();
      for (String t : SOURCES) {
        topics.add(t);
        topics.add(t + "-dlt");
      }
      broker = new EmbeddedKafkaKraftBroker(1, 3, topics.toArray(String[]::new));
      broker.brokerProperty("auto.create.topics.enable", "false");
      broker.brokerListProperty("apex.m34.owned-brokers");
      broker.afterPropertiesSet();
      brokers = broker.getBrokersAsString();
      admin = Admin.create(Map.of("bootstrap.servers", brokers));
      for (String owner : List.of("order-cell", "inventory-cell", "payment-cell")) {
        var db =
            EmbeddedPostgres.builder()
                .setServerConfig("fsync", "on")
                .setServerConfig("synchronous_commit", "on")
                .setServerConfig("full_page_writes", "on")
                .start();
        databases.put(owner, db);
      }
      orders = jdbc("order-cell");
      inventory = jdbc("inventory-cell");
      payments = jdbc("payment-cell");
      registryPort = freePort();
      registry =
          launchOwned(
              "apex-discovery-server",
              "registry.log",
              List.of(
                  "--spring.config.location=classpath:application.properties",
                  "--server.port=" + registryPort,
                  "--apex.registry.loopback-development=true",
                  "--eureka.server.enable-self-preservation=false",
                  "--eureka.server.use-read-only-response-cache=false",
                  "--eureka.server.eviction-interval-timer-in-ms=1000",
                  "--eureka.server.wait-time-in-ms-when-sync-empty=0"));
      readyProcess(registry, output.resolve("registry.log"));
      for (int replica = 1; replica <= 2; replica++)
        for (String owner : List.of("order-cell", "inventory-cell", "payment-cell")) {
          var node = new Node(this, owner, replica);
          nodes.add(node);
          node.start();
          if (owner.equals("order-cell") && replica == 1) installAudit();
        }
      for (String cell : groups.keySet()) registrations(cell, 2);
      edgePort = freePort();
      var edge = new ArrayList<>(eureka(false));
      edge.addAll(
          List.of(
              "--spring.config.location=classpath:application.properties",
              "--server.port=" + edgePort,
              "--apex.gateway.mode=loopback-development",
              "--apex.discovery.targets.order-cell.authority=localhost"));
      gateway = launchOwned("apex-api-gateway", "gateway.log", edge);
      readyProcess(gateway, output.resolve("gateway.log"));
      client = new Client(this, "client", certs.primary, "m32-client", List.of());
      assignments("initial", 2, 2, 2);
      evidence.add(
          Map.of(
              "stage",
              "topology",
              "registryPid",
              registry.pid(),
              "gatewayPid",
              gateway.pid(),
              "clientPid",
              client.process.pid(),
              "databases",
              databases.entrySet().stream()
                  .map(e -> Map.of("owner", e.getKey(), "port", e.getValue().getPort()))
                  .toList(),
              "groups",
              groups,
              "partitions",
              3,
              "broker",
              brokers));
    }

    Path artifact(String name) {
      return root.resolve("out/m34-artifacts/" + name + ".jar");
    }

    JdbcTemplate jdbc(String owner) {
      return new JdbcTemplate(databases.get(owner).getPostgresDatabase());
    }

    Process launchOwned(String app, String filename, List<String> options) throws IOException {
      var cmd =
          new ArrayList<>(
              List.of(
                  Path.of(System.getProperty("java.home"), "bin/java").toString(),
                  "-Xms64m",
                  "-Xmx256m",
                  "-Dfile.encoding=UTF-8",
                  "-jar",
                  artifact(app).toString()));
      cmd.addAll(options);
      var p =
          new ProcessBuilder(cmd)
              .redirectErrorStream(true)
              .redirectOutput(output.resolve(filename).toFile());
      p.environment()
          .keySet()
          .removeIf(
              k ->
                  k.matches(
                      "(?i)(SPRING_.*|APEX_.*|CELL_.*|ORDER_.*|INVENTORY_.*|PAYMENT_.*|KAFKA_.*|EUREKA_.*|KUBERNETES_.*|SERVER_.*|JAVA_TOOL_OPTIONS|JDK_JAVA_OPTIONS|_JAVA_OPTIONS)"));
      var child = p.start();
      children.add(child);
      return child;
    }

    void readyProcess(Process process, Path log) {
      await()
          .atMost(Duration.ofSeconds(60))
          .pollInterval(Duration.ofMillis(100))
          .untilAsserted(
              () -> {
                if (read(log).contains("Application run failed"))
                  throw new AssertionError("Owned child startup failed; inspect " + log);
                assertThat(process.isAlive())
                    .withFailMessage("Inspect owned child %s", log)
                    .isTrue();
                assertThat(read(log)).containsPattern("Started [A-Za-z0-9]+Application in ");
              });
    }

    List<String> eureka(boolean register) {
      return new ArrayList<>(
          List.of(
              "--spring.profiles.active=eureka",
              "--server.address=127.0.0.1",
              "--apex.discovery.provider=eureka",
              "--apex.discovery.insecure-loopback-test-only=true",
              "--apex.discovery.registration.enabled=" + register,
              "--apex.discovery.max-stale-ms=5000",
              "--eureka.client.service-url.defaultZone=http://127.0.0.1:"
                  + registryPort
                  + "/eureka/",
              "--eureka.client.registry-fetch-interval-seconds=1",
              "--eureka.client.initial-instance-info-replication-interval-seconds=1",
              "--eureka.instance.lease-renewal-interval-in-seconds=1",
              "--eureka.instance.lease-expiration-duration-in-seconds=4",
              "--eureka.instance.hostname=127.0.0.1",
              "--spring.cloud.loadbalancer.retry.enabled=false"));
    }

    void registrations(String cell, int expected) {
      var observed = new ArrayList<Map<String, Object>>();
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () -> {
                var r =
                    http.send(
                        HttpRequest.newBuilder(
                                URI.create(
                                    "http://127.0.0.1:"
                                        + registryPort
                                        + "/eureka/apps/"
                                        + cell.toUpperCase(Locale.ROOT)))
                            .header("Accept", "application/json")
                            .timeout(Duration.ofSeconds(3))
                            .GET()
                            .build(),
                        HttpResponse.BodyHandlers.ofString());
                assertThat(r.statusCode()).isEqualTo(200);
                var instances = JSON.readTree(r.body()).path("application").path("instance");
                int count = 0;
                observed.clear();
                for (var i : instances) if ("UP".equals(i.path("status").asText())) count++;
                assertThat(count).isEqualTo(expected);
                for (var i : instances)
                  if ("UP".equals(i.path("status").asText()))
                    observed.add(
                        Map.of(
                            "instanceId",
                            i.path("instanceId").asText(),
                            "host",
                            i.path("hostName").asText(),
                            "httpPort",
                            i.path("port").path("$").asInt(),
                            "grpcPort",
                            i.path("metadata").path("apex.grpc.port").asText(),
                            "authority",
                            i.path("metadata").path("apex.grpc.authority").asText()));
              });
      evidence.add(
          Map.of(
              "stage", "eureka-registrations", "cell", cell, "instances", List.copyOf(observed)));
    }

    void installAudit() {
      orders.execute(
          "CREATE TABLE m34_claims(event_id uuid,token uuid,publisher text,claimed_at"
              + " timestamptz,cleared_at timestamptz)");
      orders.execute(
          """
          CREATE FUNCTION m34_claim_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
          IF NEW.claim_token IS NOT NULL AND NEW.claim_token IS DISTINCT FROM OLD.claim_token THEN
            INSERT INTO m34_claims VALUES(NEW.event_id,NEW.claim_token,NEW.publisher_instance_id,clock_timestamp(),NULL);
          END IF;
          IF OLD.claim_token IS NOT NULL AND NEW.claim_token IS NULL THEN
            UPDATE m34_claims SET cleared_at=clock_timestamp() WHERE event_id=NEW.event_id AND token=OLD.claim_token;
          END IF; RETURN NEW; END $$
          """);
      orders.execute(
          "CREATE TRIGGER m34_claim_audit AFTER UPDATE ON outbox_event FOR EACH ROW EXECUTE"
              + " FUNCTION m34_claim_audit()");
    }

    UUID place(String label, int quantity, String decision) throws Exception {
      UUID product = UUID.randomUUID();
      inventory.update("INSERT INTO inventory_stock VALUES (?,10,0)", product);
      initialStock.put(product, 10);
      UUID id = post(label, product, quantity);
      if (decision != null)
        payments.update("INSERT INTO payment_simulation_policy VALUES (?,?)", id, decision);
      productForOrder.put(id, product);
      return id;
    }

    UUID post(String label, UUID product, int quantity) throws Exception {
      var body =
          Map.of(
              "externalReference",
              "m34-" + label + "-" + UUID.randomUUID(),
              "description",
              "owned multi-instance test",
              "items",
              List.of(Map.of("productId", product, "quantity", quantity)));
      var response =
          http.send(
              HttpRequest.newBuilder(
                      URI.create("http://127.0.0.1:" + edgePort + "/api/v1/order-placements"))
                  .timeout(Duration.ofSeconds(10))
                  .header("Content-Type", "application/json")
                  .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)))
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      assertThat(response.statusCode()).as(response.body()).isEqualTo(201);
      var created = JSON.readTree(response.body());
      assertThat(created.path("status").asText()).isEqualTo("PENDING_INVENTORY");
      return UUID.fromString(created.path("orderId").asText());
    }

    void status(UUID id, String expected) {
      await()
          .atMost(Duration.ofSeconds(45))
          .untilAsserted(
              () ->
                  assertThat(
                          orders.queryForObject(
                              "SELECT status FROM orders WHERE id=?", String.class, id))
                      .isEqualTo(expected));
    }

    void stock(UUID id, int available, int reserved) {
      var row =
          inventory.queryForMap(
              "SELECT available_quantity,reserved_quantity FROM inventory_stock WHERE product_id=?",
              productForOrder.get(id));
      assertThat(row.get("available_quantity")).isEqualTo(available);
      assertThat(row.get("reserved_quantity")).isEqualTo(reserved);
    }

    void httpDistribution(UUID id) throws Exception {
      var requests = new HashSet<String>();
      String correlation = UUID.randomUUID().toString();
      for (int i = 0; i < 18; i++) {
        var r =
            http.send(
                HttpRequest.newBuilder(
                        URI.create(
                            "http://127.0.0.1:" + edgePort + "/api/v1/order-placements/" + id))
                    .timeout(Duration.ofSeconds(8))
                    .header("X-User", "forged")
                    .header("X-Forwarded-Host", "forged.invalid")
                    .header("X-Correlation-ID", correlation)
                    .GET()
                    .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(r.statusCode()).isEqualTo(200);
        assertThat(JSON.readTree(r.body()).path("status").asText()).isEqualTo("CONFIRMED");
        assertThat(r.headers().firstValue("X-Correlation-ID")).contains(correlation);
        requests.add(r.headers().firstValue("X-Request-ID").orElseThrow());
      }
      var hits = new HashMap<String, Integer>();
      await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(
              () -> {
                hits.clear();
                for (Node n : nodes)
                  if (n.owner.equals("order-cell")) {
                    String access = n.access();
                    int seen =
                        (int)
                            access
                                .lines()
                                .filter(line -> requests.stream().anyMatch(line::contains))
                                .count();
                    if (seen > 0) hits.put(n.instance, seen);
                    assertThat(access).doesNotContain("forged.invalid", "forged");
                  }
                assertThat(hits).hasSizeGreaterThanOrEqualTo(2);
                assertThat(hits.values().stream().mapToInt(Integer::intValue).sum()).isEqualTo(18);
              });
      evidence.add(
          Map.of(
              "stage",
              "gateway-http-distribution",
              "backendAccessLogHits",
              Map.copyOf(hits),
              "requests",
              18));
    }

    void grpcDistribution(UUID id, int replicas) throws Exception {
      var requestIds = new HashSet<String>();
      for (int i = 0; i < 30; i++) {
        var request = new LinkedHashMap<String, Object>();
        request.put("action", "query");
        request.put("orderId", id.toString());
        String requestId = UUID.randomUUID().toString();
        request.put("requestId", requestId);
        requestIds.add(requestId);
        assertThat(client.call(request).path("orderId").asText()).isEqualTo(id.toString());
      }
      var hits = new HashMap<String, Integer>();
      for (Node n : nodes)
        if (n.owner.equals("order-cell") && n.process.isAlive()) {
          int seen =
              (int)
                  n.logs.stream()
                      .flatMap(p -> read(p).lines())
                      .filter(
                          line ->
                              requestIds.stream().anyMatch(x -> line.contains("requestId=" + x)))
                      .count();
          if (seen > 0) hits.put(n.instance, seen);
        }
      assertThat(hits).hasSize(replicas);
      evidence.add(
          Map.of(
              "stage",
              "grpc-per-rpc-distribution",
              "replicas",
              replicas,
              "calls",
              30,
              "hits",
              hits));
    }

    void compensatesOnlyAfterRelease() throws Exception {
      UUID id = place("decline", 3, null);
      status(id, "PENDING_PAYMENT");
      try (var lock = databases.get("inventory-cell").getPostgresDatabase().getConnection()) {
        lock.setAutoCommit(false);
        try (var sql =
            lock.prepareStatement(
                "SELECT product_id FROM inventory_stock WHERE product_id=? FOR UPDATE")) {
          sql.setObject(1, productForOrder.get(id));
          sql.executeQuery().close();
        }
        payments.update("INSERT INTO payment_simulation_policy VALUES (?,'DECLINED')", id);
        status(id, "CANCELLING");
        stock(id, 7, 3);
        assertThat(
                orders.queryForObject(
                    "SELECT release_acknowledged FROM orders WHERE id=?", Boolean.class, id))
            .isFalse();
        lock.commit();
      }
      status(id, "CANCELLED");
      stock(id, 10, 0);
      assertThat(
              inventory.queryForObject(
                  "SELECT status FROM inventory_reservations WHERE order_id=?", String.class, id))
          .isEqualTo("RELEASED");
      evidence.add(
          Map.of(
              "stage",
              "decline-compensation",
              "orderId",
              id,
              "observedCANCELLINGBeforeRelease",
              true,
              "stock",
              List.of(10, 0)));
      duplicate("apex.order.inventory-release-requests.v1", id, "ORDER_CANCELLATION_REQUESTED");
      stock(id, 10, 0);
    }

    void rejectsStockWithoutPayment() throws Exception {
      UUID id = place("stock-rejected", 100, null);
      status(id, "CANCELLED");
      stock(id, 10, 0);
      assertThat(
              payments.queryForObject(
                  "SELECT count(*) FROM payment_authorizations WHERE order_id=?", Long.class, id))
          .isZero();
      assertThat(
              orders.queryForObject(
                  "SELECT count(*) FROM outbox_event WHERE aggregate_id=? AND event_type IN"
                      + " ('ORDER_READY_FOR_PAYMENT','ORDER_CANCELLATION_REQUESTED')",
                  Long.class,
                  id))
          .isZero();
      evidence.add(Map.of("stage", "insufficient-stock", "orderId", id, "paymentAttempts", 0));
    }

    void sameBusinessDuplicates(UUID id) throws Exception {
      duplicate(SOURCES.getFirst(), id, "ORDER_PLACED");
      duplicate("apex.order.payment-requests.v1", id, "ORDER_READY_FOR_PAYMENT");
      stock(id, 8, 2);
      assertThat(
              payments.queryForObject(
                  "SELECT count(*) FROM payment_authorizations WHERE order_id=?", Long.class, id))
          .isEqualTo(1);
      assertThat(
              payments.queryForObject(
                  "SELECT count(*) FROM outbox_event WHERE aggregate_id=?", Long.class, id))
          .isEqualTo(1);
    }

    void duplicate(String topic, UUID order, String type) throws Exception {
      UUID event =
          orders.queryForObject(
              "SELECT event_id FROM outbox_event WHERE aggregate_id=? AND event_type=?",
              UUID.class,
              order,
              type);
      try (var observer =
              new KafkaConsumer<String, String>(
                  Map.of(
                      "bootstrap.servers",
                      brokers,
                      "enable.auto.commit",
                      false,
                      "key.deserializer",
                      StringDeserializer.class,
                      "value.deserializer",
                      StringDeserializer.class));
          var producer =
              new KafkaProducer<String, String>(
                  Map.of(
                      "bootstrap.servers",
                      brokers,
                      "acks",
                      "all",
                      "enable.idempotence",
                      true,
                      "key.serializer",
                      StringSerializer.class,
                      "value.serializer",
                      StringSerializer.class))) {
        var partitions =
            java.util.stream.IntStream.range(0, 3)
                .mapToObj(p -> new TopicPartition(topic, p))
                .toList();
        observer.assign(partitions);
        observer.seekToBeginning(partitions);
        ConsumerRecord<String, String> original = null;
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (original == null && System.nanoTime() < end)
          for (var record : observer.poll(Duration.ofMillis(100))) {
            if (JSON.readTree(record.value())
                .path("metadata")
                .path("eventId")
                .asText()
                .equals(event.toString())) original = record;
          }
        assertThat(original).isNotNull();
        producer
            .send(
                new ProducerRecord<>(topic, original.partition(), original.key(), original.value()))
            .get(5, TimeUnit.SECONDS);
        var copy = (ObjectNode) JSON.readTree(original.value());
        ((ObjectNode) copy.path("metadata")).put("eventId", UUID.randomUUID().toString());
        producer
            .send(
                new ProducerRecord<>(
                    topic, original.partition(), original.key(), JSON.writeValueAsString(copy)))
            .get(5, TimeUnit.SECONDS);
      }
      drain();
      evidence.add(
          Map.of(
              "stage",
              "duplicate",
              "type",
              type,
              "sameEventAndNewBusinessEvent",
              true,
              "orderId",
              order));
    }

    void lostReplyRetriesOnOtherReplica(UUID existing) throws Exception {
      var request = command("create");
      request.put("deadlineMs", 5000);
      for (Node n : nodes) if (n.proxy != null) n.proxy.dropReplies = true;
      int index = client.send(request);
      await()
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(
              () ->
                  assertThat(
                          count(
                              orders,
                              "order_request_receipt",
                              "idempotency_key",
                              request.get("key")))
                      .isEqualTo(1));
      UUID id =
          orders.queryForObject(
              "SELECT id FROM orders WHERE external_reference=?",
              UUID.class,
              request.get("externalReference"));
      assertCommitted(orders, request, id);
      Node receiver =
          nodes.stream()
              .filter(
                  n ->
                      n.owner.equals("order-cell")
                          && n.logs.stream()
                              .anyMatch(
                                  p -> read(p).contains("requestId=" + request.get("requestId"))))
              .findFirst()
              .orElseThrow();
      long killed = receiver.process.pid();
      receiver.process.destroyForcibly();
      assertThat(receiver.process.waitFor(10, TimeUnit.SECONDS)).isTrue();
      receiver.proxy.disconnect();
      for (Node n : nodes) if (n.proxy != null) n.proxy.dropReplies = false;
      var lost = client.receive(index);
      assertThat(lost.path("error").asText()).isIn("UNAVAILABLE", "DEADLINE_EXCEEDED");
      assertThat(lost.path("outcomeUnknown").asBoolean()).isTrue();
      await()
          .atMost(Duration.ofSeconds(15))
          .untilAsserted(
              () ->
                  assertThat(
                          client
                              .call(Map.of("action", "query", "orderId", existing.toString()))
                              .path("orderId")
                              .asText())
                      .isEqualTo(existing.toString()));
      var retried = client.call(request);
      assertThat(retried.path("orderId").asText()).isEqualTo(id.toString());
      assertCommitted(orders, request, id);
      Node alternate =
          nodes.stream()
              .filter(
                  n ->
                      n.owner.equals("order-cell")
                          && n.process.isAlive()
                          && n.logs.stream()
                              .anyMatch(
                                  p -> read(p).contains("requestId=" + request.get("requestId"))))
              .findFirst()
              .orElseThrow();
      assertThat(alternate.process.pid()).isNotEqualTo(killed);
      var changed = new LinkedHashMap<>(request);
      changed.put("quantity", 2);
      assertThat(client.call(changed).path("error").asText()).isEqualTo("ALREADY_EXISTS");
      receiver.start();
      registrations("order-cell", 2);
      evidence.add(
          Map.of(
              "stage",
              "committed-lost-reply-alternate-replica",
              "originalPid",
              killed,
              "retryPid",
              alternate.process.pid(),
              "orderId",
              id,
              "receiptCount",
              1,
              "placedOutboxCount",
              1,
              "requestId",
              request.get("requestId")));
    }

    void addOrderReplica() throws Exception {
      var extra = new Node(this, "order-cell", 3);
      nodes.add(extra);
      extra.start();
      registrations("order-cell", 3);
      assignments("scaled", 3, 2, 2);
      // Wait for resolver reconciliation via read-only calls, not a write retry.
      await()
          .atMost(Duration.ofSeconds(15))
          .untilAsserted(
              () -> {
                String requestId = UUID.randomUUID().toString();
                var id =
                    orders.queryForObject(
                        "SELECT id FROM orders WHERE status='CONFIRMED' LIMIT 1", UUID.class);
                assertThat(
                        client
                            .call(
                                Map.of(
                                    "action",
                                    "query",
                                    "orderId",
                                    id.toString(),
                                    "requestId",
                                    requestId))
                            .path("orderId")
                            .asText())
                    .isEqualTo(id.toString());
                assertThat(read(extra.logs.getLast())).contains("requestId=" + requestId);
              });
    }

    void publisherMarkFailureAndConcurrentClaims() throws Exception {
      orders.execute("CREATE TABLE m34_mark_fault(enabled boolean NOT NULL)");
      orders.update("INSERT INTO m34_mark_fault VALUES(true)");
      orders.execute(
          """
          CREATE FUNCTION m34_fail_mark() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
          IF NEW.published AND NOT OLD.published AND NEW.event_type='ORDER_PLACED'
             AND NEW.payload->>'externalReference' LIKE 'm34-mark-fault-%'
             AND EXISTS(SELECT 1 FROM m34_mark_fault WHERE enabled) THEN
            RAISE EXCEPTION 'm34 isolated published mark COMMIT fault';
          END IF; RETURN NEW; END $$
          """);
      orders.execute(
          "CREATE CONSTRAINT TRIGGER m34_fail_mark AFTER UPDATE ON outbox_event DEFERRABLE"
              + " INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION m34_fail_mark()");
      UUID fault = place("mark-fault", 1, "AUTHORIZED");
      await()
          .atMost(Duration.ofSeconds(15))
          .untilAsserted(
              () -> {
                var row =
                    orders.queryForMap(
                        "SELECT published,retry_count FROM outbox_event WHERE aggregate_id=? AND"
                            + " event_type='ORDER_PLACED'",
                        fault);
                assertThat(row.get("published")).isEqualTo(false);
                assertThat(((Number) row.get("retry_count")).intValue()).isGreaterThan(0);
                assertThat(
                        inventory.queryForObject(
                            "SELECT count(*) FROM inventory_reservations WHERE order_id=?",
                            Long.class,
                            fault))
                    .isEqualTo(1);
              });
      orders.update("UPDATE m34_mark_fault SET enabled=false");
      status(fault, "CONFIRMED");
      drain();
      stock(fault, 9, 1);
      // Test-owned mark delay exposes simultaneous live leases; no delivery algorithm/config
      // change.
      orders.execute(
          """
          CREATE FUNCTION m34_observe_parallel_claims() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
          IF NEW.published AND NOT OLD.published THEN
            IF NEW.payload->>'externalReference' LIKE 'm34-claims-prime-%' THEN PERFORM pg_sleep(1);
            ELSE PERFORM pg_sleep(0.15); END IF;
          END IF; RETURN NEW; END $$
          """);
      orders.execute(
          "CREATE TRIGGER m34_observe_parallel_claims BEFORE UPDATE ON outbox_event FOR EACH ROW"
              + " EXECUTE FUNCTION m34_observe_parallel_claims()");
      UUID prime = post("claims-prime", UUID.randomUUID(), 1);
      await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(
              () ->
                  assertThat(
                          orders.queryForObject(
                              "SELECT count(*) FROM m34_claims WHERE cleared_at IS NULL",
                              Long.class))
                      .isGreaterThan(0));
      try (var requests = Executors.newFixedThreadPool(6)) {
        var jobs = new ArrayList<Future<UUID>>();
        for (int i = 0; i < 30; i++)
          jobs.add(requests.submit(() -> post("claims", UUID.randomUUID(), 1)));
        for (var job : jobs) status(job.get(15, TimeUnit.SECONDS), "CANCELLED");
      }
      status(prime, "CANCELLED");
      drain();
      orders.execute("DROP TRIGGER m34_observe_parallel_claims ON outbox_event");
      orders.execute("DROP FUNCTION m34_observe_parallel_claims()");
      long publishers =
          orders.queryForObject("SELECT count(DISTINCT publisher) FROM m34_claims", Long.class);
      assertThat(publishers).isGreaterThanOrEqualTo(2);
      long overlapping =
          orders.queryForObject(
              "SELECT count(*) FROM m34_claims a JOIN m34_claims b ON a.publisher<>b.publisher AND"
                  + " a.event_id<>b.event_id WHERE a.cleared_at IS NOT NULL AND b.cleared_at IS NOT"
                  + " NULL AND a.claimed_at<b.cleared_at AND b.claimed_at<a.cleared_at",
              Long.class);
      assertThat(overlapping)
          .as("Different live publishers must hold different-row leases concurrently")
          .isGreaterThan(0);
      assertThat(
              orders.queryForObject(
                  "SELECT count(*) FROM outbox_event WHERE claim_token IS NOT NULL", Long.class))
          .isZero();
      assertThat(
              orders.queryForObject(
                  "SELECT count(*) FROM m34_claims a JOIN m34_claims b ON a.event_id=b.event_id AND"
                      + " a.token<>b.token WHERE a.claimed_at<b.claimed_at AND (a.cleared_at IS"
                      + " NULL OR a.cleared_at>b.claimed_at)",
                  Long.class))
          .isZero();
      evidence.add(
          Map.of(
              "stage",
              "concurrent-publishers-and-ack-mark-fault",
              "distinctOrderPublishers",
              publishers,
              "sameBusinessEffect",
              true,
              "overlappingDifferentPublisherClaims",
              overlapping,
              "testOnlyMarkDelayMs",
              150,
              "orderId",
              fault));
    }

    void concurrentReserveAndRelease() throws Exception {
      UUID product = UUID.randomUUID();
      initialStock.put(product, 10);
      inventory.update("INSERT INTO inventory_stock VALUES(?,10,0)", product);
      var ids = new ArrayList<UUID>();
      try (var connection = databases.get("inventory-cell").getPostgresDatabase().getConnection();
          var workers = Executors.newFixedThreadPool(6)) {
        connection.setAutoCommit(false);
        try (var statement =
            connection.prepareStatement(
                "SELECT product_id FROM inventory_stock WHERE product_id=? FOR UPDATE")) {
          statement.setObject(1, product);
          statement.executeQuery().close();
        }
        var jobs = new ArrayList<Future<UUID>>();
        for (int i = 0; i < 20; i++) {
          final String decision = i % 3 == 0 ? "DECLINED" : "AUTHORIZED";
          jobs.add(
              workers.submit(
                  () -> {
                    UUID id = post("shared-stock", product, 1);
                    payments.update(
                        "INSERT INTO payment_simulation_policy VALUES(?,?)", id, decision);
                    return id;
                  }));
        }
        for (var job : jobs) {
          UUID id = job.get(15, TimeUnit.SECONDS);
          ids.add(id);
          productForOrder.put(id, product);
        }
        connection.commit();
      }
      for (UUID id : ids)
        await()
            .atMost(Duration.ofSeconds(45))
            .untilAsserted(
                () ->
                    assertThat(
                            orders.queryForObject(
                                "SELECT status FROM orders WHERE id=?", String.class, id))
                        .isIn("CONFIRMED", "CANCELLED"));
      drain();
      long confirmed = 0, released = 0, rejected = 0;
      for (UUID id : ids) {
        String status =
            orders.queryForObject("SELECT status FROM orders WHERE id=?", String.class, id);
        String reservation =
            inventory.queryForObject(
                "SELECT status FROM inventory_reservations WHERE order_id=?", String.class, id);
        if (status.equals("CONFIRMED")) {
          confirmed++;
          assertThat(reservation).isEqualTo("RESERVED");
        } else if (reservation.equals("RELEASED")) {
          released++;
          assertThat(
                  orders.queryForObject(
                      "SELECT release_acknowledged FROM orders WHERE id=?", Boolean.class, id))
              .isTrue();
        } else {
          rejected++;
          assertThat(reservation).isEqualTo("REJECTED");
          assertThat(
                  payments.queryForObject(
                      "SELECT count(*) FROM payment_authorizations WHERE order_id=?",
                      Long.class,
                      id))
              .isZero();
        }
        assertThat(
                payments.queryForObject(
                    "SELECT count(*) FROM payment_authorizations WHERE order_id=?", Long.class, id))
            .isLessThanOrEqualTo(1);
      }
      assertThat(confirmed).isBetween(1L, 10L);
      assertThat(rejected).isGreaterThan(0);
      var counters =
          inventory.queryForMap(
              "SELECT available_quantity,reserved_quantity FROM inventory_stock WHERE product_id=?",
              product);
      assertThat(((Number) counters.get("available_quantity")).longValue())
          .isEqualTo(10 - confirmed);
      assertThat(((Number) counters.get("reserved_quantity")).longValue()).isEqualTo(confirmed);
      evidence.add(
          Map.of(
              "stage",
              "shared-sku-concurrent-reserve-release",
              "orders",
              20,
              "confirmed",
              confirmed,
              "released",
              released,
              "rejected",
              rejected,
              "stock",
              counters));
    }

    void replicaStopAndRebalance() throws Exception {
      for (String cell : List.of("payment-cell", "inventory-cell")) {
        drain();
        Node node =
            nodes.stream()
                .filter(n -> n.owner.equals(cell) && n.replica == 2)
                .findFirst()
                .orElseThrow();
        stop(node.process);
        UUID id = place("replica-down", 1, "AUTHORIZED");
        status(id, "CONFIRMED");
        stock(id, 9, 1);
        drain();
        assignments(
            cell + "-one-down",
            3,
            cell.equals("inventory-cell") ? 1 : 2,
            cell.equals("payment-cell") ? 1 : 2);
        node.start();
        registrations(cell, 2);
        assignments(cell + "-restarted", 3, 2, 2);
      }
    }

    void assignments(String stage, int order, int inv, int pay) {
      var wanted = Map.of("order-cell", order, "inventory-cell", inv, "payment-cell", pay);
      var state = new HashMap<String, Object>();
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () -> {
                for (var entry : wanted.entrySet()) {
                  var description =
                      admin
                          .describeConsumerGroups(List.of(groups.get(entry.getKey())))
                          .describedGroups()
                          .get(groups.get(entry.getKey()))
                          .get(5, TimeUnit.SECONDS);
                  assertThat(description.members()).hasSize(entry.getValue());
                  assertThat(description.members())
                      .allMatch(m -> !m.assignment().topicPartitions().isEmpty());
                  state.put(
                      entry.getKey(),
                      description.members().stream()
                          .map(
                              m ->
                                  Map.of(
                                      "member",
                                      m.consumerId(),
                                      "partitions",
                                      m.assignment().topicPartitions().stream()
                                          .map(Object::toString)
                                          .toList()))
                          .toList());
                }
              });
      evidence.add(Map.of("stage", "assignments-" + stage, "groups", Map.copyOf(state)));
    }

    void drain() {
      await()
          .atMost(Duration.ofSeconds(60))
          .untilAsserted(
              () -> {
                for (var db : List.of(orders, inventory, payments)) {
                  assertThat(
                          db.queryForObject(
                              "SELECT count(*) FROM outbox_event WHERE NOT published AND NOT"
                                  + " dead_letter",
                              Long.class))
                      .isZero();
                  assertThat(
                          db.queryForObject(
                              "SELECT count(*) FROM outbox_event WHERE dead_letter", Long.class))
                      .isZero();
                }
                for (var pair :
                    Map.of(
                            "order-cell",
                            List.of(SOURCES.get(1), SOURCES.get(4)),
                            "inventory-cell",
                            List.of(SOURCES.get(0), SOURCES.get(3)),
                            "payment-cell",
                            List.of(SOURCES.get(2)))
                        .entrySet()) {
                  var committed =
                      admin
                          .listConsumerGroupOffsets(groups.get(pair.getKey()))
                          .partitionsToOffsetAndMetadata()
                          .get(5, TimeUnit.SECONDS);
                  for (String topic : pair.getValue())
                    for (int p = 0; p < 3; p++) {
                      var tp = new TopicPartition(topic, p);
                      long end =
                          admin
                              .listOffsets(Map.of(tp, OffsetSpec.latest()))
                              .all()
                              .get(5, TimeUnit.SECONDS)
                              .get(tp)
                              .offset();
                      assertThat(committed.containsKey(tp) ? committed.get(tp).offset() : 0)
                          .as(tp.toString())
                          .isEqualTo(end);
                    }
                }
              });
    }

    void assertInvariants() throws Exception {
      var durableCounts = new LinkedHashMap<String, Object>();
      for (String owner : databases.keySet())
        durableCounts.put(
            owner,
            Map.of(
                "outbox",
                    jdbc(owner)
                        .queryForList(
                            "SELECT event_type,count(*) AS events,count(*) FILTER(WHERE published)"
                                + " AS published FROM outbox_event GROUP BY event_type ORDER BY"
                                + " event_type"),
                "inboxCount",
                    jdbc(owner)
                        .queryForObject("SELECT count(*) FROM processed_event", Long.class)));
      var offsetEvidence = new ArrayList<Map<String, Object>>();
      for (var entry : groups.entrySet()) {
        var offsets =
            admin
                .listConsumerGroupOffsets(entry.getValue())
                .partitionsToOffsetAndMetadata()
                .get(5, TimeUnit.SECONDS);
        for (var position : offsets.entrySet())
          offsetEvidence.add(
              Map.of(
                  "cell",
                  entry.getKey(),
                  "group",
                  entry.getValue(),
                  "topic",
                  position.getKey().topic(),
                  "partition",
                  position.getKey().partition(),
                  "committed",
                  position.getValue().offset(),
                  "end",
                  admin
                      .listOffsets(Map.of(position.getKey(), OffsetSpec.latest()))
                      .all()
                      .get(5, TimeUnit.SECONDS)
                      .get(position.getKey())
                      .offset()));
      }
      evidence.add(
          Map.of(
              "stage",
              "durable-db-and-offset-evidence",
              "owners",
              durableCounts,
              "offsets",
              offsetEvidence));
      for (var owner : databases.entrySet()) {
        var tables =
            jdbc(owner.getKey())
                .queryForList(
                    "SELECT tablename FROM pg_tables WHERE schemaname='public'", String.class);
        for (String other : List.of("orders", "inventory_stock", "payment_authorizations")) {
          boolean own =
              owner.getKey().equals("order-cell")
                  ? other.equals("orders")
                  : owner.getKey().equals("inventory-cell")
                      ? other.equals("inventory_stock")
                      : other.equals("payment_authorizations");
          if (!own) assertThat(tables).doesNotContain(other);
        }
      }
      for (var row : orders.queryForList("SELECT id,status,release_acknowledged FROM orders")) {
        UUID id = (UUID) row.get("id");
        String state = row.get("status").toString();
        String reserved =
            inventory.queryForObject(
                "SELECT status FROM inventory_reservations WHERE order_id=?", String.class, id);
        if (state.equals("CONFIRMED")) {
          assertThat(reserved).isEqualTo("RESERVED");
          assertThat(
                  payments.queryForObject(
                      "SELECT status FROM payment_authorizations WHERE order_id=?",
                      String.class,
                      id))
              .isEqualTo("AUTHORIZED");
        } else {
          assertThat(state).isEqualTo("CANCELLED");
          if (reserved.equals("RELEASED")) {
            assertThat(row.get("release_acknowledged")).isEqualTo(true);
            assertThat(
                    payments.queryForObject(
                        "SELECT status FROM payment_authorizations WHERE order_id=?",
                        String.class,
                        id))
                .isEqualTo("DECLINED");
          } else {
            assertThat(reserved).isEqualTo("REJECTED");
            assertThat(
                    payments.queryForObject(
                        "SELECT count(*) FROM payment_authorizations WHERE order_id=?",
                        Long.class,
                        id))
                .isZero();
          }
        }
      }
      for (var entry : initialStock.entrySet()) {
        long expected =
            inventory.queryForObject(
                "SELECT COALESCE(sum(i.quantity),0) FROM inventory_reservation_items i JOIN"
                    + " inventory_reservations r USING(order_id) WHERE product_id=? AND"
                    + " r.status='RESERVED'",
                Long.class,
                entry.getKey());
        var actual =
            inventory.queryForMap(
                "SELECT available_quantity,reserved_quantity FROM inventory_stock WHERE"
                    + " product_id=?",
                entry.getKey());
        assertThat(((Number) actual.get("available_quantity")).longValue())
            .isEqualTo(entry.getValue() - expected)
            .isNotNegative();
        assertThat(((Number) actual.get("reserved_quantity")).longValue())
            .isEqualTo(expected)
            .isNotNegative();
      }
      for (String t : SOURCES)
        for (int p = 0; p < 3; p++)
          assertThat(
                  admin
                      .listOffsets(Map.of(new TopicPartition(t + "-dlt", p), OffsetSpec.latest()))
                      .all()
                      .get(5, TimeUnit.SECONDS)
                      .values()
                      .iterator()
                      .next()
                      .offset())
              .isZero();
      evidence.add(
          Map.of(
              "stage",
              "invariants",
              "stockConservation",
              true,
              "noDlt",
              true,
              "noPendingOutbox",
              true));
    }

    void securityAndDiscoveryNegatives(UUID id) throws Exception {
      long before = total(orders, "orders");
      // An advertised authority cannot bypass actual certificate hostname verification.
      for (Node node : nodes)
        if (node.owner.equals("order-cell")) metadataAuthority(node, "wrong.invalid");
      try (var mismatch =
          new Client(
              this,
              "wrong-server-authority",
              certs.primary,
              "m32-client",
              List.of(
                  "--apex.discovery.targets.order-cell.authority=wrong.invalid",
                  "--spring.grpc.client.channel.order.target=dns:///wrong.invalid:443"))) {
        assertThat(mismatch.call(command("create")).path("error").asText())
            .isEqualTo("UNAVAILABLE");
      } finally {
        for (Node node : nodes)
          if (node.owner.equals("order-cell")) metadataAuthority(node, "localhost");
      }
      await()
          .atMost(Duration.ofSeconds(15))
          .untilAsserted(
              () ->
                  assertThat(
                          client
                              .call(Map.of("action", "query", "orderId", id.toString()))
                              .path("orderId")
                              .asText())
                      .isEqualTo(id.toString()));
      try (var wrong = new Client(this, "wrong-san", certs.wrongSan, "m32-client", List.of())) {
        assertThat(wrong.call(command("create")).path("error").asText())
            .isEqualTo("UNAUTHENTICATED");
      }
      try (var forged =
          new Client(this, "forged-origin", certs.observer, "m32-client", List.of())) {
        assertThat(forged.call(command("create")).path("error").asText())
            .isEqualTo("UNAUTHENTICATED");
      }
      try (var deniedPeer =
          new Client(this, "valid-unprivileged-peer", certs.observer, "m32-observer", List.of())) {
        assertThat(deniedPeer.call(command("create")).path("error").asText())
            .isEqualTo("PERMISSION_DENIED");
      }
      stop(registry);
      await()
          .atMost(Duration.ofSeconds(15))
          .untilAsserted(
              () ->
                  assertThat(
                          client
                              .call(Map.of("action", "query", "orderId", id.toString()))
                              .path("error")
                              .asText())
                      .isEqualTo("UNAVAILABLE"));
      var denied = client.call(command("create"));
      assertThat(denied.path("error").asText()).isEqualTo("UNAVAILABLE");
      var readiness =
          http.send(
              HttpRequest.newBuilder(
                      URI.create("http://127.0.0.1:" + edgePort + "/actuator/health/readiness"))
                  .GET()
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      var liveness =
          http.send(
              HttpRequest.newBuilder(
                      URI.create("http://127.0.0.1:" + edgePort + "/actuator/health/liveness"))
                  .GET()
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      assertThat(readiness.statusCode()).isEqualTo(503);
      assertThat(liveness.statusCode()).isEqualTo(200);
      assertThat(total(orders, "orders")).isEqualTo(before);
      evidence.add(
          Map.of(
              "stage",
              "mtls-and-registry-expiry",
              "wrongSanDenied",
              true,
              "serverHostnameMismatchDenied",
              true,
              "forgedCallerDenied",
              true,
              "expiredRegistryNewWrites",
              0));
    }

    void databaseLossChangesReadinessNotLiveness() throws Exception {
      // Only after all owner SQL/Kafka evidence is captured; close an owned disposable server.
      var orderDb = databases.get("order-cell");
      orderDb.close();
      databases.remove("order-cell");
      var node =
          nodes.stream()
              .filter(n -> n.owner.equals("order-cell") && n.process.isAlive())
              .findFirst()
              .orElseThrow();
      var timedOutProbes = new java.util.concurrent.atomic.AtomicInteger();
      await()
          .atMost(Duration.ofSeconds(20))
          .ignoreException(java.net.http.HttpTimeoutException.class)
          .untilAsserted(
              () -> {
                try {
                  var component =
                      http.send(
                          HttpRequest.newBuilder(
                                  URI.create(
                                      "http://127.0.0.1:" + node.http + "/actuator/health/db"))
                              .timeout(Duration.ofSeconds(5))
                              .GET()
                              .build(),
                          HttpResponse.BodyHandlers.ofString());
                  assertThat(component.statusCode()).isEqualTo(503);
                  assertThat(JSON.readTree(component.body()).path("status").asText())
                      .isEqualTo("DOWN");
                  var ready =
                      http.send(
                          HttpRequest.newBuilder(
                                  URI.create(
                                      "http://127.0.0.1:"
                                          + node.http
                                          + "/actuator/health/readiness"))
                              .timeout(Duration.ofSeconds(5))
                              .GET()
                              .build(),
                          HttpResponse.BodyHandlers.ofString());
                  assertThat(ready.statusCode()).isEqualTo(503);
                } catch (java.net.http.HttpTimeoutException timeout) {
                  timedOutProbes.incrementAndGet();
                  throw timeout;
                }
              });
      var live =
          http.send(
              HttpRequest.newBuilder(
                      URI.create("http://127.0.0.1:" + node.http + "/actuator/health/liveness"))
                  .timeout(Duration.ofSeconds(5))
                  .GET()
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      assertThat(live.statusCode()).isEqualTo(200);
      evidence.add(
          Map.of(
              "stage",
              "owning-database-loss",
              "pid",
              node.process.pid(),
              "readiness",
              503,
              "liveness",
              200,
              "timedOutReadOnlyProbes",
              timedOutProbes.get(),
              "probeTimeoutSeconds",
              5,
              "convergenceBoundSeconds",
              20));
    }

    void metadataAuthority(Node node, String authority) throws Exception {
      var response =
          http.send(
              HttpRequest.newBuilder(
                      URI.create(
                          "http://127.0.0.1:"
                              + registryPort
                              + "/eureka/apps/ORDER-CELL/"
                              + node.instance
                              + "/metadata?apex.grpc.authority="
                              + authority))
                  .timeout(Duration.ofSeconds(3))
                  .PUT(HttpRequest.BodyPublishers.noBody())
                  .build(),
              HttpResponse.BodyHandlers.discarding());
      assertThat(response.statusCode()).isEqualTo(200);
    }

    @Override
    public void close() throws Exception {
      if (closed) return;
      closed = true;
      var failures = new ArrayList<Throwable>();
      if (client != null)
        try {
          client.close();
        } catch (Throwable e) {
          failures.add(e);
        }
      for (var p : children)
        try {
          stop(p);
        } catch (Throwable e) {
          failures.add(e);
        }
      for (var node : nodes)
        if (node.proxy != null)
          try {
            node.proxy.close();
            assertThat(node.proxy.threads.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
          } catch (Throwable e) {
            failures.add(e);
          }
      if (admin != null)
        try {
          admin.close();
        } catch (Throwable e) {
          failures.add(e);
        }
      if (broker != null)
        try {
          broker.destroy();
        } catch (Throwable e) {
          failures.add(e);
        }
      for (var db : databases.values())
        try {
          db.close();
        } catch (Throwable e) {
          failures.add(e);
        }
      if (certs != null)
        try {
          certs.close();
        } catch (Throwable e) {
          failures.add(e);
        }
      if (http != null)
        try {
          http.close();
        } catch (Throwable e) {
          failures.add(e);
        }
      evidence.add(
          Map.of(
              "stage",
              "teardown",
              "pids",
              children.stream().map(Process::pid).toList(),
              "allStopped",
              children.stream().noneMatch(Process::isAlive),
              "cleanupFailures",
              failures.size()));
      Files.createDirectories(output);
      Files.writeString(
          output.resolve("acceptance.json"),
          JSON.writerWithDefaultPrettyPrinter().writeValueAsString(evidence));
      if (!failures.isEmpty())
        throw new AssertionError("Owned fixture cleanup failed", failures.getFirst());
    }
  }

  static final class Node {
    final Fleet f;
    final String owner, instance;
    final int replica, http, grpc;
    final GrpcOrderIT.ReplyLossProxy proxy;
    final List<Path> logs = new ArrayList<>();
    Process process;

    Node(Fleet f, String owner, int replica) throws Exception {
      this.f = f;
      this.owner = owner;
      this.replica = replica;
      instance = owner + ":replica-" + replica;
      http = freePort();
      grpc = freePort();
      proxy = owner.equals("order-cell") ? new GrpcOrderIT.ReplyLossProxy(grpc) : null;
    }

    void start() throws Exception {
      var args = new ArrayList<>(f.eureka(true));
      args.removeIf(x -> x.startsWith("--spring.profiles.active="));
      args.add(
          "--spring.profiles.active="
              + (owner.equals("payment-cell")
                  ? "three-cell-saga,simulation,grpc,eureka"
                  : "saga,three-cell-saga,grpc,eureka"));
      var db = f.databases.get(owner);
      args.addAll(
          List.of(
              "--spring.config.location=classpath:application.yml",
              "--spring.config.additional-location=",
              "--spring.config.import=",
              "--spring.profiles.include=",
              "--server.port=" + http,
              "--spring.main.web-application-type=servlet",
              "--spring.datasource.url=" + db.getJdbcUrl("postgres", "postgres"),
              "--spring.datasource.username=postgres",
              "--spring.datasource.password=",
              "--spring.datasource.hikari.connection-timeout=2000",
              "--spring.datasource.hikari.validation-timeout=1000",
              "--management.health.db.enabled=true",
              "--management.endpoint.health.show-components=always",
              "--apex.cell.id=" + owner,
              "--apex.discovery.instance-id=replica-" + replica,
              "--apex.discovery.authority=localhost",
              "--spring.kafka.bootstrap-servers=" + f.brokers,
              "--spring.kafka.consumer.group-id=" + f.groups.get(owner),
              "--spring.kafka.consumer.enable-auto-commit=false",
              "--spring.kafka.consumer.properties.allow.auto.create.topics=false",
              "--spring.kafka.consumer.properties.session.timeout.ms=6000",
              "--spring.kafka.consumer.properties.heartbeat.interval.ms=1000",
              "--spring.kafka.admin.auto-create=false",
              "--spring.kafka.listener.auto-startup=true",
              "--spring.kafka.listener.concurrency=1",
              "--apex.messaging.cell.enabled=true",
              "--apex.messaging.consumer.enabled=true",
              "--apex.messaging.outbox.publisher.enabled=true",
              "--apex.messaging.outbox.publisher.interval-ms=100",
              "--apex.messaging.consumer.reliability.enabled=true",
              "--apex.messaging.consumer.reliability.max-attempts=30",
              "--apex.messaging.consumer.reliability.retry-delay=200ms",
              "--apex.grpc.server.enabled=true",
              "--apex.grpc.cell-id=" + owner,
              "--spring.grpc.server.address=127.0.0.1",
              "--spring.grpc.server.port=" + grpc,
              "--spring.grpc.server.ssl.enabled=true",
              "--spring.grpc.server.ssl.bundle=m32",
              "--spring.grpc.server.ssl.client-auth=require",
              "--spring.grpc.server.reflection.enabled=false",
              "--apex.grpc.security.enabled=true",
              "--apex.grpc.security.tenant=alpha",
              "--apex.grpc.security.peers.primary.uri=" + PRIMARY,
              "--apex.grpc.security.peers.primary.contracts=QUERY:GET_ORDER_PERSPECTIVE:1"
                  + (owner.equals("order-cell") ? ",QUERY:GET_ORDER:1,REQUEST:CREATE_ORDER:1" : ""),
              "--logging.level.com.apex.platform.grpc.internal.RpcDispatcher=DEBUG"));
      args.addAll(f.certs.bundle(f.certs.server));
      args.add(
          "--apex.grpc.security.peers.observer.uri=spiffe://apex.test/tenant/alpha/cell/m32-observer");
      args.add(
          "--apex.grpc.security.peers.observer.contracts="
              + (owner.equals("order-cell")
                  ? "QUERY:GET_ORDER:1"
                  : "QUERY:GET_ORDER_PERSPECTIVE:1"));
      if (proxy != null) args.add("--apex.discovery.grpc-advertised-port=" + proxy.port());
      if (owner.equals("order-cell"))
        args.addAll(
            List.of(
                "--server.tomcat.accesslog.enabled=true",
                "--server.tomcat.accesslog.buffered=false",
                "--server.tomcat.accesslog.directory=" + f.output.resolve("access-" + replica),
                "--server.tomcat.accesslog.pattern=%{X-Request-ID}i %{X-User}i %{X-Forwarded-Host}i"
                    + " %s"));
      Path log = f.output.resolve(owner + "-" + replica + "-" + logs.size() + ".log");
      logs.add(log);
      process = f.launchOwned(owner, log.getFileName().toString(), args);
      f.readyProcess(process, log);
      assertThat(read(log)).contains("gRPC Server started");
      f.evidence.add(
          Map.of(
              "stage",
              "replica-started",
              "cell",
              owner,
              "instance",
              instance,
              "pid",
              process.pid(),
              "httpPort",
              http,
              "grpcPort",
              grpc,
              "advertisedGrpcPort",
              proxy == null ? grpc : proxy.port(),
              "databasePort",
              db.getPort()));
    }

    String access() {
      Path dir = f.output.resolve("access-" + replica);
      if (!Files.exists(dir)) return "";
      try (var files = Files.list(dir)) {
        return files.filter(Files::isRegularFile).map(GrpcOrderIT::read).reduce("", String::concat);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  static final class Client implements AutoCloseable {
    final Fleet f;
    final Process process;
    final Path log;
    final BufferedWriter input;
    int commands;

    Client(Fleet f, String name, Path key, String caller, List<String> extras) throws Exception {
      this.f = f;
      log = f.output.resolve(name + ".log");
      var args = new ArrayList<>(f.eureka(false));
      args.addAll(
          List.of(
              "--spring.main.web-application-type=none",
              "--spring.config.location=optional:classpath:/m34-no-external.properties",
              "--apex.grpc.client.enabled=true",
              "--apex.grpc.cell-id=" + caller,
              "--apex.grpc.destinations.order-cell.channel=order",
              "--spring.grpc.client.channel.order.target=dns:///localhost:443",
              "--spring.grpc.client.channel.order.ssl.enabled=true",
              "--spring.grpc.client.channel.order.ssl.bundle=m32",
              "--apex.discovery.grpc.enabled=true",
              "--apex.discovery.targets.order-cell.authority=localhost"));
      args.addAll(f.certs.bundle(key));
      for (String override : extras) {
        String optionPrefix = override.substring(0, override.indexOf('=') + 1);
        args.removeIf(existing -> existing.startsWith(optionPrefix));
        args.add(override);
      }
      process = f.launchOwned("sample-grpc-client", name + ".log", args);
      input =
          new BufferedWriter(
              new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));
      try {
        await()
            .atMost(Duration.ofSeconds(45))
            .untilAsserted(
                () -> {
                  assertThat(process.isAlive()).withFailMessage("Inspect %s", log).isTrue();
                  assertThat(read(log)).contains("M31_CLIENT_READY");
                });
      } catch (Exception | Error failure) {
        stop(process);
        input.close();
        throw failure;
      }
    }

    int send(Map<String, Object> cmd) throws Exception {
      int index = commands++;
      input.write(JSON.writeValueAsString(cmd));
      input.newLine();
      input.flush();
      return index;
    }

    JsonNode receive(int index) {
      await()
          .atMost(Duration.ofSeconds(15))
          .untilAsserted(
              () -> {
                assertThat(process.isAlive()).isTrue();
                assertThat(replies()).hasSizeGreaterThan(index);
              });
      var wrapped = JSON.readTree(replies().get(index).substring("M31_REPLY ".length()));
      assertThat(wrapped.path("contextCleared").asBoolean()).isTrue();
      return wrapped.path("result");
    }

    List<String> replies() {
      return read(log).lines().filter(s -> s.startsWith("M31_REPLY ")).toList();
    }

    JsonNode call(Map<String, Object> request) throws Exception {
      return receive(send(request));
    }

    public void close() throws Exception {
      try {
        if (process.isAlive()) {
          input.write("EXIT\n");
          input.flush();
          process.waitFor(5, TimeUnit.SECONDS);
        }
      } finally {
        stop(process);
        input.close();
      }
    }
  }
}
