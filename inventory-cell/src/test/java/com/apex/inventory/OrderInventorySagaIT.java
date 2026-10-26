package com.apex.inventory;

import java.io.InputStream;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarFile;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Packaged applications in TWO CHILD JVMs; all databases, topics and groups belong to this test. */
@Tag("integration")
class OrderInventorySagaIT {
    private static final String ORDER_TOPIC = "apex.order.events.v1";
    private static final String INVENTORY_TOPIC = "apex.inventory.events.v1";
    private static final String PERSIAN = "سفارش آزمایشی";
    private static final List<String> MODULES = List.of("apex-platform-core", "apex-platform-events",
            "apex-platform-context-autoconfigure", "apex-platform-webmvc", "apex-platform-messaging",
            "apex-starter-webmvc", "apex-starter-cell-messaging", "order-cell");
    private static final JsonMapper JSON = JsonMapper.builder().build();

    @Test
    void twoPackagedProcessesReserveRejectDeduplicateRollbackRetryAndRecoverAfterRestart() throws Exception {
        Path root = Path.of(System.getProperty("saga.repositoryRoot")).toAbsolutePath().normalize();
        Path inventoryJar = Path.of(System.getProperty("saga.inventoryJar")).toAbsolutePath().normalize();
        Path orderJar = verifyBuild(root, inventoryJar);
        Path output = inventoryJar.getParent();
        List<Map<String, Object>> evidence = new ArrayList<>();
        EmbeddedKafkaKraftBroker broker = new EmbeddedKafkaKraftBroker(1, 1, ORDER_TOPIC, INVENTORY_TOPIC,
                ORDER_TOPIC + "-dlt", INVENTORY_TOPIC + "-dlt");
        broker.brokerProperty("auto.create.topics.enable", "false");
        broker.brokerListProperty("apex.m26.saga.test.brokers");
        Process order = null;
        Process inventory = null;
        try (EmbeddedPostgres orderDb = EmbeddedPostgres.builder().start();
             EmbeddedPostgres inventoryDb = EmbeddedPostgres.builder().start();
             HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()) {
            try {
                broker.afterPropertiesSet();
                String brokers = broker.getBrokersAsString();
                String orderGroup = "order-saga-test-" + UUID.randomUUID();
                String inventoryGroup = "inventory-saga-test-" + UUID.randomUUID();
                JdbcTemplate orders = new JdbcTemplate(orderDb.getPostgresDatabase());
                JdbcTemplate stock = new JdbcTemplate(inventoryDb.getPostgresDatabase());
                int orderPort = freePort();
                int inventoryPort = freePort();
                Path orderLog = output.resolve("saga-order.log");
                Path inventoryLog = output.resolve("saga-inventory.log");
                order = launch(orderJar, orderDb, brokers, orderGroup, orderPort, orderLog, ORDER_TOPIC, INVENTORY_TOPIC);
                inventory = launch(inventoryJar, inventoryDb, brokers, inventoryGroup, inventoryPort,
                        inventoryLog, INVENTORY_TOPIC, ORDER_TOPIC);
                ready(order, orderLog, http, orderPort);
                ready(inventory, inventoryLog, http, inventoryPort);
                assertThat(Files.readString(orderLog)).contains("Saga preflight validated INVENTORY_RESERVED V1",
                        "Saga preflight validated INVENTORY_RESERVATION_REJECTED V1");
                assertThat(Files.readString(inventoryLog)).contains("Saga preflight validated ORDER_PLACED V1");
                assertThat(Files.readString(orderLog)).contains("APEX reliable consumer active");
                assertThat(Files.readString(inventoryLog)).contains("APEX reliable consumer active");
                assertThat(order.pid()).isNotEqualTo(inventory.pid()).isNotEqualTo(ProcessHandle.current().pid());
                assertThat(inventory.pid()).isNotEqualTo(ProcessHandle.current().pid());
                evidence.add(Map.of("stage", "started", "orderPid", order.pid(), "inventoryPid", inventory.pid(),
                        "orderGroup", orderGroup, "inventoryGroup", inventoryGroup, "brokers", brokers));
                assertThat(orders.queryForList("SELECT tablename FROM pg_tables WHERE schemaname='public'", String.class))
                        .contains("orders", "order_items", "outbox_event", "processed_event").doesNotContain("inventory_stock");
                assertThat(stock.queryForList("SELECT tablename FROM pg_tables WHERE schemaname='public'", String.class))
                        .contains("inventory_stock", "inventory_reservations", "outbox_event", "processed_event")
                        .doesNotContain("orders", "order_items");

                try (Admin admin = Admin.create(Map.of("bootstrap.servers", brokers));
                     KafkaConsumer<String, String> observer = observer(brokers);
                     KafkaConsumer<String, String> quarantineObserver = observer(brokers);
                     KafkaProducer<String, String> replay = producer(brokers)) {
                    assertThat(admin.listTopics().names().get(10, TimeUnit.SECONDS))
                            .contains(ORDER_TOPIC, INVENTORY_TOPIC, ORDER_TOPIC + "-dlt", INVENTORY_TOPIC + "-dlt");
                    quarantineObserver.assign(List.of(new TopicPartition(ORDER_TOPIC + "-dlt", 0),
                            new TopicPartition(INVENTORY_TOPIC + "-dlt", 0)));
                    quarantineObserver.seekToBeginning(quarantineObserver.assignment());
                    Map<UUID, ConsumerRecord<String, String>> quarantines = new HashMap<>();
                    observer.assign(List.of(new TopicPartition(ORDER_TOPIC, 0), new TopicPartition(INVENTORY_TOPIC, 0)));
                    observer.seekToBeginning(observer.assignment());
                    Map<UUID, ConsumerRecord<String, String>> observed = new HashMap<>();
                    UUID a = UUID.fromString("11111111-1111-4111-8111-111111111111");
                    UUID b = UUID.fromString("88888888-2222-4222-8222-222222222222");
                    stock.update("INSERT INTO inventory_stock VALUES (?, 10, 0), (?, 5, 0)", a, b);

                    var happy = post(http, orderPort, List.of(item(a, 2), item(b, 1)));
                    UUID happyId = UUID.fromString(happy.get("orderId").asText());
                    awaitStatus(orders, happyId, "PENDING_PAYMENT");
                    assertThat(getStatus(http, orderPort, happyId)).isEqualTo("PENDING_PAYMENT");
                    assertStock(stock, a, 8, 2);
                    assertStock(stock, b, 4, 1);
                    assertThat(count(stock, "inventory_reservations", "order_id", happyId)).isEqualTo(1);
                    assertThat(count(stock, "inventory_reservation_items", "order_id", happyId)).isEqualTo(2);
                    assertThat(count(orders, "order_items", "order_id", happyId)).isEqualTo(2);
                    assertThat(orders.queryForObject("SELECT description FROM orders WHERE id=?", String.class, happyId))
                            .isEqualTo(PERSIAN);
                    var placed = eventFor(orders, happyId, null);
                    UUID placedId = (UUID) placed.get("event_id");
                    var original = observe(observer, observed, placedId);
                    assertEnvelope(orders, placedId, original);
                    var reserved = eventFor(stock, happyId, placedId);
                    UUID reservedId = (UUID) reserved.get("event_id");
                    var reservedWire = observe(observer, observed, reservedId);
                    assertEnvelope(stock, reservedId, reservedWire);
                    assertThat(JSON.readTree(reservedWire.value()).get("payload").get("orderId").asText())
                            .isEqualTo(happyId.toString());
                    assertThat(original.topic()).isEqualTo(ORDER_TOPIC);
                    assertThat(reservedWire.topic()).isEqualTo(INVENTORY_TOPIC);
                    assertThat(reserved.get("correlation_id")).isEqualTo(placed.get("correlation_id"));
                    assertThat(reserved.get("source_service")).isEqualTo("inventory-cell");
                    assertThat(reserved.get("cell_id")).isEqualTo("inventory-cell");
                    assertThat(count(stock, "processed_event", "event_id", placedId)).isEqualTo(1);
                    assertThat(count(orders, "processed_event", "event_id", reservedId)).isEqualTo(1);
                    awaitPublished(orders, placedId);
                    awaitPublished(stock, reservedId);
                    evidence.add(Map.of("stage", "happy", "response", happy, "placedEventId", placedId,
                            "outcomeEventId", reservedId, "correlationId", placed.get("correlation_id"),
                            "status", "PENDING_PAYMENT", "stockA", List.of(8, 2), "stockB", List.of(4, 1),
                            "request", Map.of("externalReference",
                                    JSON.readTree(original.value()).get("payload").get("externalReference").asText(),
                                    "description", PERSIAN, "items", List.of(item(a, 2), item(b, 1)))));

                    var duplicate = replay.send(new ProducerRecord<>(ORDER_TOPIC, original.key(), original.value()))
                            .get(10, TimeUnit.SECONDS);
                    awaitConsumed(admin, inventoryGroup, ORDER_TOPIC, duplicate.offset());
                    assertStock(stock, a, 8, 2);
                    assertStock(stock, b, 4, 1);
                    assertThat(count(stock, "outbox_event", "aggregate_id", happyId)).isEqualTo(1);
                    assertThat(count(stock, "processed_event", "event_id", placedId)).isEqualTo(1);
                    var outcomeDuplicate = replay.send(new ProducerRecord<>(INVENTORY_TOPIC,
                            reservedWire.key(), reservedWire.value())).get(10, TimeUnit.SECONDS);
                    awaitConsumed(admin, orderGroup, INVENTORY_TOPIC, outcomeDuplicate.offset());
                    assertThat(count(orders, "processed_event", "event_id", reservedId)).isEqualTo(1);

                    ObjectNode newDelivery = (ObjectNode) JSON.readTree(original.value());
                    UUID newEventId = UUID.randomUUID();
                    ((ObjectNode) newDelivery.get("metadata")).put("eventId", newEventId.toString());
                    replay.send(new ProducerRecord<>(ORDER_TOPIC, original.key(), JSON.writeValueAsString(newDelivery)))
                            .get(10, TimeUnit.SECONDS);
                    await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                            assertThat(count(stock, "outbox_event", "aggregate_id", happyId)).isEqualTo(2));
                    var repeatedOutcome = eventFor(stock, happyId, newEventId);
                    UUID repeatedId = (UUID) repeatedOutcome.get("event_id");
                    observe(observer, observed, repeatedId);
                    await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                            assertThat(count(orders, "processed_event", "event_id", repeatedId)).isEqualTo(1));
                    assertThat(status(orders, happyId)).isEqualTo("PENDING_PAYMENT");
                    assertStock(stock, a, 8, 2);
                    assertStock(stock, b, 4, 1);
                    assertThat(count(stock, "inventory_reservations", "order_id", happyId)).isEqualTo(1);
                    evidence.add(Map.of("stage", "duplicates", "sameEventId", placedId,
                            "newEventId", newEventId, "repeatedOutcomeId", repeatedId, "logicalStockDeductions", 1));

                    var rejected = post(http, orderPort, List.of(item(a, 100), item(b, 1)));
                    UUID rejectedId = UUID.fromString(rejected.get("orderId").asText());
                    awaitStatus(orders, rejectedId, "CANCELLED");
                    assertThat(stock.queryForObject("SELECT status FROM inventory_reservations WHERE order_id=?",
                            String.class, rejectedId)).isEqualTo("REJECTED");
                    assertStock(stock, a, 8, 2);
                    assertStock(stock, b, 4, 1);
                    var rejectedOutcome = eventFor(stock, rejectedId, null);
                    assertThat(rejectedOutcome.get("event_type")).isEqualTo("INVENTORY_RESERVATION_REJECTED");
                    observe(observer, observed, (UUID) rejectedOutcome.get("event_id"));
                    evidence.add(Map.of("stage", "rejected", "orderId", rejectedId, "status", "CANCELLED"));

                    // Deferred trigger fails at COMMIT, after stock, Inbox and Outbox writes.
                    stock.execute("CREATE TABLE saga_failure_gate(enabled boolean NOT NULL)");
                    stock.update("INSERT INTO saga_failure_gate VALUES (true)");
                    // Sequence increments survive rollback, so the test can observe actual retries.
                    stock.execute("CREATE SEQUENCE saga_failure_attempt");
                    stock.execute("CREATE FUNCTION saga_fail_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                            + "IF (SELECT enabled FROM saga_failure_gate) THEN PERFORM nextval('saga_failure_attempt'); "
                            + "PERFORM pg_sleep(2); "
                            + "RAISE EXCEPTION 'saga injected commit failure'; END IF; RETURN NEW; END $$");
                    stock.execute("CREATE CONSTRAINT TRIGGER saga_commit_failure AFTER INSERT ON outbox_event "
                            + "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION saga_fail_commit()");
                    UUID failureId;
                    try {
                        var failing = post(http, orderPort, List.of(item(a, 1), item(b, 1)));
                        failureId = UUID.fromString(failing.get("orderId").asText());
                        var failingPlaced = eventFor(orders, failureId, null);
                        UUID failingEventId = (UUID) failingPlaced.get("event_id");
                        var failingRecord = observe(observer, observed, failingEventId);
                        await().atMost(Duration.ofSeconds(20)).until(() -> stock.queryForObject(
                                "SELECT last_value FROM saga_failure_attempt", Long.class) >= 2);
                        assertThat(count(stock, "inventory_reservations", "order_id", failureId)).isZero();
                        assertThat(count(stock, "inventory_reservation_items", "order_id", failureId)).isZero();
                        assertThat(count(stock, "processed_event", "event_id", failingEventId)).isZero();
                        assertThat(count(stock, "outbox_event", "aggregate_id", failureId)).isZero();
                        assertStock(stock, a, 8, 2);
                        assertStock(stock, b, 4, 1);
                        assertThat(status(orders, failureId)).isEqualTo("PENDING_INVENTORY");
                        var offsets = admin.listConsumerGroupOffsets(inventoryGroup).partitionsToOffsetAndMetadata()
                                .get(10, TimeUnit.SECONDS);
                        var committed = offsets.get(new TopicPartition(ORDER_TOPIC, 0));
                        assertThat(committed == null || committed.offset() <= failingRecord.offset()).isTrue();
                        stock.update("UPDATE saga_failure_gate SET enabled=false");
                        awaitStatus(orders, failureId, "PENDING_PAYMENT");
                        assertStock(stock, a, 7, 3);
                        assertStock(stock, b, 3, 2);
                        assertThat(count(stock, "processed_event", "event_id", failingEventId)).isEqualTo(1);
                        assertThat(count(stock, "outbox_event", "aggregate_id", failureId)).isEqualTo(1);
                        assertEnvelope(orders, failingEventId, failingRecord);
                        var retriedOutcome = eventFor(stock, failureId, failingEventId);
                        assertThat(retriedOutcome.get("correlation_id")).isEqualTo(failingPlaced.get("correlation_id"));
                        assertThat(retriedOutcome.get("causation_id")).isEqualTo(failingEventId);
                        assertEnvelope(stock, (UUID) retriedOutcome.get("event_id"),
                                observe(observer, observed, (UUID) retriedOutcome.get("event_id")));
                        evidence.add(Map.of("stage", "commit-failure-retry", "orderId", failureId,
                                "eventId", failingEventId, "offsetNotAckedWhileFailed", true,
                                "statusAfterRecovery", "PENDING_PAYMENT"));
                    } finally {
                        stock.update("UPDATE saga_failure_gate SET enabled=false");
                        stock.execute("DROP TRIGGER saga_commit_failure ON outbox_event");
                        stock.execute("DROP FUNCTION saga_fail_commit()");
                        stock.execute("DROP SEQUENCE saga_failure_attempt");
                        stock.execute("DROP TABLE saga_failure_gate");
                    }

                    awaitPublished(stock, (UUID) eventFor(stock, failureId, null).get("event_id"));
                    // Supported contract + conflicting business request: exhaust retries, then confirmed DLT.
                    ObjectNode conflictWire = (ObjectNode) JSON.readTree(original.value());
                    UUID conflictEventId = UUID.randomUUID();
                    ((ObjectNode) conflictWire.get("metadata")).put("eventId", conflictEventId.toString());
                    ((ObjectNode) conflictWire.get("payload").get("items").get(0)).put("quantity", 3);
                    String conflictRaw = JSON.writeValueAsString(conflictWire);
                    var conflictSource = replay.send(new ProducerRecord<>(ORDER_TOPIC, original.key(), conflictRaw))
                            .get(10, TimeUnit.SECONDS);
                    var conflictDlt = observe(quarantineObserver, quarantines, conflictEventId);
                    assertQuarantine(conflictDlt, conflictRaw, ORDER_TOPIC, conflictSource.offset(),
                            inventoryGroup, "ReservationConflictException");
                    awaitConsumed(admin, inventoryGroup, ORDER_TOPIC, conflictSource.offset());
                    assertThat(count(stock, "processed_event", "event_id", conflictEventId)).isZero();
                    assertThat(count(stock, "outbox_event", "aggregate_id", happyId)).isEqualTo(2);
                    assertStock(stock, a, 7, 3);
                    assertStock(stock, b, 3, 2);
                    evidence.add(Map.of("stage", "inventory-dlt", "eventId", conflictEventId,
                            "sourceTopic", ORDER_TOPIC, "sourceOffset", conflictSource.offset(),
                            "dltTopic", conflictDlt.topic(), "dltPartition", conflictDlt.partition(),
                            "sourceCommittedOffset", conflictSource.offset() + 1, "rawValuePreserved", true));

                    ObjectNode unsupportedWire = (ObjectNode) JSON.readTree(original.value());
                    UUID unsupportedId = UUID.randomUUID();
                    ((ObjectNode) unsupportedWire.get("metadata")).put("eventId", unsupportedId.toString());
                    ((ObjectNode) unsupportedWire.get("metadata")).put("eventVersion", 99);
                    String unsupportedRaw = JSON.writeValueAsString(unsupportedWire);
                    var unsupportedSource = replay.send(new ProducerRecord<>(ORDER_TOPIC, original.key(), unsupportedRaw))
                            .get(10, TimeUnit.SECONDS);
                    var unsupportedDlt = observe(quarantineObserver, quarantines, unsupportedId);
                    assertQuarantine(unsupportedDlt, unsupportedRaw, ORDER_TOPIC, unsupportedSource.offset(),
                            inventoryGroup, "UnsupportedEventContractException");
                    awaitConsumed(admin, inventoryGroup, ORDER_TOPIC, unsupportedSource.offset());
                    assertThat(count(stock, "processed_event", "event_id", unsupportedId)).isZero();
                    evidence.add(Map.of("stage", "unsupported-version-dlt", "eventId", unsupportedId,
                            "version", 99, "sourceOffset", unsupportedSource.offset(),
                            "dltTopic", unsupportedDlt.topic(), "sourceCommittedOffset", unsupportedSource.offset() + 1));

                    ObjectNode missingOrderWire = (ObjectNode) JSON.readTree(reservedWire.value());
                    UUID missingOrderId = UUID.randomUUID();
                    UUID missingOrderEventId = UUID.randomUUID();
                    ((ObjectNode) missingOrderWire.get("metadata")).put("eventId", missingOrderEventId.toString());
                    ((ObjectNode) missingOrderWire.get("metadata")).put("aggregateId", missingOrderId.toString());
                    ((ObjectNode) missingOrderWire.get("payload")).put("orderId", missingOrderId.toString());
                    String missingOrderRaw = JSON.writeValueAsString(missingOrderWire);
                    var missingSource = replay.send(new ProducerRecord<>(INVENTORY_TOPIC,
                            "INVENTORY_RESERVATION:" + missingOrderId, missingOrderRaw)).get(10, TimeUnit.SECONDS);
                    var missingDlt = observe(quarantineObserver, quarantines, missingOrderEventId);
                    assertQuarantine(missingDlt, missingOrderRaw, INVENTORY_TOPIC, missingSource.offset(),
                            orderGroup, "IllegalStateException");
                    awaitConsumed(admin, orderGroup, INVENTORY_TOPIC, missingSource.offset());
                    assertThat(count(orders, "processed_event", "event_id", missingOrderEventId)).isZero();
                    assertThat(count(orders, "orders", "id", missingOrderId)).isZero();
                    evidence.add(Map.of("stage", "order-dlt", "eventId", missingOrderEventId,
                            "sourceTopic", INVENTORY_TOPIC, "sourceOffset", missingSource.offset(),
                            "dltTopic", missingDlt.topic(), "dltPartition", missingDlt.partition(),
                            "sourceCommittedOffset", missingSource.offset() + 1, "rawValuePreserved", true));

                    long originalPid = inventory.pid();
                    stop(inventory);
                    inventory = null;
                    var queued = post(http, orderPort, List.of(item(a, 1)));
                    UUID queuedId = UUID.fromString(queued.get("orderId").asText());
                    UUID queuedEventId = (UUID) eventFor(orders, queuedId, null).get("event_id");
                    observe(observer, observed, queuedEventId);
                    awaitPublished(orders, queuedEventId);
                    assertThat(status(orders, queuedId)).isEqualTo("PENDING_INVENTORY");
                    assertThat(count(stock, "inventory_reservations", "order_id", queuedId)).isZero();
                    Path restartLog = output.resolve("saga-inventory-restarted.log");
                    inventory = launch(inventoryJar, inventoryDb, brokers, inventoryGroup, inventoryPort,
                            restartLog, INVENTORY_TOPIC, ORDER_TOPIC);
                    ready(inventory, restartLog, http, inventoryPort);
                    awaitStatus(orders, queuedId, "PENDING_PAYMENT");
                    assertStock(stock, a, 6, 4);
                    assertStock(stock, b, 3, 2);
                    assertThat(count(stock, "inventory_reservations", "order_id", happyId)).isEqualTo(1);
                    assertThat(count(stock, "processed_event", "event_id", placedId)).isEqualTo(1);
                    assertThat(count(stock, "processed_event", "event_id", queuedEventId)).isEqualTo(1);
                    assertThat(count(stock, "processed_event", "event_id", conflictEventId)).isZero();
                    assertThat(count(stock, "processed_event", "event_id", unsupportedId)).isZero();
                    assertThat(count(stock, "outbox_event", "aggregate_id", happyId)).isEqualTo(2);
                    quarantines.clear();
                    quarantineObserver.seekToBeginning(quarantineObserver.assignment());
                    assertQuarantine(observe(quarantineObserver, quarantines, conflictEventId),
                            conflictRaw, ORDER_TOPIC, conflictSource.offset(), inventoryGroup, "ReservationConflictException");
                    evidence.add(Map.of("stage", "restart", "oldInventoryPid", originalPid,
                            "newInventoryPid", inventory.pid(), "unchangedGroup", inventoryGroup,
                            "queuedOrderId", queuedId, "status", "PENDING_PAYMENT", "dltStillInspectable", true));
                    evidence.add(Map.of("stage", "artifacts", "orderJarSha256", sha(orderJar),
                            "inventoryJarSha256", sha(inventoryJar)));
                    Files.writeString(output.resolve("saga-two-process-evidence.json"),
                            JSON.writerWithDefaultPrettyPrinter().writeValueAsString(evidence), StandardCharsets.UTF_8);
                    System.out.println("SAGA_TWO_JVM_SUCCESS " + JSON.writeValueAsString(evidence));
                }
            } finally {
                try {
                    stop(inventory);
                } finally {
                    try {
                        stop(order);
                    } finally {
                        broker.destroy();
                    }
                }
            }
        }
    }

    private static Process launch(Path jar, EmbeddedPostgres db, String brokers, String group,
                                  int port, Path log, String publish, String subscribe) throws Exception {
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xms64m", "-Xmx256m", "-Dfile.encoding=UTF-8", "-jar", jar.toString(),
                "--server.address=127.0.0.1", "--server.port=" + port, "--spring.profiles.active=saga",
                "--spring.profiles.include=", "--spring.config.location=classpath:application.yml",
                "--spring.config.additional-location=", "--spring.config.import=",
                "--spring.datasource.url=" + db.getJdbcUrl("postgres", "postgres"),
                "--spring.datasource.username=postgres", "--spring.datasource.password=",
                "--spring.kafka.bootstrap-servers=" + brokers, "--spring.kafka.consumer.group-id=" + group,
                "--spring.kafka.consumer.properties.allow.auto.create.topics=false",
                // Windows process termination can skip LeaveGroup; use a short test-only failure-detection interval.
                "--spring.kafka.consumer.properties.session.timeout.ms=6000",
                "--spring.kafka.consumer.properties.heartbeat.interval.ms=1000",
                "--spring.kafka.consumer.enable-auto-commit=false",
                "--spring.kafka.admin.auto-create=false", "--spring.kafka.listener.missing-topics-fatal=true",
                "--apex.messaging.publisher.topic=" + publish, "--apex.messaging.consumer.topics=" + subscribe,
                "--apex.messaging.consumer.enabled=true", "--apex.messaging.outbox.publisher.enabled=true",
                "--apex.messaging.outbox.publisher.interval-ms=100"));
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.environment().keySet().removeIf(key -> key.matches(
                "(?i)(SPRING_.*|APEX_.*|CELL_.*|ORDER_.*|INVENTORY_.*|KAFKA_.*|SERVER_.*|JAVA_TOOL_OPTIONS|JDK_JAVA_OPTIONS|_JAVA_OPTIONS)"));
        return builder.redirectErrorStream(true).redirectOutput(log.toFile()).start();
    }

    private static void ready(Process process, Path log, HttpClient http, int port) {
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            assertThat(process.isAlive()).withFailMessage(() -> read(log)).isTrue();
            try {
                http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/"))
                        .timeout(Duration.ofSeconds(2)).GET().build(), HttpResponse.BodyHandlers.discarding());
            } catch (Exception notReady) {
                throw new AssertionError("Waiting for isolated HTTP process", notReady);
            }
            assertThat(read(log)).contains("Started ");
        });
    }

    private static String read(Path file) {
        try { return Files.readString(file); } catch (Exception failure) { return failure.toString(); }
    }

    private static void stop(Process process) throws Exception {
        if (process != null && process.isAlive()) {
            process.destroy();
            if (!process.waitFor(20, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                assertThat(process.waitFor(10, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static Map<String, Object> item(UUID id, int quantity) {
        return Map.of("productId", id, "quantity", quantity);
    }

    private static JsonNode post(HttpClient http, int port, List<Map<String, Object>> items) throws Exception {
        String body = JSON.writeValueAsString(Map.of("externalReference", "saga-" + UUID.randomUUID(),
                "description", PERSIAN, "items", items));
        var response = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/api/v1/order-placements"))
                .timeout(Duration.ofSeconds(10)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build(),
                HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        assertThat(response.statusCode()).isEqualTo(201);
        JsonNode json = JSON.readTree(response.body());
        assertThat(json.get("status").asText()).isEqualTo("PENDING_INVENTORY");
        return json;
    }

    private static String getStatus(HttpClient http, int port, UUID id) throws Exception {
        var response = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port
                + "/api/v1/order-placements/" + id)).timeout(Duration.ofSeconds(10)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return JSON.readTree(response.body()).get("status").asText();
    }

    private static String status(JdbcTemplate jdbc, UUID id) {
        return jdbc.queryForObject("SELECT status FROM orders WHERE id=?", String.class, id);
    }

    private static void awaitStatus(JdbcTemplate jdbc, UUID id, String status) {
        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> assertThat(status(jdbc, id)).isEqualTo(status));
    }

    private static void assertStock(JdbcTemplate jdbc, UUID product, int available, int reserved) {
        assertThat(jdbc.queryForObject("SELECT available_quantity FROM inventory_stock WHERE product_id=?",
                Integer.class, product)).isEqualTo(available);
        assertThat(jdbc.queryForObject("SELECT reserved_quantity FROM inventory_stock WHERE product_id=?",
                Integer.class, product)).isEqualTo(reserved);
    }

    private static long count(JdbcTemplate jdbc, String table, String column, UUID id) {
        return jdbc.queryForObject("SELECT count(*) FROM " + table + " WHERE " + column + "=?", Long.class, id);
    }

    private static Map<String, Object> eventFor(JdbcTemplate jdbc, UUID orderId, UUID causation) {
        if (causation == null) {
            return jdbc.queryForMap("SELECT * FROM outbox_event WHERE aggregate_id=?", orderId);
        }
        return jdbc.queryForMap("SELECT * FROM outbox_event WHERE aggregate_id=? AND causation_id=?", orderId, causation);
    }

    private static void awaitPublished(JdbcTemplate jdbc, UUID id) {
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(jdbc.queryForObject(
                "SELECT published FROM outbox_event WHERE event_id=?", Boolean.class, id)).isTrue());
    }

    private static void assertEnvelope(JdbcTemplate jdbc, UUID id, ConsumerRecord<String, String> wire) {
        var row = jdbc.queryForMap("SELECT * FROM outbox_event WHERE event_id=?", id);
        JsonNode metadata = JSON.readTree(wire.value()).get("metadata");
        Map<String, String> names = Map.of("eventId", "event_id", "eventType", "event_type", "eventVersion",
                "event_version", "correlationId", "correlation_id", "aggregateId", "aggregate_id",
                "aggregateType", "aggregate_type", "sourceService", "source_service", "cellId", "cell_id");
        names.forEach((json, sql) -> assertThat(metadata.get(json).asText()).isEqualTo(row.get(sql).toString()));
        if (row.get("causation_id") == null) {
            assertThat(metadata.get("causationId") == null || metadata.get("causationId").isNull()).isTrue();
        } else {
            assertThat(metadata.get("causationId").asText()).isEqualTo(row.get("causation_id").toString());
        }
        var occurred = jdbc.queryForObject("SELECT occurred_at FROM outbox_event WHERE event_id=?",
                (rs, n) -> rs.getObject(1, OffsetDateTime.class).toInstant(), id);
        assertThat(java.time.Instant.parse(metadata.get("occurredAt").asText())).isEqualTo(occurred);
        assertThat(wire.key()).isEqualTo(row.get("aggregate_type") + ":" + row.get("aggregate_id"));
        assertThat(JSON.readTree(wire.value()).get("payload")).isEqualTo(JSON.readTree(row.get("payload").toString()));
    }

    private static KafkaConsumer<String, String> observer(String brokers) {
        return new KafkaConsumer<>(Map.of("bootstrap.servers", brokers, "group.id", "saga-observer-" + UUID.randomUUID(),
                "enable.auto.commit", false, "allow.auto.create.topics", false,
                "key.deserializer", StringDeserializer.class, "value.deserializer", StringDeserializer.class));
    }

    private static void assertQuarantine(ConsumerRecord<String, String> record, String raw, String source,
            long offset, String group, String errorType) {
        assertThat(record.topic()).isEqualTo(source + "-dlt");
        assertThat(record.partition()).isZero();
        assertThat(record.value()).isEqualTo(raw);
        assertThat(record.key()).isEqualTo(JSON.readTree(raw).get("metadata").get("aggregateType").asText()
                + ":" + JSON.readTree(raw).get("metadata").get("aggregateId").asText());
        var headers = record.headers();
        assertThat(new String(headers.lastHeader(org.springframework.kafka.support.KafkaHeaders.DLT_ORIGINAL_TOPIC).value(),
                StandardCharsets.UTF_8)).isEqualTo(source);
        assertThat(new String(headers.lastHeader(org.springframework.kafka.support.KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP).value(),
                StandardCharsets.UTF_8)).isEqualTo(group);
        assertThat(java.nio.ByteBuffer.wrap(headers.lastHeader(
                org.springframework.kafka.support.KafkaHeaders.DLT_ORIGINAL_OFFSET).value()).getLong()).isEqualTo(offset);
        assertThat(new String(headers.lastHeader(org.springframework.kafka.support.KafkaHeaders.DLT_EXCEPTION_FQCN).value(),
                StandardCharsets.UTF_8)).contains(errorType);
        assertThat(headers.lastHeader(org.springframework.kafka.support.KafkaHeaders.DLT_EXCEPTION_STACKTRACE)).isNull();
    }

    private static KafkaProducer<String, String> producer(String brokers) {
        return new KafkaProducer<>(Map.of("bootstrap.servers", brokers, "acks", "all",
                "key.serializer", StringSerializer.class, "value.serializer", StringSerializer.class));
    }

    private static ConsumerRecord<String, String> observe(KafkaConsumer<String, String> consumer,
            Map<UUID, ConsumerRecord<String, String>> cache, UUID id) {
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            if (!cache.containsKey(id)) {
                for (var record : consumer.poll(Duration.ofMillis(100))) {
                    UUID eventId = UUID.fromString(JSON.readTree(record.value()).get("metadata").get("eventId").asText());
                    cache.put(eventId, record);
                }
            }
            return cache.containsKey(id);
        });
        return cache.get(id);
    }

    private static void awaitConsumed(Admin admin, String group, String topic, long offset) {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var committed = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                    .get(10, TimeUnit.SECONDS).get(new TopicPartition(topic, 0));
            assertThat(committed).isNotNull();
            assertThat(committed.offset()).isGreaterThan(offset);
        });
    }

    private static Path verifyBuild(Path root, Path inventoryJar) throws Exception {
        Path receipt = root.resolve("out/m26-order-build.properties");
        assertThat(receipt).withFailMessage("Run inventory-cell/tools/verify-saga.ps1 for fresh, ordered builds").isRegularFile();
        Properties properties = new Properties();
        try (var reader = Files.newBufferedReader(receipt, StandardCharsets.UTF_8)) {
            properties.load(reader);
        }
        assertThat(properties.getProperty("schemaVersion")).isEqualTo("1");
        List<String> paths = new ArrayList<>(List.of("pom.xml", "inventory-cell/tools/verify-saga.ps1"));
        for (String module : MODULES) {
            paths.add(module + "/pom.xml");
            Path main = root.resolve(module + "/src/main");
            if (Files.isDirectory(main)) {
                try (var files = Files.walk(main)) {
                    files.filter(Files::isRegularFile)
                            .forEach(file -> paths.add(root.relativize(file).toString().replace('\\', '/')));
                }
            }
        }
        Collections.sort(paths);
        StringBuilder fingerprint = new StringBuilder();
        for (String path : paths) {
            fingerprint.append(path).append('\n').append(sha(root.resolve(path))).append('\n');
        }
        assertThat(hex(MessageDigest.getInstance("SHA-256").digest(fingerprint.toString().getBytes(StandardCharsets.UTF_8))))
                .withFailMessage("Order/Platform sources differ from clean-build receipt; rerun verify-saga.ps1")
                .isEqualTo(properties.getProperty("sourceSha256"));
        for (String module : MODULES) {
            Path jar = root.resolve(module.equals("order-cell") ? "order-cell/target/order-cell.jar"
                    : module + "/target/" + module + "-1.0-SNAPSHOT.jar");
            assertThat(sha(jar)).as(module).isEqualTo(properties.getProperty(module + ".sha256"));
        }
        try (JarFile jar = new JarFile(inventoryJar.toFile())) {
            assertThat(jar.getManifest().getMainAttributes().getValue("Start-Class"))
                    .isEqualTo("com.apex.inventory.InventoryCellApplication");
            for (String module : MODULES) {
                if (!module.equals("order-cell")) {
                    try (InputStream stream = jar.getInputStream(jar.getEntry("BOOT-INF/lib/" + module + "-1.0-SNAPSHOT.jar"))) {
                        assertThat(hex(MessageDigest.getInstance("SHA-256").digest(stream.readAllBytes())))
                                .as("Inventory uses the same Platform snapshot: " + module)
                                .isEqualTo(properties.getProperty(module + ".sha256"));
                    }
                }
            }
        }
        return root.resolve("order-cell/target/order-cell.jar");
    }

    private static String sha(Path path) throws Exception {
        assertThat(path).isRegularFile();
        return hex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path)));
    }

    private static String hex(byte[] hash) {
        return HexFormat.of().formatHex(hash);
    }
}
