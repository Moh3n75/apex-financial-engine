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
    static final String ORDER_TOPIC = "apex.order.events.v1";
    static final String INVENTORY_TOPIC = "apex.inventory.events.v1";
    static final String PERSIAN = "سفارش آزمایشی";
    static final List<String> MODULES = List.of("apex-platform-core", "apex-platform-events",
            "apex-platform-context-autoconfigure", "apex-platform-webmvc", "apex-platform-messaging",
            "apex-starter-webmvc", "apex-starter-cell-messaging", "order-cell");
    static final JsonMapper JSON = JsonMapper.builder().build();

    @Test
    void controlledReplayRecoversBothPackagedCellsThroughDedicatedTopics() throws Exception {
        Path root = Path.of(System.getProperty("saga.repositoryRoot")).toAbsolutePath().normalize();
        Path inventoryJar = Path.of(System.getProperty("saga.inventoryJar")).toAbsolutePath().normalize();
        Path orderJar = verifyBuild(root, inventoryJar);
        Path toolJar = root.resolve("apex-replay-tool/target/apex-replay-tool.jar");
        assertThat(toolJar).isRegularFile();
        Path output = inventoryJar.getParent();
        String inventoryReplay = "apex.inventory.order-placed.replay.v1";
        String orderReplay = "apex.order.inventory-outcome.replay.v1";
        List<String> topics = List.of(ORDER_TOPIC, INVENTORY_TOPIC, ORDER_TOPIC + "-dlt", INVENTORY_TOPIC + "-dlt",
                inventoryReplay, inventoryReplay + "-dlt", orderReplay, orderReplay + "-dlt");
        List<Map<String, Object>> evidence = new ArrayList<>();
        var broker = new EmbeddedKafkaKraftBroker(1, 1, topics.toArray(String[]::new));
        broker.brokerProperty("auto.create.topics.enable", "false");
        broker.brokerListProperty("apex.m269.replay.test.brokers");
        Process order = null; Process inventory = null;
        try (var orderDb = EmbeddedPostgres.builder().start(); var inventoryDb = EmbeddedPostgres.builder().start();
             var auditDb = EmbeddedPostgres.builder().start();
             var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()) {
            try {
                broker.afterPropertiesSet(); String brokers = broker.getBrokersAsString();
                String orderGroup = "order-replay-test-" + UUID.randomUUID();
                String inventoryGroup = "inventory-replay-test-" + UUID.randomUUID();
                var orders = new JdbcTemplate(orderDb.getPostgresDatabase());
                var stock = new JdbcTemplate(inventoryDb.getPostgresDatabase());
                var audit = new JdbcTemplate(auditDb.getPostgresDatabase());
                int orderPort = freePort(); int inventoryPort = freePort();
                Path orderLog = output.resolve("replay-order.log"); Path inventoryLog = output.resolve("replay-inventory.log");
                order = launch(orderJar, orderDb, brokers, orderGroup, orderPort, orderLog, ORDER_TOPIC,
                        INVENTORY_TOPIC + "," + orderReplay, "--spring.profiles.active=saga,replay");
                inventory = launch(inventoryJar, inventoryDb, brokers, inventoryGroup, inventoryPort, inventoryLog, INVENTORY_TOPIC,
                        ORDER_TOPIC + "," + inventoryReplay, "--spring.profiles.active=saga,replay");
                ready(order, orderLog, http, orderPort); ready(inventory, inventoryLog, http, inventoryPort);
                assertThat(read(orderLog)).contains("topics=[" + INVENTORY_TOPIC + ", " + orderReplay + "]",
                        "Saga preflight validated INVENTORY_RESERVED V1", "Saga preflight validated INVENTORY_RESERVATION_REJECTED V1");
                assertThat(read(inventoryLog)).contains("topics=[" + ORDER_TOPIC + ", " + inventoryReplay + "]",
                        "Saga preflight validated ORDER_PLACED V1");
                evidence.add(Map.of("stage", "processes", "orderPid", order.pid(), "inventoryPid", inventory.pid(),
                        "brokers", brokers, "orderGroup", orderGroup, "inventoryGroup", inventoryGroup, "topics", topics));
                Path routes = output.resolve("replay-routes.json");
                Files.writeString(routes, JSON.writeValueAsString(List.of(
                        replayRoute(ORDER_TOPIC, inventoryGroup, inventoryReplay, List.of("ORDER_PLACED")),
                        replayRoute(INVENTORY_TOPIC, orderGroup, orderReplay, List.of("INVENTORY_RESERVED", "INVENTORY_RESERVATION_REJECTED")))));
                replayCli(toolJar, auditDb, brokers, evidence, output, "init-audit");
                UUID a = UUID.randomUUID(); UUID b = UUID.randomUUID();
                stock.update("INSERT INTO inventory_stock(product_id,available_quantity,reserved_quantity) VALUES (?,10,0),(?,5,0)", a, b);
                technicalFailure(stock); technicalFailure(orders);
                try (var admin = Admin.create(Map.of("bootstrap.servers", brokers));
                     var sourceObserver = observer(brokers); var dltObserver = observer(brokers);
                     var replayObserver = observer(brokers); var unrelated = observer(brokers);
                     var duplicateProducer = producer(brokers)) {
                    assertThat(admin.listTopics().names().get(10, TimeUnit.SECONDS)).containsAll(topics);
                    sourceObserver.assign(List.of(new TopicPartition(ORDER_TOPIC, 0), new TopicPartition(INVENTORY_TOPIC, 0)));
                    sourceObserver.seekToBeginning(sourceObserver.assignment());
                    dltObserver.assign(List.of(new TopicPartition(ORDER_TOPIC + "-dlt", 0), new TopicPartition(INVENTORY_TOPIC + "-dlt", 0)));
                    dltObserver.seekToBeginning(dltObserver.assignment());
                    replayObserver.assign(List.of(new TopicPartition(inventoryReplay, 0), new TopicPartition(orderReplay, 0)));
                    replayObserver.seekToBeginning(replayObserver.assignment());
                    unrelated.subscribe(List.of(ORDER_TOPIC));
                    await().atMost(Duration.ofSeconds(20)).until(() -> {
                        unrelated.poll(Duration.ofMillis(100)); return !unrelated.assignment().isEmpty();
                    });
                    unrelated.seekToBeginning(unrelated.assignment());
                    var sources = new HashMap<UUID, ConsumerRecord<String, String>>();
                    var dlts = new HashMap<UUID, ConsumerRecord<String, String>>();
                    var replays = new HashMap<UUID, ConsumerRecord<String, String>>();
                    UUID orderId = UUID.fromString(post(http, orderPort, List.of(item(a, 2), item(b, 1))).path("orderId").asText());
                    var placed = eventFor(orders, orderId, null); UUID eventId = (UUID) placed.get("event_id");
                    var original = observe(sourceObserver, sources, eventId);
                    var quarantine = observe(dltObserver, dlts, eventId);
                    assertQuarantine(quarantine, original.value(), ORDER_TOPIC, original.offset(), inventoryGroup, "PSQLException");
                    awaitConsumed(admin, inventoryGroup, ORDER_TOPIC, original.offset());
                    assertThat(count(stock, "processed_event", "event_id", eventId)).isZero();
                    assertThat(count(stock, "inventory_reservations", "order_id", orderId)).isZero();
                    assertThat(count(stock, "outbox_event", "aggregate_id", orderId)).isZero();
                    assertStock(stock, a, 10, 0); assertStock(stock, b, 5, 0);
                    await().atMost(Duration.ofSeconds(20)).until(() -> {
                        boolean found = false;
                        for (var record : unrelated.poll(Duration.ofMillis(100))) { if (record.offset() == original.offset()) { found = true; } }
                        return found;
                    });
                    // Both faults are genuine deferred PostgreSQL commit failures, not business rejection.
                    stock.update("UPDATE replay_test_gate SET enabled=false");
                    long originalEnd = topicEnd(admin, ORDER_TOPIC);
                    var inventoryPlan = replayApproval(toolJar, auditDb, brokers, routes, quarantine, inventoryGroup,
                            inventoryReplay, evidence, output);
                    var inventoryReceipt = replayExecute(toolJar, auditDb, brokers, routes, inventoryPlan,
                            inventoryGroup, inventoryReplay, evidence, output);
                    assertThat(inventoryReceipt.path("publication").asText()).isEqualTo("PUBLISHED_TO_REPLAY_TOPIC");
                    var replayed = observe(replayObserver, replays, eventId);
                    assertReplayIdentity(original, replayed, inventoryPlan);
                    assertThat(topicEnd(admin, ORDER_TOPIC)).isEqualTo(originalEnd);
                    await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(stock.queryForObject(
                            "SELECT status FROM inventory_reservations WHERE order_id=?", String.class, orderId)).isEqualTo("RESERVED"));
                    assertStock(stock, a, 8, 2); assertStock(stock, b, 4, 1);
                    assertThat(stock.queryForObject("SELECT topic FROM processed_event WHERE event_id=? AND consumer_group=?",
                            String.class, eventId, inventoryGroup)).isEqualTo(inventoryReplay);
                    var outcome = eventFor(stock, orderId, eventId); UUID outcomeId = (UUID) outcome.get("event_id");
                    assertThat(outcome.get("correlation_id")).isEqualTo(placed.get("correlation_id"));
                    assertThat(outcome.get("causation_id")).isEqualTo(eventId);
                    var outcomeSource = observe(sourceObserver, sources, outcomeId);
                    var outcomeDlt = observe(dltObserver, dlts, outcomeId);
                    assertQuarantine(outcomeDlt, outcomeSource.value(), INVENTORY_TOPIC, outcomeSource.offset(), orderGroup, "PSQLException");
                    awaitConsumed(admin, orderGroup, INVENTORY_TOPIC, outcomeSource.offset());
                    assertThat(status(orders, orderId)).isEqualTo("PENDING_INVENTORY");
                    assertThat(count(orders, "processed_event", "event_id", outcomeId)).isZero();
                    orders.update("UPDATE replay_test_gate SET enabled=false");
                    long outcomeEnd = topicEnd(admin, INVENTORY_TOPIC);
                    var orderPlan = replayApproval(toolJar, auditDb, brokers, routes, outcomeDlt, orderGroup,
                            orderReplay, evidence, output);
                    var orderReceipt = replayExecute(toolJar, auditDb, brokers, routes, orderPlan, orderGroup, orderReplay, evidence, output);
                    assertThat(orderReceipt.path("publication").asText()).isEqualTo("PUBLISHED_TO_REPLAY_TOPIC");
                    assertReplayIdentity(outcomeSource, observe(replayObserver, replays, outcomeId), orderPlan);
                    awaitStatus(orders, orderId, "PENDING_PAYMENT");
                    assertThat(getStatus(http, orderPort, orderId)).isEqualTo("PENDING_PAYMENT");
                    assertThat(orders.queryForObject("SELECT topic FROM processed_event WHERE event_id=? AND consumer_group=?",
                            String.class, outcomeId, orderGroup)).isEqualTo(orderReplay);
                    assertThat(topicEnd(admin, INVENTORY_TOPIC)).isEqualTo(outcomeEnd);
                    assertThat(unrelated.poll(Duration.ofMillis(500)).isEmpty()).isTrue();
                    long replayEnd = topicEnd(admin, inventoryReplay);
                    replayExecute(toolJar, auditDb, brokers, routes, inventoryPlan, inventoryGroup, inventoryReplay, evidence, output);
                    assertThat(topicEnd(admin, inventoryReplay)).isEqualTo(replayEnd);
                    var duplicate = duplicateProducer.send(new ProducerRecord<>(inventoryReplay, 0, original.key(), original.value())).get(10, TimeUnit.SECONDS);
                    awaitConsumed(admin, inventoryGroup, inventoryReplay, duplicate.offset());
                    assertStock(stock, a, 8, 2); assertStock(stock, b, 4, 1);
                    assertThat(count(stock, "outbox_event", "aggregate_id", orderId)).isEqualTo(1);
                    assertThat(count(stock, "processed_event", "event_id", eventId)).isEqualTo(1);
                    assertThat(audit.queryForObject("SELECT count(*) FROM replay_delivery WHERE state='PUBLISHED'", Integer.class)).isEqualTo(2);
                    assertThat(audit.queryForObject("SELECT count(*) FROM replay_action WHERE action='PUBLISHED'", Integer.class)).isEqualTo(2);
                    evidence.add(Map.of("stage", "bidirectional-recovery", "orderId", orderId, "placedId", eventId,
                            "outcomeId", outcomeId, "correlationId", placed.get("correlation_id"), "status", "PENDING_PAYMENT",
                            "inventoryReceipt", inventoryReceipt, "orderReceipt", orderReceipt, "sourceOffsetsUnchanged", true,
                            "unrelatedGroupReceivedNoReplay", true));
                    // Deliberately unresolved test-only fault: replay itself must quarantine, never loop.
                    stock.update("UPDATE replay_test_gate SET enabled=true");
                    UUID failedOrder = UUID.fromString(post(http, orderPort, List.of(item(a, 1))).path("orderId").asText());
                    UUID failedId = (UUID) eventFor(orders, failedOrder, null).get("event_id");
                    var failedDlt = observe(dltObserver, dlts, failedId);
                    var negativePlan = replayApproval(toolJar, auditDb, brokers, routes, failedDlt, inventoryGroup,
                            inventoryReplay, evidence, output);
                    var negativeReceipt = replayExecute(toolJar, auditDb, brokers, routes, negativePlan,
                            inventoryGroup, inventoryReplay, evidence, output);
                    try (var replayDlt = observer(brokers)) {
                        replayDlt.assign(List.of(new TopicPartition(inventoryReplay + "-dlt", 0)));
                        replayDlt.seekToBeginning(replayDlt.assignment());
                        var failedReplay = observe(replayDlt, new HashMap<>(), failedId);
                        assertQuarantine(failedReplay, failedDlt.value(), inventoryReplay,
                                negativeReceipt.path("deliveries").get(0).path("offset").asLong(), inventoryGroup, "PSQLException");
                        awaitConsumed(admin, inventoryGroup, inventoryReplay,
                                negativeReceipt.path("deliveries").get(0).path("offset").asLong());
                    }
                    assertThat(count(stock, "inventory_reservations", "order_id", failedOrder)).isZero();
                    assertThat(count(stock, "processed_event", "event_id", failedId)).isZero();
                    assertStock(stock, a, 8, 2);
                    stock.update("UPDATE replay_test_gate SET enabled=false");
                    long noLoopEnd = topicEnd(admin, inventoryReplay);
                    long oldPid = inventory.pid(); stop(inventory); inventory = null;
                    inventory = launch(inventoryJar, inventoryDb, brokers, inventoryGroup, inventoryPort,
                            output.resolve("replay-inventory-restarted.log"), INVENTORY_TOPIC, ORDER_TOPIC + "," + inventoryReplay,
                            "--spring.profiles.active=saga,replay");
                    ready(inventory, output.resolve("replay-inventory-restarted.log"), http, inventoryPort);
                    assertThat(topicEnd(admin, inventoryReplay)).isEqualTo(noLoopEnd);
                    assertThat(count(stock, "processed_event", "event_id", failedId)).isZero();
                    assertThat(status(orders, failedOrder)).isEqualTo("PENDING_INVENTORY");
                    evidence.add(Map.of("stage", "replay-dlt-no-auto-replay", "failedEventId", failedId,
                            "oldInventoryPid", oldPid, "newInventoryPid", inventory.pid(), "replayEnd", noLoopEnd));
                }
                Files.writeString(output.resolve("controlled-replay-evidence.json"), JSON.writerWithDefaultPrettyPrinter().writeValueAsString(evidence));
            } finally {
                try { stop(inventory); } finally { try { stop(order); } finally { broker.destroy(); } }
            }
        }
    }

    static Map<String, Object> replayRoute(String source, String group, String replay, List<String> types) {
        return Map.of("originTopic", source, "dltTopic", source + "-dlt", "targetGroup", group,
                "replayTopic", replay, "replayDltTopic", replay + "-dlt", "eventTypes", types);
    }

    static void technicalFailure(JdbcTemplate db) {
        db.execute("CREATE TABLE replay_test_gate(enabled boolean NOT NULL)"); db.update("INSERT INTO replay_test_gate VALUES(true)");
        db.execute("CREATE FUNCTION replay_test_commit_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                + "IF (SELECT enabled FROM replay_test_gate) THEN RAISE EXCEPTION 'test technical commit failure'; END IF; RETURN NEW; END $$");
        db.execute("CREATE CONSTRAINT TRIGGER replay_test_failure AFTER INSERT ON processed_event "
                + "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION replay_test_commit_failure()");
    }

    static long topicEnd(Admin admin, String topic) throws Exception {
        return admin.listOffsets(Map.of(new TopicPartition(topic, 0), org.apache.kafka.clients.admin.OffsetSpec.latest()))
                .all().get(10, TimeUnit.SECONDS).values().iterator().next().offset();
    }

    static JsonNode replayApproval(Path jar, EmbeddedPostgres db, String brokers, Path routes,
            ConsumerRecord<String, String> dlt, String group, String replay, List<Map<String, Object>> evidence, Path output) throws Exception {
        var inspection = replayCli(jar, db, brokers, evidence, output, "inspect", "--routes", routes.toString(),
                "--dlt-topic", dlt.topic(), "--partition", "0", "--offset", Long.toString(dlt.offset()), "--allow-kafka-interaction");
        String eventId = JSON.readTree(dlt.value()).path("metadata").path("eventId").asText();
        assertThat(inspection.path("eventId").asText()).isEqualTo(eventId);
        Path selector = output.resolve("selector-" + eventId + ".json");
        Files.writeString(selector, JSON.writeValueAsString(List.of(Map.of("dlt", Map.of("topic", dlt.topic(), "partition", 0,
                "offset", dlt.offset()), "eventId", eventId, "rawSha256", inspection.path("rawSha256").asText()))));
        var plan = replayCli(jar, db, brokers, evidence, output, "plan", "--routes", routes.toString(), "--selector", selector.toString(),
                "--target-group", group, "--replay-topic", replay, "--actor", "test-operator", "--reason", "controlled test cause review",
                "--limit", "10", "--allow-kafka-interaction");
        replayCli(jar, db, brokers, evidence, output, "approve", "--plan-id", plan.path("planId").asText(),
                "--confirm-digest", plan.path("digest").asText(), "--actor", "test-approver", "--reason", "explicit bounded test approval");
        return plan;
    }

    static JsonNode replayExecute(Path jar, EmbeddedPostgres db, String brokers, Path routes, JsonNode plan,
            String group, String replay, List<Map<String, Object>> evidence, Path output) throws Exception {
        return replayCli(jar, db, brokers, evidence, output, "execute", "--routes", routes.toString(),
                "--plan-id", plan.path("planId").asText(), "--confirm-digest", plan.path("digest").asText(),
                "--target-group", group, "--replay-topic", replay, "--actor", "test-operator", "--reason", "explicit test recovery",
                "--failure-resolved", "operator test attestation; negative scenario intentionally retains fault",
                "--max-records", "10", "--rate", "10", "--attempts", "1", "--allow-kafka-interaction");
    }

    static JsonNode replayCli(Path jar, EmbeddedPostgres db, String brokers, List<Map<String, Object>> evidence,
            Path output, String... args) throws Exception {
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx128m", "-Dfile.encoding=UTF-8", "-jar", jar.toString())); command.addAll(List.of(args));
        Path log = output.resolve("replay-cli-" + UUID.randomUUID() + ".log");
        var builder = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile());
        builder.environment().keySet().removeIf(k -> k.matches("(?i)(REPLAY_.*|SPRING_.*|JAVA_TOOL_OPTIONS|JDK_JAVA_OPTIONS|_JAVA_OPTIONS)"));
        builder.environment().putAll(Map.of("REPLAY_KAFKA_BOOTSTRAP_SERVERS", brokers,
                "REPLAY_AUDIT_DB_URL", db.getJdbcUrl("postgres", "postgres"), "REPLAY_AUDIT_DB_USERNAME", "postgres", "REPLAY_AUDIT_DB_PASSWORD", ""));
        var process = builder.start();
        try {
            assertThat(process.waitFor(30, TimeUnit.SECONDS)).isTrue(); assertThat(process.exitValue()).as(read(log)).isZero();
            String result = Files.readAllLines(log).stream().filter(s -> !s.isBlank()).reduce((a,b) -> b).orElseThrow();
            JsonNode json = result.startsWith("{") ? JSON.readTree(result) : JSON.valueToTree(Map.of("message", result));
            evidence.add(Map.of("stage", "cli", "command", args[0], "pid", process.pid(), "result", json)); return json;
        } finally { stop(process); }
    }

    static void assertReplayIdentity(ConsumerRecord<String, String> original, ConsumerRecord<String, String> replay, JsonNode plan) {
        assertThat(replay.value().getBytes(StandardCharsets.UTF_8)).isEqualTo(original.value().getBytes(StandardCharsets.UTF_8));
        assertThat(replay.key()).isEqualTo(original.key());
        assertThat(new String(replay.headers().lastHeader("apex-replay-plan-id").value(), StandardCharsets.UTF_8))
                .isEqualTo(plan.path("planId").asText());
        replay.headers().forEach(header -> assertThat(header.key()).doesNotStartWith("kafka_dlt-"));
    }

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
                            + "IF (SELECT enabled FROM saga_failure_gate) THEN "
                            + "IF nextval('saga_failure_attempt') >= 3 THEN PERFORM pg_advisory_xact_lock(268267); END IF; "
                            + "IF (SELECT enabled FROM saga_failure_gate) THEN RAISE EXCEPTION 'saga injected commit failure'; END IF; "
                            + "END IF; RETURN NEW; END $$");
                    stock.execute("CREATE CONSTRAINT TRIGGER saga_commit_failure AFTER INSERT ON outbox_event "
                            + "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION saga_fail_commit()");
                    UUID failureId;
                    try (var faultConnection=inventoryDb.getPostgresDatabase().getConnection();
                         var faultBarrier=faultConnection.createStatement()) {
                        // Hold the final attempt until rollback/offset assertions finish; no timing race with retry exhaustion.
                        faultBarrier.execute("SELECT pg_advisory_lock(268267)");
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
                        faultBarrier.execute("SELECT pg_advisory_unlock(268267)");
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
                        faultBarrier.execute("SELECT pg_advisory_unlock(268267)");
                        stock.execute("DROP TRIGGER saga_commit_failure ON outbox_event");
                        stock.execute("DROP FUNCTION saga_fail_commit()");
                        stock.execute("DROP SEQUENCE saga_failure_attempt");
                        stock.execute("DROP TABLE saga_failure_gate");
                    }
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

    static Process launch(Path jar, EmbeddedPostgres db, String brokers, String group,
                                  int port, Path log, String publish, String subscribe, String... overrides) throws Exception {
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
        if (subscribe == null) {
            // New profile acceptance can exercise the packaged topic configuration instead of masking it with CLI values.
            command.removeIf(arg -> arg.startsWith("--apex.messaging.consumer.topics="));
        }
        for (String override : overrides) {
            String name = override.substring(0, override.indexOf('=') + 1);
            command.removeIf(arg -> arg.startsWith(name)); command.add(override);
        }
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.environment().keySet().removeIf(key -> key.matches(
                "(?i)(SPRING_.*|APEX_.*|CELL_.*|ORDER_.*|INVENTORY_.*|PAYMENT_.*|KAFKA_.*|SERVER_.*|JAVA_TOOL_OPTIONS|JDK_JAVA_OPTIONS|_JAVA_OPTIONS)"));
        return builder.redirectErrorStream(true).redirectOutput(log.toFile()).start();
    }

    static void ready(Process process, Path log, HttpClient http, int port) {
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

    static String read(Path file) {
        try { return Files.readString(file); } catch (Exception failure) { return failure.toString(); }
    }

    static void stop(Process process) throws Exception {
        if (process != null && process.isAlive()) {
            process.destroy();
            if (!process.waitFor(20, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                assertThat(process.waitFor(10, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    static Map<String, Object> item(UUID id, int quantity) {
        return Map.of("productId", id, "quantity", quantity);
    }

    static JsonNode post(HttpClient http, int port, List<Map<String, Object>> items) throws Exception {
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

    static String getStatus(HttpClient http, int port, UUID id) throws Exception {
        var response = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port
                + "/api/v1/order-placements/" + id)).timeout(Duration.ofSeconds(10)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return JSON.readTree(response.body()).get("status").asText();
    }

    static String status(JdbcTemplate jdbc, UUID id) {
        return jdbc.queryForObject("SELECT status FROM orders WHERE id=?", String.class, id);
    }

    static void awaitStatus(JdbcTemplate jdbc, UUID id, String status) {
        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> assertThat(status(jdbc, id)).isEqualTo(status));
    }

    static void assertStock(JdbcTemplate jdbc, UUID product, int available, int reserved) {
        assertThat(jdbc.queryForObject("SELECT available_quantity FROM inventory_stock WHERE product_id=?",
                Integer.class, product)).isEqualTo(available);
        assertThat(jdbc.queryForObject("SELECT reserved_quantity FROM inventory_stock WHERE product_id=?",
                Integer.class, product)).isEqualTo(reserved);
    }

    static long count(JdbcTemplate jdbc, String table, String column, UUID id) {
        return jdbc.queryForObject("SELECT count(*) FROM " + table + " WHERE " + column + "=?", Long.class, id);
    }

    static Map<String, Object> eventFor(JdbcTemplate jdbc, UUID orderId, UUID causation) {
        if (causation == null) {
            return jdbc.queryForMap("SELECT * FROM outbox_event WHERE aggregate_id=?", orderId);
        }
        return jdbc.queryForMap("SELECT * FROM outbox_event WHERE aggregate_id=? AND causation_id=?", orderId, causation);
    }

    static void awaitPublished(JdbcTemplate jdbc, UUID id) {
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(jdbc.queryForObject(
                "SELECT published FROM outbox_event WHERE event_id=?", Boolean.class, id)).isTrue());
    }

    static void assertEnvelope(JdbcTemplate jdbc, UUID id, ConsumerRecord<String, String> wire) {
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

    static KafkaConsumer<String, String> observer(String brokers) {
        return new KafkaConsumer<>(Map.of("bootstrap.servers", brokers, "group.id", "saga-observer-" + UUID.randomUUID(),
                "enable.auto.commit", false, "allow.auto.create.topics", false,
                "key.deserializer", StringDeserializer.class, "value.deserializer", StringDeserializer.class));
    }

    static void assertQuarantine(ConsumerRecord<String, String> record, String raw, String source,
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

    static KafkaProducer<String, String> producer(String brokers) {
        return new KafkaProducer<>(Map.of("bootstrap.servers", brokers, "acks", "all",
                "key.serializer", StringSerializer.class, "value.serializer", StringSerializer.class));
    }

    static ConsumerRecord<String, String> observe(KafkaConsumer<String, String> consumer,
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

    static void awaitConsumed(Admin admin, String group, String topic, long offset) {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var committed = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                    .get(10, TimeUnit.SECONDS).get(new TopicPartition(topic, 0));
            assertThat(committed).isNotNull();
            assertThat(committed.offset()).isGreaterThan(offset);
        });
    }

    static Path verifyBuild(Path root, Path inventoryJar) throws Exception {
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

    static String sha(Path path) throws Exception {
        assertThat(path).isRegularFile();
        return hex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path)));
    }

    static String hex(byte[] hash) {
        return HexFormat.of().formatHex(hash);
    }
}
