package com.apex.inventory;

import static com.apex.inventory.OrderInventorySagaIT.*;
import static com.apex.inventory.ThreeCellSagaIT.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

/** Four owning PostgreSQL servers, three packaged Cells, separately packaged operator CLI. */
@Tag("integration")
class OperationalRecoverySagaIT {
    static final String INVENTORY_REPLAY = "apex.inventory.order-placed.replay.v1";
    static final String ORDER_INVENTORY_REPLAY = "apex.order.inventory-outcome.replay.v1";
    static final String PAYMENT_REPLAY = "apex.payment.authorization-request.replay.v1";
    static final String ORDER_PAYMENT_REPLAY = "apex.order.payment-outcome.replay.v1";
    static final String RELEASE_REPLAY = "apex.inventory.release-request.replay.v1";
    private final List<Map<String, Object>> evidence = new ArrayList<>();
    private final Map<UUID, ConsumerRecord<String, String>> sourceCache = new HashMap<>();
    private final Map<UUID, ConsumerRecord<String, String>> dltCache = new HashMap<>();
    private final Map<UUID, ConsumerRecord<String, String>> replayCache = new HashMap<>();
    private final String orderGroup = "order-m29-" + UUID.randomUUID();
    private final String inventoryGroup = "inventory-m29-" + UUID.randomUUID();
    private final String paymentGroup = "payment-m29-" + UUID.randomUUID();
    private final String token = UUID.randomUUID().toString() + UUID.randomUUID();
    private Path root, output, orderJar, inventoryJar, paymentJar, toolJar, routes;
    private EmbeddedPostgres orderDb, inventoryDb, paymentDb, auditDb;
    private EmbeddedKafkaKraftBroker broker;
    private Process order, inventory, payment;
    private JdbcTemplate orders, stock, payments, audit;
    private Admin admin;
    private KafkaConsumer<String, String> sourceObserver, dltObserver, replayObserver;
    private KafkaProducer<String, String> testProducer;
    private HttpClient http;
    private String brokers;
    private int orderPort, inventoryPort;

    @Test
    void packagedOperatorsRecoverEveryNewStreamWithoutTimingOutBusinessState() throws Exception {
        try {
            start();
            UUID a = sku(), b = sku(), c = sku(), d = sku();

            // No payment response is UNKNOWN, never DECLINED or a reason to release stock.
            stopPayment();
            UUID happy = place(a, 2);
            awaitStatus(orders, happy, "PENDING_PAYMENT");
            var waiting = awaitStalled(happy, "PENDING_PAYMENT");
            assertThat(waiting.path("paymentDecision").isNull()).isTrue();
            assertThat(waiting.path("releaseAcknowledged").asBoolean()).isFalse();
            assertThat(count(payments, "payment_authorizations", "order_id", happy)).isZero();
            assertThat(outboxCount(orders, happy, "ORDER_CANCELLATION_REQUESTED")).isZero();
            assertThat(status(orders, happy)).isEqualTo("PENDING_PAYMENT");
            policy(happy, "AUTHORIZED"); startPayment();
            finalStatus(happy, "CONFIRMED"); assertStock(stock, a, 8, 2);
            assertThat(count(payments, "payment_authorizations", "order_id", happy)).isEqualTo(1);
            assertThat(payments.queryForObject("SELECT status FROM payment_authorizations WHERE order_id=?", String.class, happy)).isEqualTo("AUTHORIZED");
            assertThat(stock.queryForObject("SELECT status FROM inventory_reservations WHERE order_id=?", String.class, happy)).isEqualTo("RESERVED");
            assertThat(outboxCount(payments, happy, "PAYMENT_AUTHORIZED")).isEqualTo(1);
            var happyRequest = typed(orders, happy, "ORDER_READY_FOR_PAYMENT");
            var happyAuthorization = typed(payments, happy, "PAYMENT_AUTHORIZED");
            assertLink(happyRequest, happyAuthorization);
            evidence.add(Map.of("stage", "payment-down-unknown-then-restart", "orderId", happy,
                    "stalled", waiting, "finalStatus", "CONFIRMED", "stock", List.of(8, 2)));

            // Explicit business decline is durable; release COMMIT failure cannot prematurely cancel.
            stopPayment(); setFault(stock, "ORDER_CANCELLATION_REQUESTED", true);
            UUID declined = place(b, 3); policy(declined, "DECLINED"); startPayment();
            awaitStatus(orders, declined, "CANCELLING");
            var cancellation = typed(orders, declined, "ORDER_CANCELLATION_REQUESTED");
            assertThat(payments.queryForObject("SELECT status FROM payment_authorizations WHERE order_id=?", String.class, declined)).isEqualTo("DECLINED");
            assertThat(count(payments, "payment_authorizations", "order_id", declined)).isEqualTo(1);
            assertLink(typed(payments, declined, "PAYMENT_DECLINED"), cancellation);
            var releaseSource = source(orders, cancellation);
            var releaseDlt = quarantined(releaseSource, inventoryGroup);
            assertStock(stock, b, 7, 3);
            assertThat(count(stock, "processed_event", "event_id", eventId(cancellation))).isZero();
            assertThat(outboxCount(stock, declined, "INVENTORY_RELEASED")).isZero();
            var cancelling = awaitStalled(declined, "CANCELLING");
            assertThat(cancelling.path("paymentDecision").asText()).isEqualTo("DECLINED");
            assertThat(cancelling.path("releaseAcknowledged").asBoolean()).isFalse();
            setFault(stock, "ORDER_CANCELLATION_REQUESTED", false);
            recover(releaseSource, releaseDlt, inventoryGroup, RELEASE_REPLAY);
            finalStatus(declined, "CANCELLED"); assertStock(stock, b, 10, 0);
            assertThat(stock.queryForObject("SELECT status FROM inventory_reservations WHERE order_id=?", String.class, declined))
                    .isEqualTo("RELEASED");
            assertThat(stock.queryForObject("SELECT topic FROM processed_event WHERE event_id=? AND consumer_group=?",
                    String.class, eventId(cancellation), inventoryGroup)).isEqualTo(RELEASE_REPLAY);
            assertLink(cancellation, typed(stock, declined, "INVENTORY_RELEASED"));
            evidence.add(Map.of("stage", "decline-release-request-recovery", "orderId", declined,
                    "stalled", cancelling, "finalStatus", "CANCELLED", "stock", List.of(10, 0)));

            // A valid authorization request is quarantined by a real deferred database COMMIT fault.
            stopPayment(); setFault(payments, "ORDER_READY_FOR_PAYMENT", true);
            UUID paymentFailure = place(c, 2); policy(paymentFailure, "AUTHORIZED"); startPayment();
            awaitStatus(orders, paymentFailure, "PENDING_PAYMENT");
            var request = typed(orders, paymentFailure, "ORDER_READY_FOR_PAYMENT");
            var requestSource = source(orders, request);
            var requestDlt = quarantined(requestSource, paymentGroup);
            assertThat(count(payments, "payment_authorizations", "order_id", paymentFailure)).isZero();
            assertThat(count(payments, "processed_event", "event_id", eventId(request))).isZero();
            assertThat(count(payments, "outbox_event", "aggregate_id", paymentFailure)).isZero();
            awaitStalled(paymentFailure, "PENDING_PAYMENT"); assertStock(stock, c, 8, 2);
            setFault(payments, "ORDER_READY_FOR_PAYMENT", false);
            recover(requestSource, requestDlt, paymentGroup, PAYMENT_REPLAY);
            finalStatus(paymentFailure, "CONFIRMED"); assertStock(stock, c, 8, 2);
            assertThat(payments.queryForObject("SELECT status FROM payment_authorizations WHERE order_id=?", String.class, paymentFailure)).isEqualTo("AUTHORIZED");
            assertThat(payments.queryForObject("SELECT topic FROM processed_event WHERE event_id=? AND consumer_group=?",
                    String.class, eventId(request), paymentGroup)).isEqualTo(PAYMENT_REPLAY);
            assertThat(count(payments, "payment_authorizations", "order_id", paymentFailure)).isEqualTo(1);
            assertThat(outboxCount(payments, paymentFailure, "PAYMENT_AUTHORIZED")).isEqualTo(1);
            assertLink(request, typed(payments, paymentFailure, "PAYMENT_AUTHORIZED"));
            evidence.add(Map.of("stage", "payment-request-recovery", "orderId", paymentFailure,
                    "originalEventId", eventId(request), "status", "CONFIRMED", "stock", List.of(8, 2)));

            // Payment outcomes share the Order group with Inventory outcomes, but use their own replay route.
            setFault(orders, "PAYMENT_AUTHORIZED", true); stopPayment();
            UUID outcomeFailure = place(d, 1); policy(outcomeFailure, "AUTHORIZED"); startPayment();
            awaitStatus(orders, outcomeFailure, "PENDING_PAYMENT");
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                    assertThat(outboxCount(payments, outcomeFailure, "PAYMENT_AUTHORIZED")).isEqualTo(1));
            var authorization = typed(payments, outcomeFailure, "PAYMENT_AUTHORIZED");
            var outcomeSource = source(payments, authorization);
            var outcomeDlt = quarantined(outcomeSource, orderGroup);
            assertThat(count(orders, "processed_event", "event_id", eventId(authorization))).isZero();
            setFault(orders, "PAYMENT_AUTHORIZED", false);
            recover(outcomeSource, outcomeDlt, orderGroup, ORDER_PAYMENT_REPLAY);
            finalStatus(outcomeFailure, "CONFIRMED"); assertStock(stock, d, 9, 1);
            assertThat(orders.queryForObject("SELECT topic FROM processed_event WHERE event_id=? AND consumer_group=?",
                    String.class, eventId(authorization), orderGroup)).isEqualTo(ORDER_PAYMENT_REPLAY);
            evidence.add(Map.of("stage", "payment-outcome-recovery", "orderId", outcomeFailure,
                    "originalEventId", eventId(authorization), "status", "CONFIRMED"));

            // Inventory already released stock; an Order COMMIT fault quarantines only the acknowledgement.
            UUID e = sku(); setFault(orders, "INVENTORY_RELEASED", true); stopPayment();
            UUID ackFailure = place(e, 3); policy(ackFailure, "DECLINED"); startPayment();
            awaitStatus(orders, ackFailure, "CANCELLING");
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(outboxCount(stock, ackFailure, "INVENTORY_RELEASED")).isEqualTo(1));
            var released = typed(stock, ackFailure, "INVENTORY_RELEASED");
            var ackSource = source(stock, released);
            var ackDlt = quarantined(ackSource, orderGroup);
            assertStock(stock, e, 10, 0); awaitStalled(ackFailure, "CANCELLING");
            assertThat(stock.queryForObject("SELECT status FROM inventory_reservations WHERE order_id=?", String.class, ackFailure)).isEqualTo("RELEASED");
            assertThat(count(orders, "processed_event", "event_id", eventId(released))).isZero();
            setFault(orders, "INVENTORY_RELEASED", false);
            recover(ackSource, ackDlt, orderGroup, ORDER_INVENTORY_REPLAY);
            finalStatus(ackFailure, "CANCELLED"); assertStock(stock, e, 10, 0);
            assertThat(orders.queryForObject("SELECT topic FROM processed_event WHERE event_id=? AND consumer_group=?",
                    String.class, eventId(released), orderGroup)).isEqualTo(ORDER_INVENTORY_REPLAY);
            assertThat(outboxCount(stock, ackFailure, "INVENTORY_RELEASED")).isEqualTo(1);
            evidence.add(Map.of("stage", "inventory-release-outcome-recovery", "orderId", ackFailure,
                    "originalEventId", eventId(released), "status", "CANCELLED", "stock", List.of(10, 0)));

            // A terminal decline in Payment is not an Order fact until its original event commits there.
            UUID f = sku(); setFault(orders, "PAYMENT_DECLINED", true); stopPayment();
            UUID declineOutcomeFailure = place(f, 2); policy(declineOutcomeFailure, "DECLINED"); startPayment();
            awaitStatus(orders, declineOutcomeFailure, "PENDING_PAYMENT");
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                    assertThat(outboxCount(payments, declineOutcomeFailure, "PAYMENT_DECLINED")).isEqualTo(1));
            var declinedOutcome = typed(payments, declineOutcomeFailure, "PAYMENT_DECLINED");
            var declineSource = source(payments, declinedOutcome);
            var declineDlt = quarantined(declineSource, orderGroup);
            assertThat(payments.queryForObject("SELECT status FROM payment_authorizations WHERE order_id=?", String.class, declineOutcomeFailure)).isEqualTo("DECLINED");
            assertThat(orders.queryForObject("SELECT payment_decision FROM orders WHERE id=?", String.class, declineOutcomeFailure)).isNull();
            assertThat(outboxCount(orders, declineOutcomeFailure, "ORDER_CANCELLATION_REQUESTED")).isZero();
            assertStock(stock, f, 8, 2); awaitStalled(declineOutcomeFailure, "PENDING_PAYMENT");
            setFault(orders, "PAYMENT_DECLINED", false);
            recover(declineSource, declineDlt, orderGroup, ORDER_PAYMENT_REPLAY);
            finalStatus(declineOutcomeFailure, "CANCELLED"); assertStock(stock, f, 10, 0);
            assertThat(orders.queryForObject("SELECT topic FROM processed_event WHERE event_id=? AND consumer_group=?",
                    String.class, eventId(declinedOutcome), orderGroup)).isEqualTo(ORDER_PAYMENT_REPLAY);
            assertThat(outboxCount(orders, declineOutcomeFailure, "ORDER_CANCELLATION_REQUESTED")).isEqualTo(1);
            assertLink(declinedOutcome, typed(orders, declineOutcomeFailure, "ORDER_CANCELLATION_REQUESTED"));
            evidence.add(Map.of("stage", "payment-decline-outcome-recovery", "orderId", declineOutcomeFailure,
                    "originalEventId", eventId(declinedOutcome), "status", "CANCELLED", "stock", List.of(10, 0)));

            // Test-only transport redeliveries, distinct from the approved operator replay (which never rewrites IDs).
            duplicates(requestSource, paymentGroup);
            assertThat(count(payments, "payment_authorizations", "order_id", paymentFailure)).isEqualTo(1);
            assertThat(outboxCount(payments, paymentFailure, "PAYMENT_AUTHORIZED")).isEqualTo(1);
            duplicates(releaseSource, inventoryGroup);
            assertStock(stock, b, 10, 0); assertThat(status(orders, declined)).isEqualTo("CANCELLED");
            duplicates(ackSource, orderGroup); assertStock(stock, e, 10, 0);
            assertThat(status(orders, ackFailure)).isEqualTo("CANCELLED");
            assertThat(outboxCount(orders, declined, "ORDER_CANCELLATION_REQUESTED")).isEqualTo(1);

            negativePreflightAndUnknownVersion(requestSource, requestDlt);
            assertThat(audit.queryForObject("SELECT count(*) FROM replay_plan", Long.class)).isEqualTo(5);
            assertThat(audit.queryForObject("SELECT count(*) FROM replay_delivery WHERE state='PUBLISHED'", Long.class)).isEqualTo(5);
            assertThat(audit.queryForObject("SELECT count(*) FROM replay_action WHERE action='PUBLISHED'", Long.class)).isEqualTo(5);
            for (UUID id : List.of(happy, declined, paymentFailure, outcomeFailure, ackFailure, declineOutcomeFailure)) {
                assertThat(reports(id)).isFalse();
            }
            Files.writeString(output.resolve("m29-operational-recovery-evidence.json"),
                    JSON.writerWithDefaultPrettyPrinter().writeValueAsString(evidence));
        } finally {
            close();
        }
    }

    private void start() throws Exception {
        root = Path.of(System.getProperty("saga.repositoryRoot")).toAbsolutePath().normalize();
        inventoryJar = Path.of(System.getProperty("saga.inventoryJar")).toAbsolutePath().normalize();
        orderJar = verifyBuild(root, inventoryJar);
        paymentJar = root.resolve("payment-cell/target/payment-cell.jar");
        toolJar = root.resolve("apex-replay-tool/target/apex-replay-tool.jar");
        assertThat(toolJar).isRegularFile();
        var receipt = new Properties();
        try (var input = Files.newInputStream(root.resolve("out/m27-payment-build.properties"))) { receipt.load(input); }
        assertThat(sha(paymentJar)).isEqualTo(receipt.getProperty("paymentJarSha256"));
        output = inventoryJar.getParent();
        orderDb = EmbeddedPostgres.builder().start(); inventoryDb = EmbeddedPostgres.builder().start();
        paymentDb = EmbeddedPostgres.builder().start(); auditDb = EmbeddedPostgres.builder().start();
        var names = new ArrayList<String>();
        for (String topic : List.of(ORDER_TOPIC, INVENTORY_TOPIC, REQUEST, RELEASE, PAYMENT, "apex.order.legacy.events.v1",
                INVENTORY_REPLAY, ORDER_INVENTORY_REPLAY, PAYMENT_REPLAY, ORDER_PAYMENT_REPLAY, RELEASE_REPLAY)) {
            names.add(topic); names.add(topic + "-dlt");
        }
        broker = new EmbeddedKafkaKraftBroker(1, 1, names.toArray(String[]::new));
        broker.brokerProperty("auto.create.topics.enable", "false"); broker.brokerListProperty("apex.m29.test.brokers");
        broker.afterPropertiesSet(); brokers = broker.getBrokersAsString();
        http = HttpClient.newHttpClient(); orderPort = freePort(); inventoryPort = freePort();
        order = launch(orderJar, orderDb, brokers, orderGroup, orderPort, output.resolve("m29-order.log"), ORDER_TOPIC, null,
                "--spring.profiles.active=saga,three-cell-saga,replay,operations", "--apex.order.operations.read-token=" + token,
                "--apex.order.operations.age-threshold=200ms", "--apex.messaging.consumer.reliability.retry-delay=200ms");
        inventory = launch(inventoryJar, inventoryDb, brokers, inventoryGroup, inventoryPort, output.resolve("m29-inventory.log"), INVENTORY_TOPIC, null,
                "--spring.profiles.active=saga,three-cell-saga,replay", "--apex.messaging.consumer.reliability.retry-delay=200ms");
        ready(order, output.resolve("m29-order.log"), http, orderPort);
        ready(inventory, output.resolve("m29-inventory.log"), http, inventoryPort);
        startPayment();
        assertThat(read(output.resolve("m29-order.log"))).contains("topics=[" + INVENTORY_TOPIC + ", " + PAYMENT + ", " + ORDER_INVENTORY_REPLAY + ", " + ORDER_PAYMENT_REPLAY + "]");
        assertThat(read(output.resolve("m29-inventory.log"))).contains("topics=[" + ORDER_TOPIC + ", " + RELEASE + ", " + INVENTORY_REPLAY + ", " + RELEASE_REPLAY + "]");
        orders = new JdbcTemplate(orderDb.getPostgresDatabase()); stock = new JdbcTemplate(inventoryDb.getPostgresDatabase());
        payments = new JdbcTemplate(paymentDb.getPostgresDatabase()); audit = new JdbcTemplate(auditDb.getPostgresDatabase());
        for (var db : List.of(orders, stock, payments)) installFault(db);
        assertThat(orders.queryForList("SELECT tablename FROM pg_tables WHERE schemaname='public'", String.class)).doesNotContain("inventory_stock", "payment_authorizations");
        assertThat(stock.queryForList("SELECT tablename FROM pg_tables WHERE schemaname='public'", String.class)).doesNotContain("orders", "payment_authorizations");
        assertThat(payments.queryForList("SELECT tablename FROM pg_tables WHERE schemaname='public'", String.class)).doesNotContain("orders", "inventory_stock");
        admin = Admin.create(Map.of("bootstrap.servers", brokers));
        sourceObserver = observer(brokers); dltObserver = observer(brokers); replayObserver = observer(brokers); testProducer = producer(brokers);
        sourceObserver.assign(List.of(ORDER_TOPIC, INVENTORY_TOPIC, REQUEST, RELEASE, PAYMENT).stream().map(t -> new TopicPartition(t, 0)).toList());
        sourceObserver.seekToBeginning(sourceObserver.assignment());
        dltObserver.assign(List.of(ORDER_TOPIC, INVENTORY_TOPIC, REQUEST, RELEASE, PAYMENT).stream().map(t -> new TopicPartition(t + "-dlt", 0)).toList());
        dltObserver.seekToBeginning(dltObserver.assignment());
        replayObserver.assign(List.of(INVENTORY_REPLAY, ORDER_INVENTORY_REPLAY, PAYMENT_REPLAY, ORDER_PAYMENT_REPLAY, RELEASE_REPLAY)
                .stream().map(t -> new TopicPartition(t, 0)).toList());
        replayObserver.seekToBeginning(replayObserver.assignment());
        routes = output.resolve("m29-replay-routes.json");
        Files.writeString(routes, JSON.writeValueAsString(List.of(
                replayRoute(ORDER_TOPIC, inventoryGroup, INVENTORY_REPLAY, List.of("ORDER_PLACED")),
                replayRoute(INVENTORY_TOPIC, orderGroup, ORDER_INVENTORY_REPLAY, List.of("INVENTORY_RESERVED", "INVENTORY_RESERVATION_REJECTED", "INVENTORY_RELEASED")),
                replayRoute(REQUEST, paymentGroup, PAYMENT_REPLAY, List.of("ORDER_READY_FOR_PAYMENT")),
                replayRoute(PAYMENT, orderGroup, ORDER_PAYMENT_REPLAY, List.of("PAYMENT_AUTHORIZED", "PAYMENT_DECLINED")),
                replayRoute(RELEASE, inventoryGroup, RELEASE_REPLAY, List.of("ORDER_CANCELLATION_REQUESTED")))));
        replayCli(toolJar, auditDb, brokers, evidence, output, "init-audit");
        evidence.add(Map.of("stage", "three-cell-four-db-processes", "orderPid", order.pid(), "inventoryPid", inventory.pid(),
                "orderGroup", orderGroup, "inventoryGroup", inventoryGroup, "paymentGroup", paymentGroup, "brokers", brokers, "topics", names));
        assertThat(new java.util.HashSet<>(List.of(order.pid(), inventory.pid(), payment.pid(), ProcessHandle.current().pid()))).hasSize(4);
    }

    private void startPayment() throws Exception {
        Path log = output.resolve("m29-payment-" + UUID.randomUUID() + ".log");
        payment = launch(paymentJar, paymentDb, brokers, paymentGroup, 0, log, PAYMENT, null,
                "--spring.profiles.active=three-cell-saga,simulation,replay", "--apex.messaging.consumer.reliability.retry-delay=200ms");
        paymentReady(payment, log);
        assertThat(read(log)).contains("topics=[" + REQUEST + ", " + PAYMENT_REPLAY + "]");
        evidence.add(Map.of("stage", "payment-started", "pid", payment.pid(), "group", paymentGroup));
    }

    private void stopPayment() throws Exception {
        long pid = payment.pid(); stop(payment); payment = null;
        evidence.add(Map.of("stage", "payment-stopped", "pid", pid));
    }

    private UUID sku() {
        UUID id = UUID.randomUUID(); stock.update("INSERT INTO inventory_stock VALUES (?,10,0)", id); return id;
    }

    private UUID place(UUID sku, int quantity) throws Exception {
        return UUID.fromString(post(http, orderPort, List.of(item(sku, quantity))).path("orderId").asText());
    }

    private void policy(UUID id, String decision) {
        payments.update("INSERT INTO payment_simulation_policy(order_id,decision) VALUES (?,?)", id, decision);
    }

    private static void installFault(JdbcTemplate jdbc) {
        jdbc.execute("CREATE TABLE m29_fault_gate(event_type text PRIMARY KEY,enabled boolean NOT NULL)");
        jdbc.execute("CREATE SEQUENCE m29_fault_attempt");
        jdbc.execute("CREATE FUNCTION m29_fail_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                + "IF EXISTS(SELECT 1 FROM m29_fault_gate WHERE event_type=NEW.event_type AND enabled) THEN "
                + "PERFORM nextval('m29_fault_attempt'); RAISE EXCEPTION 'isolated M29 deferred commit fault'; END IF; RETURN NEW; END $$");
        jdbc.execute("CREATE CONSTRAINT TRIGGER m29_fail_commit AFTER INSERT ON processed_event DEFERRABLE INITIALLY DEFERRED "
                + "FOR EACH ROW EXECUTE FUNCTION m29_fail_commit()");
    }

    private static void setFault(JdbcTemplate jdbc, String type, boolean enabled) {
        jdbc.update("INSERT INTO m29_fault_gate VALUES (?,?) ON CONFLICT(event_type) DO UPDATE SET enabled=excluded.enabled", type, enabled);
    }

    private static long outboxCount(JdbcTemplate db, UUID id, String type) {
        return db.queryForObject("SELECT count(*) FROM outbox_event WHERE aggregate_id=? AND event_type=?", Long.class, id, type);
    }

    private static UUID eventId(Map<String, Object> row) { return (UUID) row.get("event_id"); }

    private ConsumerRecord<String, String> source(JdbcTemplate db, Map<String, Object> row) {
        var record = observe(sourceObserver, sourceCache, eventId(row));
        assertEnvelope(db, eventId(row), record);
        return record;
    }

    private ConsumerRecord<String, String> quarantined(ConsumerRecord<String, String> source, String group) throws Exception {
        UUID id = UUID.fromString(JSON.readTree(source.value()).path("metadata").path("eventId").asText());
        var dlt = observe(dltObserver, dltCache, id);
        assertQuarantine(dlt, source.value(), source.topic(), source.offset(), group, "PSQLException");
        awaitConsumed(admin, group, source.topic(), source.offset());
        long committed = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS)
                .get(new TopicPartition(source.topic(), source.partition())).offset();
        evidence.add(Map.of("stage", "source-quarantined", "eventId", id, "sourceTopic", source.topic(), "sourceOffset", source.offset(),
                "dltTopic", dlt.topic(), "dltOffset", dlt.offset(), "sourceCommittedOffset", committed, "group", group,
                "sourcePartition", source.partition(), "dltPartition", dlt.partition()));
        return dlt;
    }

    private JsonNode stalled() throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + orderPort + "/internal/operations/stalled-orders?limit=100"))
                .header("Authorization", "Bearer " + token).timeout(Duration.ofSeconds(5)).GET().build();
        var response = http.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200); return JSON.readTree(response.body());
    }

    private JsonNode awaitStalled(UUID id, String status) {
        JsonNode[] found = new JsonNode[1];
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            found[0] = null;
            for (var row : stalled().path("orders")) if (row.path("orderId").asText().equals(id.toString())) found[0] = row;
            assertThat(found[0]).isNotNull(); assertThat(found[0].path("status").asText()).isEqualTo(status);
            assertThat(found[0].path("assessment").asText()).isEqualTo("POTENTIALLY_STALLED");
            assertThat(found[0].path("ageMillis").asLong()).isGreaterThanOrEqualTo(200);
            assertThat(java.time.Instant.parse(found[0].path("statusEnteredAt").asText()))
                    .isEqualTo(orders.queryForObject("SELECT status_entered_at FROM orders WHERE id=?", OffsetDateTime.class, id).toInstant());
        });
        return found[0];
    }

    private void finalStatus(UUID id, String expected) throws Exception {
        awaitStatus(orders, id, expected); assertThat(getStatus(http, orderPort, id)).isEqualTo(expected);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(reports(id)).isFalse());
    }

    private boolean reports(UUID id) throws Exception {
        for (var row : stalled().path("orders")) {
            if (row.path("orderId").asText().equals(id.toString())) return true;
        }
        return false;
    }

    private JsonNode execute(JsonNode plan, String group, String replay) throws Exception {
        return replayCli(toolJar, auditDb, brokers, evidence, output, "execute", "--routes", routes.toString(),
                "--plan-id", plan.path("planId").asText(), "--confirm-digest", plan.path("digest").asText(),
                "--target-group", group, "--replay-topic", replay, "--actor", "m29-isolated-operator", "--reason", "controlled technical fault repaired",
                "--failure-resolved", "deferred commit fault disabled and owning database state inspected",
                "--max-records", "10", "--rate", "10", "--attempts", "1", "--allow-kafka-interaction");
    }

    private void recover(ConsumerRecord<String, String> source, ConsumerRecord<String, String> dlt, String group, String replay) throws Exception {
        long sourceEnd = topicEnd(admin, source.topic());
        try (var witness = observer(brokers)) {
            witness.subscribe(List.of(source.topic()));
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                witness.poll(Duration.ofMillis(100));
                assertThat(witness.assignment()).contains(new TopicPartition(source.topic(), 0));
                assertThat(witness.position(new TopicPartition(source.topic(), 0))).isGreaterThanOrEqualTo(sourceEnd);
            });
            var plan = replayApproval(toolJar, auditDb, brokers, routes, dlt, group, replay, evidence, output);
            var receipt = execute(plan, group, replay);
            assertThat(receipt.path("publication").asText()).isEqualTo("PUBLISHED_TO_REPLAY_TOPIC");
            UUID id = UUID.fromString(JSON.readTree(source.value()).path("metadata").path("eventId").asText());
            var replayed = observe(replayObserver, replayCache, id);
            assertReplayIdentity(source, replayed, plan);
            assertThat(replayed.topic()).isEqualTo(replay);
            long offset = receipt.path("deliveries").get(0).path("offset").asLong();
            assertThat(replayed.offset()).isEqualTo(offset);
            awaitConsumed(admin, group, replay, offset);
            long committed = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS)
                    .get(new TopicPartition(replay, replayed.partition())).offset();
            assertThat(topicEnd(admin, source.topic())).isEqualTo(sourceEnd);
            assertThat(witness.poll(Duration.ofMillis(300))).isEmpty();
            long replayEnd = topicEnd(admin, replay);
            execute(plan, group, replay);
            replayCli(toolJar, auditDb, brokers, evidence, output, "reconcile", "--routes", routes.toString(),
                    "--plan-id", plan.path("planId").asText(), "--confirm-digest", plan.path("digest").asText(),
                    "--target-group", group, "--replay-topic", replay, "--actor", "m29-isolated-operator", "--reason", "verify positive broker receipt",
                    "--from-offset", "0", "--max-scan", "100", "--allow-kafka-interaction");
            replayCli(toolJar, auditDb, brokers, evidence, output, "status", "--plan-id", plan.path("planId").asText());
            assertThat(topicEnd(admin, replay)).isEqualTo(replayEnd);
            evidence.add(Map.of("stage", "approved-targeted-recovery", "eventId", id, "plan", plan, "receipt", receipt,
                    "sourceEndUnchanged", sourceEnd, "unrelatedGroupReceivedNoReplay", true,
                    "rawSha256", hash(source.value()), "keySha256", hash(source.key()), "destinationCommittedOffset", committed,
                    "incomingMetadata", JSON.readTree(source.value()).path("metadata")));
        }
    }

    private void duplicates(ConsumerRecord<String, String> original, String group) throws Exception {
        testProducer.send(new ProducerRecord<>(original.topic(), 0, original.key(), original.value())).get(10, TimeUnit.SECONDS);
        ObjectNode copy = (ObjectNode) JSON.readTree(original.value());
        ((ObjectNode) copy.get("metadata")).put("eventId", UUID.randomUUID().toString());
        var sent = testProducer.send(new ProducerRecord<>(original.topic(), 0, original.key(), JSON.writeValueAsString(copy))).get(10, TimeUnit.SECONDS);
        awaitConsumed(admin, group, original.topic(), sent.offset());
    }

    private void negativePreflightAndUnknownVersion(ConsumerRecord<String, String> original, ConsumerRecord<String, String> validDlt) throws Exception {
        ObjectNode unknown = (ObjectNode) JSON.readTree(original.value());
        UUID event = UUID.randomUUID(), id = UUID.randomUUID();
        ((ObjectNode) unknown.get("metadata")).put("eventId", event.toString()).put("eventVersion", 99).put("aggregateId", id.toString());
        ((ObjectNode) unknown.get("payload")).put("orderId", id.toString());
        String raw = JSON.writeValueAsString(unknown);
        var sent = testProducer.send(new ProducerRecord<>(REQUEST, 0, "ORDER:" + id, raw)).get(10, TimeUnit.SECONDS);
        var dlt = observe(dltObserver, dltCache, event);
        assertQuarantine(dlt, raw, REQUEST, sent.offset(), paymentGroup, "UnsupportedEventContractException");
        awaitConsumed(admin, paymentGroup, REQUEST, sent.offset());
        Path selector = selector(dlt);
        refused("UNSUPPORTED_CONTRACT", routes, selector, paymentGroup, PAYMENT_REPLAY);
        assertThat(count(payments, "payment_authorizations", "order_id", id)).isZero();
        assertThat(count(payments, "processed_event", "event_id", event)).isZero();
        String unready = "m29.unprovisioned.replay." + UUID.randomUUID();
        admin.createTopics(List.of(new NewTopic(unready, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(admin.describeTopics(List.of(unready))
                .allTopicNames().get(5, TimeUnit.SECONDS).get(unready).partitions()).hasSize(1));
        assertThat(admin.listTopics().names().get(10, TimeUnit.SECONDS)).doesNotContain(unready + "-dlt");
        Path badRoutes = output.resolve("m29-missing-replay-dlt-routes.json");
        Files.writeString(badRoutes, JSON.writeValueAsString(List.of(replayRoute(REQUEST, paymentGroup, unready, List.of("ORDER_READY_FOR_PAYMENT")))));
        refused("TOPIC_PREFLIGHT_FAILED", badRoutes, selector(validDlt), paymentGroup, unready);
        assertThat(topicEnd(admin, unready)).isZero();
        long noAutomaticReplay = topicEnd(admin, PAYMENT_REPLAY);
        stopPayment(); startPayment();
        assertThat(topicEnd(admin, PAYMENT_REPLAY)).isEqualTo(noAutomaticReplay);
        assertThat(count(payments, "processed_event", "event_id", event)).isZero();
        evidence.add(Map.of("stage", "fail-closed-and-no-startup-replay", "unknownEventId", event,
                "sourceOffset", sent.offset(), "dltOffset", dlt.offset(), "missingReplayDlt", unready + "-dlt", "publishedPlans", 5));
    }

    private Path selector(ConsumerRecord<String, String> dlt) throws Exception {
        String id = JSON.readTree(dlt.value()).path("metadata").path("eventId").asText();
        Path file = output.resolve("m29-refused-selector-" + id + ".json");
        Files.writeString(file, JSON.writeValueAsString(List.of(Map.of("dlt", Map.of("topic", dlt.topic(), "partition", dlt.partition(),
                "offset", dlt.offset()), "eventId", id, "rawSha256", hash(dlt.value())))));
        return file;
    }

    private void refused(String code, Path routeFile, Path selector, String group, String replay) throws Exception {
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx128m", "-jar", toolJar.toString(),
                "plan", "--routes", routeFile.toString(), "--selector", selector.toString(), "--target-group", group, "--replay-topic", replay,
                "--actor", "m29-isolated-operator", "--reason", "negative validation", "--limit", "10", "--allow-kafka-interaction"));
        Path log = output.resolve("m29-refused-cli-" + UUID.randomUUID() + ".log");
        var builder = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile());
        builder.environment().keySet().removeIf(k -> k.matches("(?i)(REPLAY_.*|SPRING_.*|JAVA_TOOL_OPTIONS|JDK_JAVA_OPTIONS|_JAVA_OPTIONS)"));
        builder.environment().putAll(Map.of("REPLAY_KAFKA_BOOTSTRAP_SERVERS", brokers, "REPLAY_AUDIT_DB_URL", auditDb.getJdbcUrl("postgres", "postgres"),
                "REPLAY_AUDIT_DB_USERNAME", "postgres", "REPLAY_AUDIT_DB_PASSWORD", ""));
        var process = builder.start();
        try {
            assertThat(process.waitFor(30, TimeUnit.SECONDS)).isTrue();
            assertThat(process.exitValue()).as(read(log)).isEqualTo(2); assertThat(read(log)).contains("ERROR " + code);
            evidence.add(Map.of("stage", "cli-refused", "pid", process.pid(), "code", code, "exit", 2));
        } finally { stop(process); }
    }

    private static String hash(String raw) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(raw.getBytes(StandardCharsets.UTF_8)));
    }

    private void close() throws Exception {
        // Close each owned resource even if another cleanup action fails; never discover/stop external processes.
        Exception first = null;
        List<AutoCloseable> resources = new ArrayList<>();
        if (payment != null) resources.add(() -> stop(payment));
        if (inventory != null) resources.add(() -> stop(inventory));
        if (order != null) resources.add(() -> stop(order));
        if (testProducer != null) resources.add(testProducer);
        if (sourceObserver != null) resources.add(sourceObserver);
        if (dltObserver != null) resources.add(dltObserver);
        if (replayObserver != null) resources.add(replayObserver);
        if (admin != null) resources.add(admin);
        if (http != null) resources.add(http);
        if (broker != null) resources.add(broker::destroy);
        if (auditDb != null) resources.add(auditDb);
        if (paymentDb != null) resources.add(paymentDb);
        if (inventoryDb != null) resources.add(inventoryDb);
        if (orderDb != null) resources.add(orderDb);
        for (var resource : resources) {
            try { resource.close(); } catch (Exception failure) { if (first == null) first = failure; else first.addSuppressed(failure); }
        }
        if (first != null) throw first;
    }
}
