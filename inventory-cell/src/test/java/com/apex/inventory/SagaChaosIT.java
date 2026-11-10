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
import java.util.*;
import java.sql.Connection;
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

/** Isolated packaged-process COMMIT chaos; all fault machinery exists only in disposable databases. */
@Tag("integration")
class SagaChaosIT {
    private final long seed = Long.getLong("saga.chaos.seed", 20261109L);
    private final int requested = Integer.getInteger("saga.chaos.orders", 100);
    private final double rate = Double.parseDouble(System.getProperty("saga.chaos.failureRate", "0.10"));
    private final List<Sample> samples = new ArrayList<>();
    private final List<Fault> faults = new ArrayList<>();
    private final Map<JdbcTemplate, Connection> locks = new HashMap<>();
    private final Map<String, Object> result = new LinkedHashMap<>();
    private volatile boolean stopping;
    record Sample(UUID id, UUID sku, int quantity, boolean declined) { }
    record Fault(int key, JdbcTemplate db, UUID order, String type, String checkpoint) { }

    @Test
    void randomizedCommitFailuresAndApprovedPermanentRecovery() throws Exception {
        Path report = Path.of(System.getProperty("saga.inventoryJar")).toAbsolutePath().getParent()
                .resolve("saga-chaos-evidence.json");
        result.put("seed", seed); result.put("requestedOrders", requested); result.put("failureRate", rate);
        result.put("testResult", "FAIL"); result.put("failedAssertions", List.of("Execution incomplete"));
        try {
            assertThat(requested).isBetween(10, 1000); assertThat(rate).isGreaterThan(0).isLessThan(1);
            start();
            var random = new Random(seed);
            // Inventory is stopped, so all event/checkpoint selections precede consumption.
            for (int i = 0; i < requested; i++) {
                UUID sku = UUID.nameUUIDFromBytes((seed + ":sku:" + i).getBytes(StandardCharsets.UTF_8));
                stock.update("INSERT INTO inventory_stock VALUES (?,10,0)", sku);
                int quantity = 1 + random.nextInt(4);
                UUID id = place(sku, quantity);
                boolean decline = i % 4 == 0;
                samples.add(new Sample(id, sku, quantity, decline));
                select(random, stock, id, "ORDER_PLACED", "Inventory reservation COMMIT");
                if (!decline) select(random, payments, id, "ORDER_READY_FOR_PAYMENT", "Payment authorization COMMIT");
                select(random, orders, id, decline ? "PAYMENT_DECLINED" : "PAYMENT_AUTHORIZED", "Order payment-outcome COMMIT");
                if (decline) {
                    select(random, stock, id, "ORDER_CANCELLATION_REQUESTED", "Inventory release COMMIT");
                    select(random, orders, id, "INVENTORY_RELEASED", "Order release-acknowledgement COMMIT");
                }
            }
            result.put("actualGeneratedOrderCount", samples.size());
            result.put("generatedOrders", List.copyOf(samples));
            result.put("eligibleCheckpointEventCount", samples.stream().mapToInt(s -> s.declined() ? 4 : 3).sum());
            result.put("discoveryScope", "One packaged instance per Cell; isolated Eureka-disabled fixture. Eureka failover is not exercised.");
            result.put("declineFixture", "Trusted DECLINED decisions and Outbox outcomes seeded atomically in Payment; production authorization policy unchanged.");
            result.put("selection", faults.stream().map(f -> Map.of("key", f.key(), "orderId", f.order(),
                    "eventType", f.type(), "checkpoint", f.checkpoint())).toList());
            assertThat(faults.stream().map(Fault::checkpoint).distinct().count()).isEqualTo(5);
            startInventory();
            var watcher = java.util.concurrent.CompletableFuture.runAsync(() -> {
                try { observeRollbacksAndReleaseRetries(); } catch (Exception e) { throw new RuntimeException(e); }
            });
            // Seed trusted, durable business declines in the Payment owner, including its Outbox.
            // The current production policy always authorizes; no technical exception is mapped to decline.
            for (Sample s : samples) if (s.declined()) {
                awaitStatus(orders, s.id(), "PENDING_PAYMENT");
                seedDecline(s.id());
            }
            startPayment();
            watcher.get(6, TimeUnit.MINUTES);
            for (Sample s : samples) awaitStatus(orders, s.id(), s.declined() ? "CANCELLED" : "CONFIRMED");
            verifyInvariants();
            Sample happy = samples.stream().filter(s -> !s.declined()).findFirst().orElseThrow();
            Sample declined = samples.stream().filter(Sample::declined).findFirst().orElseThrow();
            duplicateExact(source(orders,typed(orders,happy.id(),"ORDER_PLACED")),inventoryGroup);
            duplicateExact(source(orders,typed(orders,declined.id(),"ORDER_CANCELLATION_REQUESTED")),inventoryGroup);
            verifyInvariants();
            assertNoDlt(admin, List.of(ORDER_TOPIC, INVENTORY_TOPIC, REQUEST, RELEASE, PAYMENT));
            result.put("transientSourceDltRecords", 0);
            result.put("injectedFailureCount", faults.size()); result.put("observedRollbackCount", faults.size());
            result.put("recoveredFailureCount", faults.size());
            permanentFailure();
            verifyInvariants();
            result.put("testResult", "PASS"); result.put("failedAssertions", List.of());
        } catch (Throwable failure) {
            result.put("failedAssertions", List.of(failure.toString()));
            throw failure;
        } finally {
            stopping = true;
            result.putIfAbsent("injectedFailureCount", evidence.stream().filter(e -> "commit-rollback-observed".equals(e.get("stage")))
                    .map(e -> e.get("event_id")).distinct().count());
            result.putIfAbsent("observedRollbackCount", evidence.stream().filter(e -> "commit-rollback-observed".equals(e.get("stage"))).count());
            result.putIfAbsent("recoveredFailureCount", 0);
            result.put("events", evidence);
            result.put("actualGeneratedOrderCount", Math.min(requested, samples.size()));
            result.put("totalOrdersIncludingPermanentCase", samples.size());
            try { close(); } catch (Exception cleanup) {
                result.put("testResult", "FAIL"); result.put("cleanupFailure", cleanup.toString());
                throw cleanup;
            } finally {
                Files.createDirectories(report.getParent());
                Files.writeString(report, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(result));
            }
        }
    }

    private static EmbeddedPostgres disposablePostgres() throws Exception {
        return EmbeddedPostgres.builder().setServerConfig("listen_addresses", "127.0.0.1")
                .setServerConfig("fsync", "on").setServerConfig("synchronous_commit", "on")
                .setServerConfig("full_page_writes", "on").start();
    }

    private void startInventory() throws Exception {
        Path log = output.resolve("chaos-inventory-running.log");
        inventory = launch(inventoryJar, inventoryDb, brokers, inventoryGroup, inventoryPort, log, INVENTORY_TOPIC,
                ORDER_TOPIC + "," + RELEASE + "," + INVENTORY_REPLAY + "," + RELEASE_REPLAY,
                "--apex.messaging.consumer.reliability.retry-delay=200ms",
                "--apex.messaging.consumer.reliability.max-attempts=3", "--eureka.client.enabled=false");
        ready(inventory, log, http, inventoryPort);
        evidence.add(Map.of("stage", "inventory-started", "pid", inventory.pid(), "group", inventoryGroup));
    }

    private void installFault(JdbcTemplate db, EmbeddedPostgres pg, String business, String id) throws Exception {
        db.execute("CREATE EXTENSION dblink");
        db.execute("CREATE TABLE chaos_gate(k int PRIMARY KEY,order_id uuid,event_type text,permanent boolean DEFAULT false,enabled boolean DEFAULT true)");
        db.execute("CREATE TABLE chaos_attempt(k int,event_id uuid,txid bigint,backend_pid int,application_name text,inbox_present boolean,outbox_count bigint,topic text,offset_value bigint,consumer_group text)");
        // Capture the business key in transaction-local state, without altering the application rows.
        db.execute("CREATE FUNCTION chaos_context() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                + "PERFORM set_config('chaos.order_id',NEW." + id + "::text,true); RETURN NEW; END $$");
        db.execute("CREATE TRIGGER chaos_context AFTER INSERT OR UPDATE ON " + business
                + " FOR EACH ROW EXECUTE FUNCTION chaos_context()");
        String connection = "host=127.0.0.1 port=" + pg.getPort() + " dbname=postgres user=postgres";
        db.execute("""
                CREATE FUNCTION chaos_commit() RETURNS trigger LANGUAGE plpgsql AS $$
                DECLARE g chaos_gate; n bigint; effects bigint;
                BEGIN
                  SELECT * INTO g FROM chaos_gate WHERE enabled AND event_type=NEW.event_type
                    AND order_id=NULLIF(current_setting('chaos.order_id',true),'')::uuid;
                  IF NOT FOUND THEN RETURN NEW; END IF;
                  n := nextval(('chaos_seq_' || g.k)::regclass);
                  IF n=1 OR g.permanent THEN
                    SELECT count(*) INTO effects FROM outbox_event WHERE causation_id=NEW.event_id;
                    PERFORM dblink_exec('%s',format(
                      'INSERT INTO chaos_attempt VALUES (%%s,%%L,%%s,%%s,%%L,true,%%s,%%L,%%s,%%L)',
                      g.k,NEW.event_id,txid_current(),pg_backend_pid(),current_setting('application_name'),effects,
                      NEW.topic,NEW.offset_value,NEW.consumer_group));
                    RAISE EXCEPTION 'APEX_CHAOS_COMMIT key=%% event=%%',g.k,NEW.event_id USING ERRCODE='23514';
                  END IF;
                  -- A separate test connection holds this lock until the aborted transaction is inspected.
                  PERFORM pg_advisory_xact_lock(20261109,g.k);
                  RETURN NEW;
                END $$
                """.formatted(connection));
        db.execute("CREATE CONSTRAINT TRIGGER chaos_commit AFTER INSERT ON processed_event "
                + "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION chaos_commit()");
        locks.put(db, db.getDataSource().getConnection());
    }

    private void select(Random random, JdbcTemplate db, UUID id, String type, String checkpoint) throws Exception {
        if (random.nextDouble() >= rate) return;
        arm(db, id, type, checkpoint, false);
    }

    private Fault arm(JdbcTemplate db, UUID id, String type, String checkpoint, boolean permanent) throws Exception {
        int key = faults.size() + 1;
        Fault f = new Fault(key, db, id, type, checkpoint);
        db.execute("CREATE SEQUENCE chaos_seq_" + key + " CACHE 1");
        db.update("INSERT INTO chaos_gate(k,order_id,event_type,permanent) VALUES (?,?,?,?)", key, id, type, permanent);
        try (var st = locks.get(db).prepareStatement("SELECT pg_advisory_lock(20261109,?)")) {
            st.setInt(1, key); st.execute();
        }
        faults.add(f); return f;
    }

    private void observeRollbacksAndReleaseRetries() throws Exception {
        var remaining = new ArrayList<>(faults);
        long deadline = System.nanoTime() + Duration.ofMinutes(5).toNanos();
        while (!remaining.isEmpty()) {
            if (stopping) return;
            assertThat(System.nanoTime()).as("All selected checkpoints reached: %s", remaining).isLessThan(deadline);
            for (Fault f : new ArrayList<>(remaining)) {
                var attempts = f.db().queryForList("SELECT * FROM chaos_attempt WHERE k=?", f.key());
                if (attempts.isEmpty()) continue;
                assertThat(attempts).hasSize(1);
                verifyRollback(f, attempts.getFirst());
                try (var st = locks.get(f.db()).prepareStatement("SELECT pg_advisory_unlock(20261109,?)")) {
                    st.setInt(1, f.key()); st.execute();
                }
                remaining.remove(f);
            }
            Thread.sleep(25);
        }
    }

    private void verifyRollback(Fault f, Map<String,Object> attempt) {
        long xid = ((Number) attempt.get("txid")).longValue();
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(f.db().queryForObject(
                "SELECT pg_xact_status(?::text::xid8)", String.class, xid)).isEqualTo("aborted"));
        UUID event = (UUID) attempt.get("event_id");
        assertThat(attempt.get("inbox_present")).isEqualTo(true);
        long expectedEffects = Set.of("ORDER_PLACED", "ORDER_READY_FOR_PAYMENT", "PAYMENT_DECLINED",
                "ORDER_CANCELLATION_REQUESTED").contains(f.type()) ? 1 : 0;
        assertThat(((Number)attempt.get("outbox_count")).longValue()).isEqualTo(expectedEffects);
        assertThat(count(f.db(), "processed_event", "event_id", event)).isZero();
        assertThat(count(f.db(), "outbox_event", "causation_id", event)).isZero();
        Sample s = samples.stream().filter(x -> x.id().equals(f.order())).findFirst().orElseThrow();
        switch (f.type()) {
            case "ORDER_PLACED" -> { assertThat(count(stock,"inventory_reservations","order_id",s.id())).isZero(); assertStock(stock,s.sku(),10,0); }
            case "ORDER_READY_FOR_PAYMENT" -> { assertThat(count(payments,"payment_authorizations","order_id",s.id())).isZero(); assertThat(status(orders,s.id())).isEqualTo("PENDING_PAYMENT"); }
            case "PAYMENT_AUTHORIZED", "PAYMENT_DECLINED" -> { assertThat(status(orders,s.id())).isEqualTo("PENDING_PAYMENT"); assertThat(orders.queryForObject("SELECT payment_decision FROM orders WHERE id=?",String.class,s.id())).isNull(); }
            case "ORDER_CANCELLATION_REQUESTED" -> { assertStock(stock,s.sku(),10-s.quantity(),s.quantity()); assertThat(status(orders,s.id())).isEqualTo("CANCELLING"); }
            case "INVENTORY_RELEASED" -> { assertThat(status(orders,s.id())).isEqualTo("CANCELLING"); assertThat(orders.queryForObject("SELECT release_acknowledged FROM orders WHERE id=?",Boolean.class,s.id())).isFalse(); }
            default -> throw new AssertionError(f.type());
        }
        var row = new LinkedHashMap<String,Object>(attempt);
        row.put("stage","commit-rollback-observed"); row.put("checkpoint",f.checkpoint()); row.put("orderId",s.id());
        row.put("transactionStatus","aborted"); row.put("committedInboxAbsent",true); row.put("committedCausalOutboxAbsent",true);
        row.put("businessRollbackVerified",true);
        if (!f.checkpoint().startsWith("Permanent")) {
            try {
                var offsets = admin.listConsumerGroupOffsets(attempt.get("consumer_group").toString())
                        .partitionsToOffsetAndMetadata().get(10,TimeUnit.SECONDS);
                var offset = offsets.get(new TopicPartition(attempt.get("topic").toString(),0));
                assertThat(offset == null ? -1 : offset.offset()).as("No ACK before business COMMIT")
                        .isLessThanOrEqualTo(((Number)attempt.get("offset_value")).longValue());
                row.put("noAckBeforeCommit",true);
            } catch (Exception e) { throw new AssertionError("Cannot inspect source ACK",e); }
        }
        row.put("instancePid", f.db()==orders ? order.pid() : f.db()==stock ? inventory.pid() : payment.pid());
        evidence.add(row);
    }

    private void seedDecline(UUID id) throws Exception {
        var request = typed(orders,id,"ORDER_READY_FOR_PAYMENT");
        try (var c = paymentDb.getPostgresDatabase().getConnection()) {
            c.setAutoCommit(false);
            try (var decision = c.prepareStatement("INSERT INTO payment_authorizations(order_id,status) VALUES (?,'DECLINED')");
                 var out = c.prepareStatement("INSERT INTO outbox_event(event_id,event_type,event_version,correlation_id,causation_id,aggregate_id,aggregate_type,source_service,cell_id,occurred_at,payload) VALUES (?,'PAYMENT_DECLINED',1,?,?,?,'PAYMENT_AUTHORIZATION','payment-cell','payment-cell',now(),?::jsonb)")) {
                decision.setObject(1,id); decision.executeUpdate();
                out.setObject(1,UUID.randomUUID()); out.setObject(2,request.get("correlation_id"));
                out.setObject(3,request.get("event_id")); out.setObject(4,id);
                out.setString(5,JSON.writeValueAsString(Map.of("orderId",id))); out.executeUpdate();
                c.commit();
            }
        }
        evidence.add(Map.of("stage","seeded-trusted-payment-decline","orderId",id,
                "mechanism","Test-only owning Payment decision + Outbox transaction, before Payment consumer startup"));
    }

    private void verifyInvariants() throws Exception {
        assertThat(orders.queryForObject("SELECT count(*) FROM orders",Long.class)).isEqualTo(samples.size());
        int confirmed=0,cancelled=0;
        for (Sample s : samples) {
            String expected=s.declined()?"CANCELLED":"CONFIRMED";
            assertThat(status(orders,s.id())).isEqualTo(expected);
            assertThat(payments.queryForObject("SELECT status FROM payment_authorizations WHERE order_id=?",String.class,s.id()))
                    .isEqualTo(s.declined()?"DECLINED":"AUTHORIZED");
            assertThat(count(payments,"payment_authorizations","order_id",s.id())).isEqualTo(1);
            assertThat(stock.queryForObject("SELECT status FROM inventory_reservations WHERE order_id=?",String.class,s.id()))
                    .isEqualTo(s.declined()?"RELEASED":"RESERVED");
            assertThat(count(stock,"inventory_reservations","order_id",s.id())).isEqualTo(1);
            assertStock(stock,s.sku(),s.declined()?10:10-s.quantity(),s.declined()?0:s.quantity());
            if(s.declined()) {
                cancelled++;
                assertThat(orders.queryForObject("SELECT release_acknowledged FROM orders WHERE id=?",Boolean.class,s.id())).isTrue();
                assertThat(outboxCount(stock,s.id(),"INVENTORY_RELEASED")).isEqualTo(1);
                assertThat(outboxCount(orders,s.id(),"ORDER_CANCELLATION_REQUESTED")).isEqualTo(1);
            } else {
                confirmed++;
                assertThat(outboxCount(orders,s.id(),"ORDER_CANCELLATION_REQUESTED")).isZero();
                assertThat(outboxCount(payments,s.id(),"PAYMENT_DECLINED")).isZero();
            }
            assertThat(outboxCount(stock,s.id(),"INVENTORY_RESERVED")).isEqualTo(1);
            assertThat(outboxCount(payments,s.id(),s.declined()?"PAYMENT_DECLINED":"PAYMENT_AUTHORIZED")).isEqualTo(1);
            received(stock,orders,s.id(),"ORDER_PLACED");
            received(orders,stock,s.id(),"INVENTORY_RESERVED");
            received(payments,orders,s.id(),"ORDER_READY_FOR_PAYMENT");
            received(orders,payments,s.id(),s.declined()?"PAYMENT_DECLINED":"PAYMENT_AUTHORIZED");
            if(s.declined()) {
                received(stock,orders,s.id(),"ORDER_CANCELLATION_REQUESTED");
                received(orders,stock,s.id(),"INVENTORY_RELEASED");
            }
        }
        for(var db:List.of(orders,stock,payments)) {
            assertThat(db.queryForObject("SELECT count(*) FROM (SELECT aggregate_id,event_type FROM outbox_event GROUP BY aggregate_id,event_type HAVING count(*)>1) x",Long.class)).isZero();
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(db.queryForObject(
                    "SELECT count(*) FROM outbox_event WHERE NOT published OR dead_letter",Long.class)).isZero());
        }
        for(Fault f:faults) {
            var attempts=f.db().queryForList("SELECT * FROM chaos_attempt WHERE k=?",f.key());
            assertThat(attempts).hasSize(1);
            assertThat(count(f.db(),"processed_event","event_id",(UUID)attempts.getFirst().get("event_id"))).isEqualTo(1);
            assertThat(f.db().queryForObject("SELECT last_value FROM chaos_seq_"+f.key(),Long.class)).isEqualTo(2);
            for(var row:evidence) if ("commit-rollback-observed".equals(row.get("stage"))
                    && attempts.getFirst().get("event_id").equals(row.get("event_id"))) {
                row.put("recovered",true); row.put("committedInboxCount",1);
            }
        }
        // Every emitted application outcome is causally linked to a durable owning Inbox.
        for(var db:List.of(orders,stock,payments)) assertThat(db.queryForObject(
                "SELECT count(*) FROM outbox_event o WHERE event_type <> 'ORDER_PLACED' AND NOT "
                + "(event_type='PAYMENT_DECLINED') AND NOT EXISTS (SELECT 1 FROM processed_event p WHERE p.event_id=o.causation_id)",Long.class)).isZero();
        result.put("outcomes",Map.of("confirmed",confirmed,"cancelled",cancelled,"pending",0));
        result.put("invariants",Map.of("stockConserved",true,"negativeStock",false,"overRelease",false,
                "duplicatePaymentDecisions",0,"duplicateStockMovements",0,"missingOrders",0,"unpublishedOrDeadOutbox",0,
                "inboxBusinessOutboxAtomicity",true));
        result.put("stockConservation", stock.queryForMap("SELECT sum(available_quantity) AS available,sum(reserved_quantity) AS reserved,sum(available_quantity+reserved_quantity) AS total FROM inventory_stock"));
        result.put("expectedTotalStockUnits",10*samples.size());
        assertThat(stock.queryForObject("SELECT sum(available_quantity+reserved_quantity) FROM inventory_stock",Long.class)).isEqualTo(10L*samples.size());
    }

    private void received(JdbcTemplate destination,JdbcTemplate source,UUID order,String type) {
        UUID event=(UUID)typed(source,order,type).get("event_id");
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(count(destination,"processed_event","event_id",event)).as("No lost %s for %s",type,order).isEqualTo(1));
    }

    private void permanentFailure() throws Exception {
        stopPayment();
        UUID sku=sku(); UUID id=place(sku,2);
        samples.add(new Sample(id,sku,2,false));
        awaitStatus(orders,id,"PENDING_PAYMENT");
        Fault fault=arm(payments,id,"ORDER_READY_FOR_PAYMENT","Permanent Payment authorization COMMIT",true);
        // Permanent faults do not use the successful-retry barrier.
        try(var st=locks.get(payments).prepareStatement("SELECT pg_advisory_unlock(20261109,?)")) { st.setInt(1,fault.key()); st.execute(); }
        startPayment();
        var request=typed(orders,id,"ORDER_READY_FOR_PAYMENT");
        var original=source(orders,request); var dlt=quarantined(original,paymentGroup);
        var attempts=payments.queryForList("SELECT * FROM chaos_attempt WHERE k=?",fault.key());
        assertThat(attempts).hasSize(3);
        for(var a:attempts) verifyRollback(fault,a);
        assertThat(status(orders,id)).isEqualTo("PENDING_PAYMENT");
        assertThat(outboxCount(orders,id,"ORDER_CANCELLATION_REQUESTED")).isZero();
        long replayBefore=topicEnd(admin,PAYMENT_REPLAY);
        payments.update("UPDATE chaos_gate SET enabled=false WHERE k=?",fault.key());
        // Repair alone must neither replay the DLT nor advance business state.
        assertThat(count(payments,"payment_authorizations","order_id",id)).isZero();
        assertThat(topicEnd(admin,PAYMENT_REPLAY)).isEqualTo(replayBefore);
        recover(original,dlt,paymentGroup,PAYMENT_REPLAY);
        awaitStatus(orders,id,"CONFIRMED"); duplicates(original,paymentGroup);
        assertThat(audit.queryForObject("SELECT count(*) FROM replay_delivery WHERE state='PUBLISHED'",Long.class)).isEqualTo(1);
        assertThat(topicEnd(admin,REQUEST+"-dlt")).isEqualTo(1);
        result.put("permanentFailure",Map.of("orderId",id,"commitFailures",attempts.size(),"sourceDltVerified",true,
                "stalledStatus","PENDING_PAYMENT","approvedReplayPublished",1,"finalStatus","CONFIRMED",
                "repeatedExecuteDidNotRepublish",true,"duplicateDeliveryVerified",true));
        faults.remove(fault);
    }
    static final String INVENTORY_REPLAY = "apex.inventory.order-placed.replay.v1";
    static final String ORDER_INVENTORY_REPLAY = "apex.order.inventory-outcome.replay.v1";
    static final String PAYMENT_REPLAY = "apex.payment.authorization-request.replay.v1";
    static final String ORDER_PAYMENT_REPLAY = "apex.order.payment-outcome.replay.v1";
    static final String RELEASE_REPLAY = "apex.inventory.release-request.replay.v1";
    private final List<Map<String, Object>> evidence = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final Map<UUID, ConsumerRecord<String, String>> sourceCache = new HashMap<>();
    private final Map<UUID, ConsumerRecord<String, String>> dltCache = new HashMap<>();
    private final Map<UUID, ConsumerRecord<String, String>> replayCache = new HashMap<>();
    private final String orderGroup = "order-chaos-" + UUID.randomUUID();
    private final String inventoryGroup = "inventory-chaos-" + UUID.randomUUID();
    private final String paymentGroup = "payment-chaos-" + UUID.randomUUID();
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
        orderDb = disposablePostgres(); inventoryDb = disposablePostgres();
        paymentDb = disposablePostgres(); auditDb = disposablePostgres();
        var names = new ArrayList<String>();
        for (String topic : List.of(ORDER_TOPIC, INVENTORY_TOPIC, REQUEST, RELEASE, PAYMENT, "apex.order.legacy.events.v1",
                INVENTORY_REPLAY, ORDER_INVENTORY_REPLAY, PAYMENT_REPLAY, ORDER_PAYMENT_REPLAY, RELEASE_REPLAY)) {
            names.add(topic); names.add(topic + "-dlt");
        }
        broker = new EmbeddedKafkaKraftBroker(1, 1, names.toArray(String[]::new));
        broker.brokerProperty("auto.create.topics.enable", "false"); broker.brokerListProperty("apex.chaos.test.brokers");
        broker.afterPropertiesSet(); brokers = broker.getBrokersAsString();
        http = HttpClient.newHttpClient(); orderPort = freePort(); inventoryPort = freePort();
        order = launch(orderJar, orderDb, brokers, orderGroup, orderPort, output.resolve("chaos-order.log"), ORDER_TOPIC, INVENTORY_TOPIC + "," + PAYMENT + "," + ORDER_INVENTORY_REPLAY + "," + ORDER_PAYMENT_REPLAY,
                "--spring.profiles.active=saga,three-cell-saga,replay,operations", "--apex.order.operations.read-token=" + token,
                "--apex.order.operations.age-threshold=200ms", "--apex.messaging.consumer.reliability.retry-delay=200ms", "--apex.messaging.consumer.reliability.max-attempts=3", "--eureka.client.enabled=false");
        inventory = launch(inventoryJar, inventoryDb, brokers, inventoryGroup, inventoryPort, output.resolve("chaos-inventory.log"), INVENTORY_TOPIC, ORDER_TOPIC + "," + RELEASE + "," + INVENTORY_REPLAY + "," + RELEASE_REPLAY,
                "--spring.profiles.active=saga,three-cell-saga,replay", "--apex.messaging.consumer.reliability.retry-delay=200ms", "--apex.messaging.consumer.reliability.max-attempts=3", "--eureka.client.enabled=false");
        ready(order, output.resolve("chaos-order.log"), http, orderPort);
        ready(inventory, output.resolve("chaos-inventory.log"), http, inventoryPort);
        startPayment();
        assertThat(read(output.resolve("chaos-order.log"))).contains("topics=[" + INVENTORY_TOPIC + ", " + PAYMENT + ", " + ORDER_INVENTORY_REPLAY + ", " + ORDER_PAYMENT_REPLAY + "]");
        assertThat(read(output.resolve("chaos-inventory.log"))).contains("topics=[" + ORDER_TOPIC + ", " + RELEASE + ", " + INVENTORY_REPLAY + ", " + RELEASE_REPLAY + "]");
        orders = new JdbcTemplate(orderDb.getPostgresDatabase()); stock = new JdbcTemplate(inventoryDb.getPostgresDatabase());
        payments = new JdbcTemplate(paymentDb.getPostgresDatabase()); audit = new JdbcTemplate(auditDb.getPostgresDatabase());
        for(var db:List.of(orders,stock,payments)) for(String setting:List.of("fsync","synchronous_commit","full_page_writes"))
            assertThat(db.queryForObject("SHOW " + setting,String.class)).as(setting).isEqualTo("on");
        result.put("databasePorts",List.of(orderDb.getPort(),inventoryDb.getPort(),paymentDb.getPort(),auditDb.getPort()));
        installFault(orders, orderDb, "orders", "id");
        installFault(stock, inventoryDb, "inventory_reservations", "order_id");
        installFault(payments, paymentDb, "payment_authorizations", "order_id");
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
        routes = output.resolve("chaos-replay-routes.json");
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
        stopPayment();
        stop(inventory); inventory = null;
    }

    private void startPayment() throws Exception {
        Path log = output.resolve("chaos-payment-" + UUID.randomUUID() + ".log");
        payment = launch(paymentJar, paymentDb, brokers, paymentGroup, 0, log, PAYMENT, REQUEST + "," + PAYMENT_REPLAY,
                "--spring.profiles.active=three-cell-saga,simulation,replay", "--apex.messaging.consumer.reliability.retry-delay=200ms", "--apex.messaging.consumer.reliability.max-attempts=3", "--eureka.client.enabled=false");
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

    private JsonNode execute(JsonNode plan, String group, String replay) throws Exception {
        return replayCli(toolJar, auditDb, brokers, evidence, output, "execute", "--routes", routes.toString(),
                "--plan-id", plan.path("planId").asText(), "--confirm-digest", plan.path("digest").asText(),
                "--target-group", group, "--replay-topic", replay, "--actor", "chaos-isolated-operator", "--reason", "controlled technical fault repaired",
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
                    "--target-group", group, "--replay-topic", replay, "--actor", "chaos-isolated-operator", "--reason", "verify positive broker receipt",
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

    private void duplicateExact(ConsumerRecord<String,String> original,String group) throws Exception {
        var sent=testProducer.send(new ProducerRecord<>(original.topic(),0,original.key(),original.value())).get(10,TimeUnit.SECONDS);
        awaitConsumed(admin,group,original.topic(),sent.offset());
    }

    private static String hash(String raw) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(raw.getBytes(StandardCharsets.UTF_8)));
    }

    private void close() throws Exception {
        // Close each owned resource even if another cleanup action fails; never discover/stop external processes.
        Exception first = null;
        List<AutoCloseable> resources = new ArrayList<>();
        for (Connection c : locks.values()) resources.add(c);
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

    private static long outboxCount(JdbcTemplate db, UUID id, String type) {
        return db.queryForObject("SELECT count(*) FROM outbox_event WHERE aggregate_id=? AND event_type=?", Long.class, id, type);
    }

    private static UUID eventId(Map<String, Object> row) { return (UUID) row.get("event_id"); }
}
