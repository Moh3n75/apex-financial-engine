package com.apex.inventory;

import static com.apex.inventory.OrderInventorySagaIT.*;
import static com.apex.inventory.ThreeCellSagaIT.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import tools.jackson.databind.node.ObjectNode;

/** Bounded measurement fixture; no runtime application dependency or shared resources. */
@Tag("integration")
class SagaPerformanceIT {
    private static final List<String> STREAMS = List.of(ORDER_TOPIC, INVENTORY_TOPIC, REQUEST, RELEASE, PAYMENT,
            "apex.order.legacy.events.v1");
    private final List<Map<String, Object>> results = new ArrayList<>();
    private final List<Map<String, Object>> processes = new ArrayList<>();
    private final List<Sample> all = new CopyOnWriteArrayList<>();
    private final String og = "order-m30-" + UUID.randomUUID(), ig = "inventory-m30-" + UUID.randomUUID(),
            pg = "payment-m30-" + UUID.randomUUID();
    private Process order, inventory, payment;
    private Path root, output, orderJar, inventoryJar, paymentJar;
    private EmbeddedPostgres orderDb, inventoryDb, paymentDb;
    private JdbcTemplate orders, stock, payments;
    private String brokers;
    private int port, inventoryPort;
    private Admin admin;
    private HttpClient http;

    record Sample(UUID id, long acceptedAt, double httpMs, boolean declined, boolean outOfStock) { }

    @Test
    void measuresPackagedThreeCellLoadAndBoundedRestartRecovery() throws Exception {
        root = Path.of(System.getProperty("saga.repositoryRoot")).toAbsolutePath().normalize();
        inventoryJar = Path.of(System.getProperty("saga.inventoryJar")).toAbsolutePath().normalize();
        orderJar = verifyBuild(root, inventoryJar); paymentJar = root.resolve("payment-cell/target/payment-cell.jar");
        var receipt = new Properties();
        try (var in = Files.newInputStream(root.resolve("out/m27-payment-build.properties"))) { receipt.load(in); }
        assertThat(sha(paymentJar)).isEqualTo(receipt.getProperty("paymentJarSha256"));
        output = inventoryJar.getParent();
        var topics = new ArrayList<String>();
        STREAMS.forEach(t -> { topics.add(t); topics.add(t + "-dlt"); });
        var broker = new EmbeddedKafkaKraftBroker(1, 1, topics.toArray(String[]::new));
        broker.brokerProperty("auto.create.topics.enable", "false"); broker.brokerListProperty("apex.m30.test.brokers");
        try (var odb = durablePostgres(); var idb = durablePostgres();
             var pdb = durablePostgres(); var client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2)).build()) {
            orderDb = odb; inventoryDb = idb; paymentDb = pdb; http = client;
            try {
                broker.afterPropertiesSet(); brokers = broker.getBrokersAsString();
                admin = Admin.create(Map.of("bootstrap.servers", brokers));
                port = freePort(); inventoryPort = freePort();
                order = launch(orderJar, odb, brokers, og, port, output.resolve("m30-order.log"), ORDER_TOPIC, null,
                        "--spring.profiles.active=saga,three-cell-saga");
                startInventory(); startPayment(); ready(order, output.resolve("m30-order.log"), http, port);
                processes.add(Map.of("cell", "order", "pid", order.pid()));
                orders = new JdbcTemplate(odb.getPostgresDatabase()); stock = new JdbcTemplate(idb.getPostgresDatabase());
                payments = new JdbcTemplate(pdb.getPostgresDatabase());
                for (var db : List.of(orders, stock, payments)) {
                    for (String setting : List.of("fsync", "synchronous_commit", "full_page_writes")) {
                        assertThat(db.queryForObject("SHOW " + setting, String.class)).as(setting).isEqualTo("on");
                    }
                }
                assertThat(new HashSet<>(List.of(order.pid(), inventory.pid(), payment.pid(), ProcessHandle.current().pid()))).hasSize(4);
                assertThat(new HashSet<>(List.of(odb.getPort(), idb.getPort(), pdb.getPort()))).hasSize(3);
                assertThat(orders.queryForList("SELECT tablename FROM pg_tables WHERE schemaname='public'", String.class)).doesNotContain("inventory_stock", "payment_authorizations");
                assertThat(stock.queryForList("SELECT tablename FROM pg_tables WHERE schemaname='public'", String.class)).doesNotContain("orders", "payment_authorizations");
                assertThat(payments.queryForList("SELECT tablename FROM pg_tables WHERE schemaname='public'", String.class)).doesNotContain("orders", "inventory_stock");
                phase("warmup", 2, 2, 2, 4, null);
                phase("smoke", 4, 5, 2, 8, null);
                // Rates are bounded relative to observed smoke acceptance, never a capacity promise.
                double smokeRps = ((Number) results.getLast().get("acceptedRps")).doubleValue();
                int moderate = Math.min(12, Math.max(2, (int) Math.ceil(smokeRps * 2)));
                int higher = Math.min(30, moderate * 3);
                phase("moderate", 30, moderate, 4, 16, null);
                phase("higher-hotspot", 30, higher, 8, 1, null);
                phase("spike", 4, Math.min(60, higher * 2), 12, 1, null);
                phase("post-spike-recovery", 6, moderate, 4, 8, null);
                stop(payment); payment = null;
                phase("payment-stop-backlog", 3, 5, 3, 1, this::startPayment);
                stop(inventory); inventory = null;
                phase("inventory-consumer-stop-backlog", 3, 5, 3, 1, this::startInventory);
                duplicateEffects(); invariants(); metadataAtomicity();
                assertThat(snapshot().get("dltRecords")).isEqualTo(0L);
                assertThat(all).isNotEmpty();
                var evidence = new LinkedHashMap<String, Object>();
                evidence.put("scenarios", results); evidence.put("processes", processes);
                evidence.put("groups", Map.of("order", og, "inventory", ig, "payment", pg));
                evidence.put("databaseIsolation", "Three distinct native servers/ports, owner-only business tables verified");
                evidence.put("host", Map.of("processors", Runtime.getRuntime().availableProcessors(),
                        "os", System.getProperty("os.name"), "java", System.getProperty("java.version"),
                        "testMaxHeapBytes", Runtime.getRuntime().maxMemory()));
                evidence.put("configuration", Map.of("cellJvm", "-Xms64m -Xmx256m",
                        "topics", topics, "partitionsPerTopic", 1, "consumerConcurrency", 1,
                        "publisherIntervalMs", 100, "initialUnitsPerSku", 10000,
                        "payment", "Explicit seeded AUTHORIZED/DECLINED simulation, never real money",
                        "maxPhaseRequests", 1000, "databaseSettings", stock.queryForList(
                                "SELECT name,setting FROM pg_settings WHERE name IN ('shared_buffers','max_connections','fsync','synchronous_commit','full_page_writes')")));
                evidence.put("jvmDiagnostics", jvmDiagnostics());
                evidence.put("metricsUnavailable", List.of("Hikari pool utilization/wait: no exposed pool instrumentation",
                        "Production TLS/ACL/multi-broker behavior", "Broker process outage (consumer JVM outage tested)"));
                evidence.put("final", snapshot());
                evidence.put("invariants", "PASS: owner SQL, zero DLT/dead, source-to-Inbox commits, duplicate effects, stock conservation");
                Files.writeString(output.resolve("m30-performance-evidence.json"), JSON.writerWithDefaultPrettyPrinter().writeValueAsString(evidence));
            } finally {
                try { stop(payment); } finally { try { stop(inventory); } finally { stop(order); } }
                if (admin != null) admin.close(); broker.destroy();
            }
        }
    }

    @FunctionalInterface interface Restart { void run() throws Exception; }

    private static EmbeddedPostgres durablePostgres() throws Exception {
        return EmbeddedPostgres.builder().setServerConfig("fsync", "on")
                .setServerConfig("synchronous_commit", "on").setServerConfig("full_page_writes", "on").start();
    }

    private void startPayment() throws Exception {
        Path log = output.resolve("m30-payment-" + UUID.randomUUID() + ".log");
        payment = launch(paymentJar, paymentDb, brokers, pg, 0, log, PAYMENT, null,
                "--spring.profiles.active=three-cell-saga,simulation",
                "--apex.messaging.consumer.reliability.max-attempts=30", "--apex.messaging.consumer.reliability.retry-delay=200ms");
        paymentReady(payment, log); processes.add(Map.of("cell", "payment", "pid", payment.pid()));
    }

    private void startInventory() throws Exception {
        Path log = output.resolve("m30-inventory-" + UUID.randomUUID() + ".log");
        inventory = launch(inventoryJar, inventoryDb, brokers, ig, inventoryPort, log, INVENTORY_TOPIC, null,
                "--spring.profiles.active=saga,three-cell-saga");
        ready(inventory, log, http, inventoryPort); processes.add(Map.of("cell", "inventory", "pid", inventory.pid()));
    }

    private void phase(String name, int defaultSeconds, int defaultRps, int defaultVus, int defaultSkus, Restart restart) throws Exception {
        int seconds = bounded("m30." + name + ".seconds", defaultSeconds, 1, 30);
        int rps = bounded("m30." + name + ".rps", defaultRps, 1, 100);
        int vus = bounded("m30." + name + ".concurrency", defaultVus, 1, 16);
        int skus = bounded("m30." + name + ".skus", defaultSkus, 1, 64);
        assertThat(seconds * rps).as("Hard per-phase request cap").isLessThanOrEqualTo(1000);
        var products = new ArrayList<UUID>();
        for (int i = 0; i < skus; i++) { UUID id = UUID.randomUUID(); products.add(id); stock.update("INSERT INTO inventory_stock VALUES (?,10000,0)", id); }
        var samples = new CopyOnWriteArrayList<Sample>();
        var completed = new ConcurrentHashMap<UUID, Double>();
        var httpLatencies = new CopyOnWriteArrayList<Double>();
        var outcomes = new ConcurrentHashMap<Integer, AtomicInteger>();
        AtomicInteger dropped = new AtomicInteger(), errors = new AtomicInteger();
        var monitorFailure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        var permits = new Semaphore(vus); long begin = System.nanoTime();
        long beforeCpu = cpuNanos();
        try (var workers = Executors.newFixedThreadPool(vus); var monitor = Executors.newSingleThreadScheduledExecutor()) {
            monitor.scheduleWithFixedDelay(() -> {
                try {
                var states = orders.queryForList("SELECT id,status FROM orders WHERE status IN ('CONFIRMED','CANCELLED')");
                var terminal = new HashSet<UUID>(); states.forEach(row -> terminal.add((UUID) row.get("id")));
                long now = System.nanoTime();
                for (Sample s : samples) if (terminal.contains(s.id())) completed.putIfAbsent(s.id(), (now - s.acceptedAt()) / 1_000_000.0);
                } catch (Throwable failure) { monitorFailure.compareAndSet(null, failure); }
            }, 0, 50, TimeUnit.MILLISECONDS);
            for (int i = 0; i < seconds * rps; i++) {
                long due = begin + (long) (i * (1_000_000_000.0 / rps));
                while (System.nanoTime() < due) LockSupport.parkNanos(Math.min(due - System.nanoTime(), 5_000_000));
                if (!permits.tryAcquire()) { dropped.incrementAndGet(); continue; }
                int n = i;
                workers.submit(() -> {
                    long start = System.nanoTime();
                    boolean out = n % 20 == 19, decline = n % 10 == 9 && !out;
                    try {
                        var body = Map.of("externalReference", "m30-" + UUID.randomUUID(), "description", "bounded isolated load",
                                "items", List.of(item(products.get(n % skus), out ? 100000 : 1)));
                        var response = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/api/v1/order-placements"))
                                .timeout(Duration.ofSeconds(10)).header("Content-Type", "application/json")
                                .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body))).build(), HttpResponse.BodyHandlers.ofString());
                        long accepted = System.nanoTime(); double latency = (accepted - start) / 1_000_000.0;
                        httpLatencies.add(latency); outcomes.computeIfAbsent(response.statusCode(), k -> new AtomicInteger()).incrementAndGet();
                        if (response.statusCode() != 201) { errors.incrementAndGet(); return; }
                        UUID id = UUID.fromString(JSON.readTree(response.body()).path("orderId").asText());
                        var sample = new Sample(id, accepted, latency, decline, out); samples.add(sample); all.add(sample);
                        if (!out) payments.update("INSERT INTO payment_simulation_policy VALUES (?,?)", id, decline ? "DECLINED" : "AUTHORIZED");
                    } catch (Exception failure) { errors.incrementAndGet(); }
                    finally { permits.release(); }
                });
            }
            workers.shutdown(); assertThat(workers.awaitTermination(15, TimeUnit.SECONDS)).isTrue();
            long cutoff = System.nanoTime();
            var cutoffStates = states(samples); var cutoffBacklog = snapshot();
            if (restart != null) {
                assertThat(samples).isNotEmpty();
                assertThat(cutoffStates.get("CONFIRMED")).isEqualTo(0L);
                assertThat(cutoffStates.get("compensatedCancelled")).isEqualTo(0L);
                restart.run();
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (System.nanoTime() < deadline && completed.size() < samples.size() && monitorFailure.get() == null) LockSupport.parkNanos(50_000_000);
            assertThat(monitorFailure.get()).as("Independent completion observer must not silently fail").isNull();
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                var snap = snapshot(); assertThat(snap.get("pendingOutbox")).isEqualTo(0L);
                assertThat(snap.get("lagTotal")).isEqualTo(0L);
            });
            var row = new LinkedHashMap<String, Object>();
            row.put("scenario", name); row.put("targetRps", rps); row.put("durationSeconds", seconds);
            row.put("submissionElapsedSeconds", (cutoff - begin) / 1_000_000_000.0);
            row.put("concurrency", vus); row.put("skus", skus); row.put("offered", seconds * rps);
            row.put("accepted", samples.size()); row.put("failedHttpOrFixture", errors.get()); row.put("notAdmitted", dropped.get());
            row.put("httpStatuses", outcomes.entrySet().stream().collect(java.util.stream.Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().get())));
            row.put("acceptedRps", samples.size() / Math.max(seconds, (cutoff - begin) / 1_000_000_000.0));
            row.put("httpLatencyMs", percentiles(httpLatencies)); row.put("sagaLatencyMs", percentiles(new ArrayList<>(completed.values())));
            row.put("incompleteAtDrainDeadline", samples.size() - completed.size());
            row.put("cutoffStates", cutoffStates); row.put("finalStates", states(samples));
            row.put("cutoffBacklog", cutoffBacklog); row.put("afterDrain", snapshot());
            row.put("observedCpuSeconds", (cpuNanos() - beforeCpu) / 1_000_000_000.0);
            row.put("hostCpuLoad", ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getCpuLoad());
            results.add(row);
            Files.writeString(output.resolve("m30-performance-partial.json"), JSON.writerWithDefaultPrettyPrinter().writeValueAsString(results));
            assertThat(errors.get()).as("HTTP/fixture failures in %s", name).isZero();
            assertThat(snapshot().get("dltRecords")).isEqualTo(0L);
            invariants();
            if (!samples.isEmpty() && completed.containsKey(samples.getFirst().id())) {
                assertThat(getStatus(http, port, samples.getFirst().id())).isIn("CONFIRMED", "CANCELLED");
            }
        }
    }

    private Map<String, Long> states(List<Sample> samples) {
        var ids = new HashSet<UUID>(); samples.forEach(s -> ids.add(s.id()));
        var result = new LinkedHashMap<String, Long>();
        for (String state : List.of("CONFIRMED", "CANCELLED", "CANCELLING", "PENDING_PAYMENT", "PENDING_INVENTORY", "otherPending", "compensatedCancelled", "outOfStockCancelled")) result.put(state, 0L);
        for (var row : orders.queryForList("SELECT id,status,inventory_decision,payment_decision FROM orders")) if (ids.contains(row.get("id"))) {
            String state = row.get("status").toString(); result.merge(state, 1L, Long::sum);
            if (state.equals("CANCELLED")) result.merge("REJECTED".equals(row.get("inventory_decision")) ? "outOfStockCancelled" : "compensatedCancelled", 1L, Long::sum);
        }
        return result;
    }

    private Map<String, Object> snapshot() throws Exception {
        var lags = new ArrayList<Map<String, Object>>(); long lag = 0, dlt = 0;
        for (var pair : Map.of(og, List.of(INVENTORY_TOPIC, PAYMENT), ig, List.of(ORDER_TOPIC, RELEASE), pg, List.of(REQUEST)).entrySet()) {
            var committed = admin.listConsumerGroupOffsets(pair.getKey()).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
            for (String topic : pair.getValue()) {
                var tp = new TopicPartition(topic, 0); long end = topicEnd(admin, topic);
                long position = committed.containsKey(tp) ? committed.get(tp).offset() : 0;
                long behind = Math.max(0, end - position); lag += behind;
                lags.add(Map.of("group", pair.getKey(), "topic", topic, "partition", 0, "end", end, "committed", position, "lag", behind));
            }
        }
        for (String topic : STREAMS) dlt += topicEnd(admin, topic + "-dlt");
        long pending = 0, dead = 0;
        var databases = new ArrayList<Map<String, Object>>();
        for (var owner : Map.of("order", orders, "inventory", stock, "payment", payments).entrySet()) {
            var db = owner.getValue();
            long p = db.queryForObject("SELECT count(*) FROM outbox_event WHERE NOT published AND NOT dead_letter", Long.class);
            long d = db.queryForObject("SELECT count(*) FROM outbox_event WHERE dead_letter", Long.class); pending += p; dead += d;
            databases.add(Map.of("cell", owner.getKey(), "pendingOutbox", p, "deadOutbox", d,
                    "port", db.queryForObject("SELECT inet_server_port()", Integer.class),
                    "dbConnections", db.queryForObject("SELECT numbackends FROM pg_stat_database WHERE datname=current_database()", Integer.class)));
        }
        return Map.of("lagTotal", lag, "consumerOffsets", lags, "dltRecords", dlt, "pendingOutbox", pending, "deadOutbox", dead, "databases", databases);
    }

    private void invariants() {
        var decisions = new HashMap<UUID, String>();
        payments.queryForList("SELECT order_id,status FROM payment_authorizations").forEach(r -> decisions.put((UUID) r.get("order_id"), r.get("status").toString()));
        var reservations = new HashMap<UUID, String>();
        stock.queryForList("SELECT order_id,status FROM inventory_reservations").forEach(r -> reservations.put((UUID) r.get("order_id"), r.get("status").toString()));
        for (var row : orders.queryForList("SELECT id,status,inventory_decision,payment_decision,release_acknowledged FROM orders")) {
            UUID id = (UUID) row.get("id"); String state = row.get("status").toString();
            if (state.equals("CONFIRMED")) { assertThat(decisions.get(id)).isEqualTo("AUTHORIZED"); assertThat(reservations.get(id)).isEqualTo("RESERVED"); assertThat(row.get("payment_decision")).isEqualTo("AUTHORIZED"); }
            if (state.equals("CANCELLED") && "REJECTED".equals(row.get("inventory_decision"))) { assertThat(decisions).doesNotContainKey(id); assertThat(reservations.get(id)).isEqualTo("REJECTED"); }
            if (state.equals("CANCELLED") && !"REJECTED".equals(row.get("inventory_decision"))) { assertThat(decisions.get(id)).isEqualTo("DECLINED"); assertThat(reservations.get(id)).isEqualTo("RELEASED"); assertThat(row.get("release_acknowledged")).isEqualTo(true); }
        }
        for (var row : stock.queryForList("SELECT s.product_id,s.available_quantity,s.reserved_quantity,COALESCE((SELECT sum(i.quantity) FROM inventory_reservation_items i JOIN inventory_reservations r USING(order_id) WHERE i.product_id=s.product_id AND r.status='RESERVED'),0) AS expected FROM inventory_stock s")) {
            int expected = ((Number) row.get("expected")).intValue();
            assertThat(((Number) row.get("available_quantity")).intValue()).isEqualTo(10000 - expected).isNotNegative();
            assertThat(((Number) row.get("reserved_quantity")).intValue()).isEqualTo(expected).isNotNegative();
        }
        assertThat(payments.queryForObject("SELECT count(*) FROM (SELECT order_id FROM payment_authorizations GROUP BY order_id HAVING count(*)>1) x", Long.class)).isZero();
        assertThat(orders.queryForObject("SELECT count(*) FROM (SELECT aggregate_id,event_type FROM outbox_event WHERE event_type IN ('ORDER_READY_FOR_PAYMENT','ORDER_CANCELLATION_REQUESTED') GROUP BY aggregate_id,event_type HAVING count(*)>1) x", Long.class)).isZero();
        for (var db : List.of(orders, stock, payments)) assertThat(db.queryForObject("SELECT count(*) FROM outbox_event WHERE dead_letter", Long.class)).isZero();
    }

    private void duplicateEffects() throws Exception {
        try (var wire = observer(brokers); var sender = producer(brokers)) {
            wire.assign(STREAMS.stream().map(t -> new TopicPartition(t, 0)).toList()); wire.seekToBeginning(wire.assignment());
            var cache = new HashMap<UUID, org.apache.kafka.clients.consumer.ConsumerRecord<String, String>>();
            Sample happy = all.stream().filter(s -> !s.declined() && !s.outOfStock()).findFirst().orElseThrow();
            Sample decline = all.stream().filter(Sample::declined).findFirst().orElseThrow();
            for (var tuple : List.of(Map.entry(happy.id(), "ORDER_PLACED"), Map.entry(happy.id(), "ORDER_READY_FOR_PAYMENT"), Map.entry(decline.id(), "ORDER_CANCELLATION_REQUESTED"))) {
                var event = typed(orders, tuple.getKey(), tuple.getValue());
                var original = observe(wire, cache, (UUID) event.get("event_id"));
                assertEnvelope(orders, (UUID) event.get("event_id"), original);
                sender.send(new ProducerRecord<>(original.topic(), 0, original.key(), original.value())).get(10, TimeUnit.SECONDS);
                ObjectNode copy = (ObjectNode) JSON.readTree(original.value()); ((ObjectNode) copy.path("metadata")).put("eventId", UUID.randomUUID().toString());
                var sent = sender.send(new ProducerRecord<>(original.topic(), 0, original.key(), JSON.writeValueAsString(copy))).get(10, TimeUnit.SECONDS);
                awaitConsumed(admin, original.topic().equals(REQUEST) ? pg : ig, sent.topic(), sent.offset());
            }
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> { assertThat(snapshot().get("lagTotal")).isEqualTo(0L); assertThat(snapshot().get("pendingOutbox")).isEqualTo(0L); });
            assertThat(payments.queryForObject("SELECT count(*) FROM payment_authorizations WHERE order_id=?", Long.class, happy.id())).isEqualTo(1);
            assertThat(payments.queryForObject("SELECT count(*) FROM outbox_event WHERE aggregate_id=?", Long.class, happy.id())).isEqualTo(1);
        }
    }

    private void metadataAtomicity() {
        var roots = new HashMap<UUID, UUID>();
        orders.queryForList("SELECT aggregate_id,correlation_id FROM outbox_event WHERE event_type='ORDER_PLACED'")
                .forEach(row -> roots.put((UUID) row.get("aggregate_id"), (UUID) row.get("correlation_id")));
        assertThat(roots).hasSize(all.size());
        for (var owner : List.of(orders, stock, payments)) {
            var inbox = new HashSet<>(owner.queryForList("SELECT event_id FROM processed_event", UUID.class));
            for (var event : owner.queryForList("SELECT aggregate_id,event_type,correlation_id,causation_id FROM outbox_event")) {
                assertThat(event.get("correlation_id")).as("Lossless concurrent request correlation")
                        .isEqualTo(roots.get((UUID) event.get("aggregate_id")));
                if (!"ORDER_PLACED".equals(event.get("event_type"))) {
                    assertThat(event.get("causation_id")).as("Outcome links to its committed local Inbox input")
                            .isNotNull().isIn(inbox);
                }
            }
        }
    }

    private long cpuNanos() { return java.util.stream.Stream.of(order, inventory, payment).filter(Objects::nonNull)
            .mapToLong(p -> p.info().totalCpuDuration().orElse(Duration.ZERO).toNanos()).sum(); }

    private List<Map<String, Object>> jvmDiagnostics() throws Exception {
        var result = new ArrayList<Map<String, Object>>();
        for (Process p : List.of(order, inventory, payment)) {
            Path file = output.resolve("m30-jvm-" + p.pid() + ".txt");
            var child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "jstat").toString(), "-gc", Long.toString(p.pid()))
                    .redirectErrorStream(true).redirectOutput(file.toFile()).start();
            try {
                if (child.waitFor(10, TimeUnit.SECONDS) && child.exitValue() == 0) result.add(Map.of("pid", p.pid(), "gcHeapSnapshot", Files.readString(file)));
                else result.add(Map.of("pid", p.pid(), "gcHeapSnapshot", "NOT VERIFIED: jstat unavailable"));
            } finally { if (child.isAlive()) child.destroyForcibly(); }
        }
        return result;
    }

    private static int bounded(String property, int fallback, int low, int high) {
        String configured = System.getProperty(property);
        int value = configured == null ? fallback : Integer.parseInt(configured);
        assertThat(value).as(property).isBetween(low, high); return value;
    }

    private static Map<String, Object> percentiles(List<Double> data) {
        if (data.isEmpty()) return Map.of("count", 0, "status", "NOT VERIFIED: no samples");
        var sorted = data.stream().sorted().toList();
        return Map.of("count", sorted.size(), "p50", sorted.get((int) Math.ceil(sorted.size() * .50) - 1),
                "p95", sorted.get((int) Math.ceil(sorted.size() * .95) - 1), "p99", sorted.get((int) Math.ceil(sorted.size() * .99) - 1));
    }
}
