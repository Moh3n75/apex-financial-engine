package com.apex.inventory;

import static com.apex.inventory.OrderInventorySagaIT.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

/**
 * Owns disposable Order/Inventory/Payment + Console stack, then drives OPS-04B Playwright
 * against a non-fixture Next.js BFF. Does not rewrite Saga/security behavior.
 */
@Tag("integration")
class Ops04bLiveUiBridgeIT {
    static final String REQUEST = Ops03dRecentDiscoveryIT.REQUEST;
    static final String RELEASE = Ops03dRecentDiscoveryIT.RELEASE;
    static final String PAYMENT = Ops03dRecentDiscoveryIT.PAYMENT;
    static final int CONSOLE_PORT = 9190;
    static final int NEXT_PORT = 3000;

    @Test
    void browserBffConsoleLiveAcceptance() throws Exception {
        Path root = Path.of(System.getProperty("saga.repositoryRoot", ".")).toAbsolutePath().normalize();
        Path inventoryJar = Path.of(System.getProperty("saga.inventoryJar",
                root.resolve("out/ops04b/build/inventory-cell/inventory-cell.jar").toString())).toAbsolutePath().normalize();
        Path orderJar = Path.of(System.getProperty("saga.orderJar",
                root.resolve("out/ops04b/build/order-cell/order-cell.jar").toString())).toAbsolutePath().normalize();
        Path paymentJar = Path.of(System.getProperty("saga.paymentJar",
                root.resolve("out/ops04b/build/payment-cell/payment-cell.jar").toString())).toAbsolutePath().normalize();
        Path consoleJar = Path.of(System.getProperty("saga.consoleJar", root
                .resolve("out/ops04b/build/apex-operations-console/apex-operations-console-1.0-SNAPSHOT.jar").toString()))
                .toAbsolutePath().normalize();
        for (Path jar : List.of(orderJar, inventoryJar, paymentJar, consoleJar)) {
            assertThat(jar).as(jar.toString()).isRegularFile();
            assertThat(Files.size(jar)).as(jar + " must be a packaged Spring Boot jar").isGreaterThan(1_000_000L);
        }

        Path out = root.resolve("out/ops04b");
        Files.createDirectories(out);
        Path handshake = out.resolve("live-handshake.json");
        Path receipt = out.resolve("live-bridge-receipt.json");
        Files.deleteIfExists(handshake);
        Files.deleteIfExists(out.resolve("live-done.flag"));

        assertLoopbackPortFree(CONSOLE_PORT);
        assertLoopbackPortFree(NEXT_PORT);

        var ops = new Ops03dRecentDiscoveryIT();
        ops.root = root;
        Process order = null, inventory = null, payment = null, next = null;
        var streams = List.of(ORDER_TOPIC, INVENTORY_TOPIC, REQUEST, RELEASE, PAYMENT, "apex.order.legacy.events.v1");
        var topics = new ArrayList<String>();
        for (String s : streams) {
            topics.add(s);
            topics.add(s + "-dlt");
        }
        var broker = new EmbeddedKafkaKraftBroker(1, 1, topics.toArray(String[]::new));
        broker.brokerProperty("auto.create.topics.enable", "false");
        broker.brokerListProperty("apex.m27.test.brokers");

        Instant windowStart = Instant.now().minusSeconds(30);
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("headNote", "OPS-04B live UI bridge");
        summary.put("consolePort", CONSOLE_PORT);
        summary.put("nextPort", NEXT_PORT);

        try (var ownedCerts = new Ops03bCertificates();
                var orderDb = EmbeddedPostgres.builder().start();
                var invDb = EmbeddedPostgres.builder().start();
                var payDb = EmbeddedPostgres.builder().start();
                var http = HttpClient.newHttpClient()) {
            ops.certs = ownedCerts;
            ops.apiClient = http;
            try {
                broker.afterPropertiesSet();
                String brokers = broker.getBrokersAsString();
                String og = "order-three-" + UUID.randomUUID();
                String ig = "inventory-three-" + UUID.randomUUID();
                String pg = "payment-three-" + UUID.randomUUID();
                int op = freePort();
                int ip = freePort();
                Path ol = out.resolve("live-order.log");
                Path il = out.resolve("live-inventory.log");
                Path pl = out.resolve("live-payment.log");

                order = ops.launch(orderJar, orderDb, brokers, og, op, ol, ORDER_TOPIC, INVENTORY_TOPIC + "," + PAYMENT,
                        "--spring.profiles.active=saga,three-cell-saga");
                inventory = ops.launch(inventoryJar, invDb, brokers, ig, ip, il, INVENTORY_TOPIC, ORDER_TOPIC + "," + RELEASE,
                        "--spring.profiles.active=saga,three-cell-saga",
                        "--apex.messaging.consumer.reliability.max-attempts=10");
                payment = ops.launch(paymentJar, payDb, brokers, pg, 0, pl, PAYMENT, REQUEST,
                        "--spring.profiles.active=three-cell-saga,simulation",
                        "--apex.messaging.consumer.reliability.max-attempts=10");
                ready(order, ol, http, op);
                ready(inventory, il, http, ip);
                Ops03dRecentDiscoveryIT.paymentReady(payment, pl);

                var orders = new JdbcTemplate(orderDb.getPostgresDatabase());
                var stock = new JdbcTemplate(invDb.getPostgresDatabase());
                var pay = new JdbcTemplate(payDb.getPostgresDatabase());
                for (var jdbc : List.of(orders, stock, pay)) {
                    Ops03dRecentDiscoveryIT.grantReader(jdbc);
                    applyRecentIndexes(ops, jdbc);
                }

                startConsoleOnFixedPort(ops, ownedCerts.primary, "m32-client", CONSOLE_PORT);
                assertThat(ops.api("/ops/v1/recent/search/sources")).contains("OUTBOX_EVENT");

                UUID product = UUID.randomUUID();
                stock.update("INSERT INTO inventory_stock VALUES (?,10,0)", product);
                UUID orderId = UUID.fromString(post(http, op, List.of(item(product, 2))).path("orderId").asText());
                pay.update("INSERT INTO payment_simulation_policy VALUES (?,'AUTHORIZED')", orderId);
                awaitStatus(orders, orderId, "CONFIRMED");
                assertThat(getStatus(http, op, orderId)).isEqualTo("CONFIRMED");

                List<UUID> orderEvents = orders.queryForList(
                        "SELECT event_id FROM outbox_event WHERE aggregate_id=? ORDER BY created_at", UUID.class, orderId);
                assertThat(orderEvents).isNotEmpty();
                // Independent owner SQL cross-check (not Console semantics).
                assertThat(orders.queryForObject("SELECT status FROM orders WHERE id=?", String.class, orderId))
                        .isEqualTo("CONFIRMED");
                assertThat(stock.queryForObject("SELECT status FROM inventory_reservations WHERE order_id=?", String.class,
                        orderId)).isEqualTo("RESERVED");
                assertThat(pay.queryForObject("SELECT status FROM payment_authorizations WHERE order_id=?", String.class,
                        orderId)).isEqualTo("AUTHORIZED");

                // Prove Console can see the new outbox id before handing control to Playwright.
                // Honor one-admitted-read/s by spacing polls ≥2.1s.
                await().pollInterval(Duration.ofMillis(2100)).atMost(Duration.ofSeconds(90)).untilAsserted(() -> {
                    String body = ops.search("/ops/v1/recent/events", "order-cell", "owner");
                    var view = JSON.readTree(body);
                    assertThat(view.path("availability").asText()).isEqualTo("AVAILABLE");
                    assertThat(body).contains(orderEvents.get(0).toString());
                });
                // Drain Console admission backoff before Browser→BFF traffic begins.
                Thread.sleep(3000L);
                Instant windowTo = Instant.now().plusSeconds(30);

                Map<String, Object> hs = baseHandshake(windowStart, windowTo, orderId, orderEvents, ig);
                hs.put("phase", "ready");
                writeJson(handshake, hs);
                summary.put("orderId", orderId.toString());
                summary.put("orderEventIds", orderEvents.stream().map(UUID::toString).toList());
                summary.put("sqlCrossCheck", Map.of("order", "CONFIRMED", "inventory", "RESERVED", "payment", "AUTHORIZED"));

                next = startNext(root, CONSOLE_PORT, NEXT_PORT, out.resolve("live-next.log"));
                Process nextRef = next;
                await().pollInterval(Duration.ofSeconds(1)).ignoreExceptions().atMost(Duration.ofSeconds(120))
                        .untilAsserted(() -> {
                            assertThat(nextRef.isAlive())
                                    .as(() -> "next process died; log=" + safeRead(out.resolve("live-next.log")))
                                    .isTrue();
                            // HttpURLConnection is more reliable than java.net.http on this host for loopback.
                            var conn = (java.net.HttpURLConnection) java.net.URI
                                    .create("http://127.0.0.1:" + NEXT_PORT + "/")
                                    .toURL().openConnection();
                            conn.setConnectTimeout(3000);
                            conn.setReadTimeout(3000);
                            conn.setRequestMethod("GET");
                            int code = conn.getResponseCode();
                            conn.disconnect();
                            assertThat(code)
                                    .as(() -> "next HTTP status; log=" + safeRead(out.resolve("live-next.log")))
                                    .isLessThan(500);
                        });

                int happyExit = runPlaywright(root, handshake, "e2e/live-happy.spec.ts", out.resolve("playwright-happy.log"));
                summary.put("playwrightHappyExit", happyExit);
                assertThat(happyExit).as("live-happy Playwright").isZero();

                // Source stop → STALE/MISSING UI without durable projection claim.
                stop(payment);
                payment = null;
                hs.put("phase", "payment-stopped");
                hs.put("windowTo", Instant.now().plusSeconds(5).toString());
                writeJson(handshake, hs);
                // Console freshness window is 10s; wait past it so UI can observe STALE/MISSING.
                Thread.sleep(15_000L);
                Files.deleteIfExists(out.resolve("playwright-stale.log"));
                int staleExit = runPlaywright(root, handshake, "e2e/live-stale.spec.ts", out.resolve("playwright-stale.log"));
                summary.put("playwrightStaleExit", staleExit);
                assertThat(staleExit).as("live-stale Playwright").isZero();

                try (var admin = Admin.create(Map.of("bootstrap.servers", brokers))) {
                    Ops03dRecentDiscoveryIT.assertNoDlt(admin, streams);
                }
                summary.put("verdict", "LIVE_E2E_BRIDGE_PASS");
            } finally {
                try {
                    stop(next);
                } finally {
                    try {
                        stop(ops.console);
                    } finally {
                        try {
                            stop(payment);
                        } finally {
                            try {
                                stop(inventory);
                            } finally {
                                try {
                                    stop(order);
                                } finally {
                                    broker.destroy();
                                    if (ops.certs != null) {
                                        ops.certs.close();
                                    }
                                    Files.writeString(out.resolve("cleanup.json"),
                                            JSON.writeValueAsString(Map.of(
                                                    "children", ops.children.stream().map(Process::pid).toList(),
                                                    "allStopped", ops.children.stream().noneMatch(Process::isAlive),
                                                    "at", Instant.now().toString())));
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            writeJson(receipt, summary);
            Files.writeString(out.resolve("live-done.flag"), Instant.now().toString());
        }
    }

    static void assertLoopbackPortFree(int port) throws Exception {
        try (ServerSocket socket = new ServerSocket()) {
            socket.setReuseAddress(false);
            socket.bind(new InetSocketAddress("127.0.0.1", port));
        } catch (Exception e) {
            throw new AssertionError("Loopback port " + port
                    + " is busy; OPS-04B refuses to kill unrelated processes. Free the port and retry.", e);
        }
    }

    static void applyRecentIndexes(Ops03dRecentDiscoveryIT ops, JdbcTemplate db) throws Exception {
        String cell = db.queryForObject("SELECT to_regclass('orders') IS NOT NULL", Boolean.class) ? "order-cell"
                : db.queryForObject("SELECT to_regclass('inventory_reservations') IS NOT NULL", Boolean.class)
                        ? "inventory-cell"
                        : "payment-cell";
        String ddl = Files.readString(ops.root.resolve(cell + "/src/main/resources/db/ops03d/recent-indexes.sql"))
                .replaceAll("(?m)--.*$", "");
        try (var connection = db.getDataSource().getConnection(); var statement = connection.createStatement()) {
            assertThat(connection.getAutoCommit()).isTrue();
            for (String command : ddl.split(";")) {
                if (!command.isBlank()) {
                    statement.execute(command);
                }
            }
        }
    }

    static void startConsoleOnFixedPort(Ops03dRecentDiscoveryIT ops, Path keys, String identity, int port)
            throws Exception {
        ops.consolePort = port;
        var grants = new ArrayList<String>();
        for (String cell : ops.ports.keySet()) {
            grants.add(cell + "/WORKFLOW/"
                    + (cell.equals("order-cell") ? "order"
                            : cell.equals("inventory-cell") ? "reservation" : "authorization-simulation"));
            grants.add(cell + "/OUTBOX_EVENT/owner");
            grants.add(cell + "/KNOWN_GROUP_INBOX/" + ops.groups.get(cell));
        }
        var args = new ArrayList<String>(List.of("--server.port=" + port, "--server.address=127.0.0.1",
                "--apex.grpc.cell-id=" + identity, "--apex.grpc.client.enabled=true",
                "--apex.ops03d.search-enabled=true", "--apex.ops03d.environment=test",
                "--apex.ops03d.sources=" + String.join(",", grants)));
        for (var entry : ops.ports.entrySet()) {
            args.addAll(List.of("--apex.grpc.destinations." + entry.getKey() + ".channel=" + entry.getKey(),
                    "--spring.grpc.client.channel." + entry.getKey() + ".target=static://127.0.0.1:" + entry.getValue(),
                    "--spring.grpc.client.channel." + entry.getKey() + ".ssl.enabled=true",
                    "--spring.grpc.client.channel." + entry.getKey() + ".ssl.bundle=m32"));
        }
        args.addAll(ops.certs.bundle(keys));
        Path consoleJar = Path.of(System.getProperty("saga.consoleJar", ops.root
                .resolve("out/ops04b/build/apex-operations-console/apex-operations-console-1.0-SNAPSHOT.jar").toString()))
                .toAbsolutePath().normalize();
        var command = new ArrayList<String>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx256m", "-jar",
                consoleJar.toString()));
        command.addAll(args);
        var builder = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(ops.root.resolve("out/ops04b/console.log").toFile());
        builder.environment().keySet()
                .removeIf(k -> k.matches("(?i)(SPRING_.*|APEX_.*|CELL_.*|SERVER_.*|JAVA_TOOL_OPTIONS|JDK_JAVA_OPTIONS|_JAVA_OPTIONS)"));
        ops.console = builder.start();
        ops.children.add(ops.console);
        await().ignoreExceptions().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            assertThat(ops.console.isAlive()).isTrue();
            assertThat(ops.api("/ops/v1/recent/search/sources")).contains("OUTBOX_EVENT");
        });
    }

    static Map<String, Object> baseHandshake(Instant from, Instant to, UUID orderId, List<UUID> orderEvents,
            String inboxGroup) {
        Map<String, Object> hs = new LinkedHashMap<>();
        hs.put("fixtureMode", false);
        hs.put("consoleOrigin", "http://127.0.0.1:" + CONSOLE_PORT);
        hs.put("nextOrigin", "http://127.0.0.1:" + NEXT_PORT);
        hs.put("consolePort", CONSOLE_PORT);
        hs.put("environment", "test");
        hs.put("orderId", orderId.toString());
        hs.put("eventIds", Map.of("order-cell", orderEvents.stream().map(UUID::toString).toList()));
        hs.put("workflowStates",
                Map.of("order", "CONFIRMED", "reservation", "RESERVED", "authorization-simulation", "AUTHORIZED"));
        hs.put("inboxGroup", inboxGroup);
        hs.put("windowFrom", from.toString());
        hs.put("windowTo", to.toString());
        return hs;
    }

    static Process startNext(Path root, int consolePort, int nextPort, Path log) throws Exception {
        Path web = root.resolve("apps/operations-web");
        assertThat(web.resolve("package.json")).isRegularFile();
        Path nextJs = web.resolve("node_modules/next/dist/bin/next");
        assertThat(nextJs).as("next dist bin").isRegularFile();
        Path node = resolveNode();
        // Invoke node directly (avoid .cmd wrapper exiting early under ProcessBuilder).
        var command = List.of(node.toString(), nextJs.toString(), "start", "-H", "127.0.0.1", "-p",
                Integer.toString(nextPort));
        var builder = new ProcessBuilder(command).directory(web.toFile()).redirectErrorStream(true)
                .redirectOutput(log.toFile());
        var env = builder.environment();
        env.put("APEX_OPERATIONS_API_ORIGIN", "http://127.0.0.1:" + consolePort);
        env.put("OPS04B_REQUIRE_LIVE", "true");
        env.remove("APEX_OPS_UI_FIXTURE_MODE");
        env.put("APEX_OPS_UI_FIXTURE_MODE", "false");
        prependLocalNode(env);
        Process process = builder.start();
        Files.writeString(root.resolve("out/ops04b/next-launch.json"),
                JSON.writeValueAsString(Map.of("pid", process.pid(), "command", command, "at", Instant.now().toString())));
        return process;
    }

    static Path resolveNode() {
        Path local = Path.of(System.getProperty("user.home"), ".local-node", "node.exe");
        if (Files.isRegularFile(local)) {
            return local;
        }
        Path program = Path.of("C:\\Program Files\\nodejs\\node.exe");
        if (Files.isRegularFile(program)) {
            return program;
        }
        throw new IllegalStateException("node.exe not found for Next.js live launch");
    }

    static String safeRead(Path log) {
        try {
            if (!Files.isRegularFile(log)) {
                return "<missing>";
            }
            String text = Files.readString(log);
            return text.substring(0, Math.min(text.length(), 4000));
        } catch (Exception e) {
            return "<unreadable:" + e.getMessage() + ">";
        }
    }

    static int runPlaywright(Path root, Path handshake, String spec, Path log) throws Exception {
        Path web = root.resolve("apps/operations-web");
        Path pwCmd = web.resolve("node_modules/.bin/playwright.cmd");
        List<String> command;
        if (Files.isRegularFile(pwCmd)) {
            command = List.of(pwCmd.toString(), "test", "-c", "playwright.live.config.ts", spec);
        } else {
            String pnpm = resolvePnpm();
            command = List.of("cmd.exe", "/c", pnpm, "exec", "playwright", "test", "-c",
                    "playwright.live.config.ts", spec);
        }
        var builder = new ProcessBuilder(command).directory(web.toFile()).redirectErrorStream(true)
                .redirectOutput(log.toFile());
        var env = builder.environment();
        env.put("OPS04B_HANDSHAKE", handshake.toAbsolutePath().toString());
        env.put("OPS04B_LIVE", "1");
        env.put("OPS04B_NEXT_ORIGIN", "http://127.0.0.1:" + NEXT_PORT);
        env.put("OPS04B_REQUIRE_LIVE", "true");
        env.remove("APEX_OPS_UI_FIXTURE_MODE");
        prependLocalNode(env);
        Process process = builder.start();
        assertThat(process.waitFor(10, TimeUnit.MINUTES)).as("playwright timeout for " + spec).isTrue();
        return process.exitValue();
    }

    static String resolvePnpm() {
        Path local = Path.of(System.getProperty("user.home"), ".local-node", "pnpm.cmd");
        if (Files.isRegularFile(local)) {
            return local.toString();
        }
        Path localBin = Path.of(System.getProperty("user.home"), ".local-node", "bin", "pnpm.cmd");
        if (Files.isRegularFile(localBin)) {
            return localBin.toString();
        }
        return "pnpm.cmd";
    }

    static void prependLocalNode(Map<String, String> env) {
        Path local = Path.of(System.getProperty("user.home"), ".local-node");
        Path bin = local.resolve("bin");
        String extra = local + java.io.File.pathSeparator + bin;
        String path = env.getOrDefault("Path", env.getOrDefault("PATH", ""));
        env.put("Path", extra + java.io.File.pathSeparator + path);
        env.put("PATH", env.get("Path"));
    }

    static void writeJson(Path path, Object value) throws Exception {
        Files.createDirectories(path.getParent());
        Files.writeString(path, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(value), StandardCharsets.UTF_8);
    }
}
