package com.apex.inventory;

import static com.apex.inventory.OrderInventorySagaIT.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

/** Explicit interactive CLI, NOT a JUnit test. Owns only disposable demo resources. */
public final class ApexDemoSupervisor {
    private static final Map<Long, String> FINGERPRINTS = new java.util.concurrent.ConcurrentHashMap<>();
    private final Path root, run;
    private final Ops03dRecentDiscoveryIT ops = new Ops03dRecentDiscoveryIT();
    private final Map<String, Process> processes = new LinkedHashMap<>();
    private final List<EmbeddedPostgres> databases = new ArrayList<>();
    private final List<Map<String, Object>> samples = new ArrayList<>();
    private final Map<Long, ProcessHandle> nativeChildren = new LinkedHashMap<>();
    private String brokerAddress = "STARTING";
    private HttpClient http;
    private JdbcTemplate orders, stock, pay;
    private int orderPort, inventoryPort;
    private boolean closed;
    private String phase = "STARTING";

    private ApexDemoSupervisor(Path root, Path run) { this.root = root; this.run = run; }

    public static void main(String[] args) throws Exception {
        var demo = new ApexDemoSupervisor(Path.of(args[0]), Path.of(args[1]));
        Runtime.getRuntime().addShutdownHook(new Thread(demo::close, "demo-owned-cleanup"));
        try {
            demo.manifest();
            demo.start();
            // Persistent supervisor ends ONLY on explicit stop or an owned child failure.
            while (!Files.exists(demo.run.resolve("stop.flag"))) {
                for (var e : demo.processes.entrySet())
                    if (!e.getValue().isAlive()) throw new IllegalStateException("Owned service exited: " + e.getKey());
                demo.commands();
                Thread.sleep(250);
            }
        } catch (Throwable failure) {
            demo.phase = "FAILED";
            // Deliberately exclude exception text, which may contain a JDBC URL or credential.
            Files.writeString(demo.run.resolve("failure.txt"), "Supervisor failure type=" + failure.getClass().getSimpleName()
                    + "; inspect service logs in this owned directory.\n");
            demo.manifest();
            throw new IllegalStateException("Demo failed; see owned run directory (details not exported)");
        } finally { demo.close(); }
    }

    private void start() throws Exception {
        Files.createDirectories(run.resolve("commands"));
        Ops04bLiveUiBridgeIT.assertLoopbackPortFree(3000);
        Ops04bLiveUiBridgeIT.assertLoopbackPortFree(9190);
        ops.root = root;
        ops.consolePort = 9190;
        ops.certs = new Ops03bCertificates();
        http = HttpClient.newHttpClient();
        ops.apiClient = http;
        var streams = List.of(ORDER_TOPIC, INVENTORY_TOPIC, Ops03dRecentDiscoveryIT.REQUEST,
                Ops03dRecentDiscoveryIT.RELEASE, Ops03dRecentDiscoveryIT.PAYMENT, "apex.order.legacy.events.v1");
        var topics = new ArrayList<String>();
        streams.forEach(s -> { topics.add(s); topics.add(s + "-dlt"); });
        startKraft(topics);
        for (int i = 0; i < 3; i++) {
            databases.add(EmbeddedPostgres.builder().setDataDirectory(run.resolve("postgres-" + i))
                    .setServerConfig("listen_addresses", "127.0.0.1").start());
            manifest();
        }
        orderPort = freePort(); inventoryPort = freePort();
        launchOwner("order-cell", 0, orderPort, ORDER_TOPIC, INVENTORY_TOPIC + "," + Ops03dRecentDiscoveryIT.PAYMENT,
                "--spring.profiles.active=saga,three-cell-saga");
        launchOwner("inventory-cell", 1, inventoryPort, INVENTORY_TOPIC, ORDER_TOPIC + "," + Ops03dRecentDiscoveryIT.RELEASE,
                "--spring.profiles.active=saga,three-cell-saga", "--apex.messaging.consumer.reliability.max-attempts=10");
        launchOwner("payment-cell", 2, 0, Ops03dRecentDiscoveryIT.PAYMENT, Ops03dRecentDiscoveryIT.REQUEST,
                "--spring.profiles.active=three-cell-saga,simulation", "--apex.messaging.consumer.reliability.max-attempts=10");
        ready(processes.get("order-cell"), run.resolve("order-cell.log"), http, orderPort);
        ready(processes.get("inventory-cell"), run.resolve("inventory-cell.log"), http, inventoryPort);
        Ops03dRecentDiscoveryIT.paymentReady(processes.get("payment-cell"), run.resolve("payment-cell.log"));
        orders = new JdbcTemplate(databases.get(0).getPostgresDatabase());
        stock = new JdbcTemplate(databases.get(1).getPostgresDatabase());
        pay = new JdbcTemplate(databases.get(2).getPostgresDatabase());
        for (var db : List.of(orders, stock, pay)) {
            Ops03dRecentDiscoveryIT.grantReader(db);
            Ops04bLiveUiBridgeIT.applyRecentIndexes(ops, db);
        }
        launchConsole();
        var seed = createOrder();
        samples.add(seed);
        write(run.resolve("seed.json"), seed);
        startNext();
        phase = "RUNNING";
        manifest();
    }

    private void launchOwner(String cell, int db, int port, String publish, String subscribe, String... overrides) throws Exception {
        var process = ops.launch(root.resolve(cell + "/target/" + cell + ".jar"), databases.get(db),
                brokerAddress, cell.replace("-cell", "-three-demo"), port,
                run.resolve(cell + ".log"), publish, subscribe, overrides);
        processes.put(cell, process);
        manifest();
    }

    private void launchConsole() throws Exception {
        var grants = new ArrayList<String>();
        for (var cell : ops.ports.keySet()) {
            grants.add(cell + "/WORKFLOW/" + (cell.equals("order-cell") ? "order" : cell.equals("inventory-cell") ? "reservation" : "authorization-simulation"));
            grants.add(cell + "/OUTBOX_EVENT/owner");
            grants.add(cell + "/KNOWN_GROUP_INBOX/" + ops.groups.get(cell));
        }
        var args = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin/java.exe").toString(), "-Xmx256m", "-jar",
                root.resolve("apex-operations-console/target/apex-operations-console-1.0-SNAPSHOT.jar").toString(),
                "--server.port=9190", "--server.address=127.0.0.1", "--apex.ops.m36a.enabled=false",
                "--apex.ops.m36b.resource-auth-enabled=false", "--apex.grpc.cell-id=m32-client", "--apex.grpc.client.enabled=true",
                "--apex.ops03d.search-enabled=true", "--apex.ops03d.environment=test", "--apex.ops03.environment=test",
                "--apex.ops03.owners=order-cell,inventory-cell,payment-cell", "--apex.ops03d.sources=" + String.join(",", grants)));
        for (var e : ops.ports.entrySet()) args.addAll(List.of("--apex.grpc.destinations." + e.getKey() + ".channel=" + e.getKey(),
                "--spring.grpc.client.channel." + e.getKey() + ".target=static://127.0.0.1:" + e.getValue(),
                "--spring.grpc.client.channel." + e.getKey() + ".ssl.enabled=true",
                "--spring.grpc.client.channel." + e.getKey() + ".ssl.bundle=m32"));
        args.addAll(ops.certs.bundle(ops.certs.primary));
        Process p = spawn("console", args, root);
        ops.console = p;
        long until = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(90);
        boolean ready = false;
        while (p.isAlive() && System.nanoTime() < until) {
            try { if (ops.api("/ops/v1/recent/search/sources").contains("OUTBOX_EVENT")) { ready = true; break; } }
            catch (Exception ignored) { }
            Thread.sleep(1000);
        }
        if (!ready) throw new IllegalStateException("Console readiness failed");
    }

    private void startKraft(List<String> topics) throws Exception {
        // Same Kafka test dependency/version as the acceptance fixture. Its TestKit
        // prebinds wildcard sockets regardless of listeners; use Kafka's actual CLI
        // bootstrap here so this persistent local demo NEVER opens wildcard listeners.
        int external = freePort(), controller = freePort();
        brokerAddress = "127.0.0.1:" + external;
        var properties = new Properties();
        properties.setProperty("process.roles", "broker,controller");
        properties.setProperty("node.id", "1");
        properties.setProperty("controller.quorum.voters", "1@127.0.0.1:" + controller);
        properties.setProperty("listeners", "EXTERNAL://127.0.0.1:" + external + ",CONTROLLER://127.0.0.1:" + controller);
        properties.setProperty("advertised.listeners", "EXTERNAL://" + brokerAddress);
        properties.setProperty("listener.security.protocol.map", "EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT");
        properties.setProperty("inter.broker.listener.name", "EXTERNAL");
        properties.setProperty("controller.listener.names", "CONTROLLER");
        properties.setProperty("log.dirs", run.resolve("kafka-data").toString());
        properties.setProperty("auto.create.topics.enable", "false");
        properties.setProperty("offsets.topic.replication.factor", "1");
        properties.setProperty("transaction.state.log.replication.factor", "1");
        properties.setProperty("transaction.state.log.min.isr", "1");
        properties.setProperty("group.initial.rebalance.delay.ms", "0");
        try (var writer = Files.newBufferedWriter(run.resolve("kafka.properties"))) { properties.store(writer, "OWNED LOCAL DISPOSABLE DEMO"); }
        var format = kafkaProcess("storage-format", "kafka.tools.StorageTool", List.of("format", "-t",
                org.apache.kafka.common.Uuid.randomUuid().toString(), "-c", run.resolve("kafka.properties").toString()));
        if (!format.waitFor(30, java.util.concurrent.TimeUnit.SECONDS) || format.exitValue() != 0)
            throw new IllegalStateException("Owned KRaft format failed");
        var kraft = kafkaProcess("kraft", "kafka.Kafka", List.of(run.resolve("kafka.properties").toString()));
        long until = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(60);
        try (var admin = org.apache.kafka.clients.admin.Admin.create(Map.of("bootstrap.servers", brokerAddress,
                "request.timeout.ms", "2000", "default.api.timeout.ms", "2000"))) {
            boolean ready = false;
            while (kraft.isAlive() && System.nanoTime() < until) {
                try { admin.listTopics().names().get(2, java.util.concurrent.TimeUnit.SECONDS); ready = true; break; }
                catch (Exception ignored) { Thread.sleep(500); }
            }
            if (!ready) throw new IllegalStateException("Owned KRaft readiness failed");
            admin.createTopics(topics.stream().map(t -> new org.apache.kafka.clients.admin.NewTopic(t, 1, (short) 1)).toList())
                    .all().get(10, java.util.concurrent.TimeUnit.SECONDS);
        }
        processes.remove("storage-format");
    }

    private Process kafkaProcess(String name, String mainClass, List<String> arguments) throws Exception {
        var lines = new ArrayList<>(List.of("-Xmx256m", "-cp", quoteArg(System.getProperty("java.class.path")), mainClass));
        arguments.forEach(arg -> lines.add(quoteArg(arg)));
        Path argfile = run.resolve(name + ".args"); Files.write(argfile, lines);
        return spawn(name, List.of(Path.of(System.getProperty("java.home"), "bin/java.exe").toString(), "@" + argfile), root);
    }
    private static String quoteArg(String value) { return "\"" + value.replace("\\", "/").replace("\"", "\\\"") + "\""; }

    private void startNext() throws Exception {
        Path web = root.resolve("apps/operations-web");
        var builder = builder("next", List.of(Ops04bLiveUiBridgeIT.resolveNode().toString(),
                web.resolve("node_modules/next/dist/bin/next").toString(), "start", "-H", "127.0.0.1", "-p", "3000"), web);
        var env = builder.environment();
        env.put("APEX_OPERATIONS_API_ORIGIN", "http://127.0.0.1:9190");
        env.put("APEX_OPS_UI_FIXTURE_MODE", "false"); env.put("OPS04B_REQUIRE_LIVE", "true");
        env.put("APEX_OPS_M36A_ENABLED", "false"); env.put("APEX_OPS_M36B_ENABLED", "false");
        var p = builder.start(); processes.put("next", p); manifest();
        long until = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(90);
        while (p.isAlive() && System.nanoTime() < until) {
            try {
                var c = (java.net.HttpURLConnection) URI.create("http://127.0.0.1:3000/").toURL().openConnection();
                c.setConnectTimeout(2000); c.setReadTimeout(4000);
                int status = c.getResponseCode(); c.disconnect();
                if (status == 200) return;
            } catch (Exception ignored) { }
            Thread.sleep(1000);
        }
        throw new IllegalStateException("Next readiness failed");
    }

    private ProcessBuilder builder(String name, List<String> command, Path directory) {
        var b = new ProcessBuilder(command).directory(directory.toFile()).redirectErrorStream(true)
                .redirectOutput(run.resolve(name + ".log").toFile());
        b.environment().keySet().removeIf(k -> k.matches("(?i)(SPRING_.*|APEX_.*|OPS04B_.*|OIDC_.*|CELL_.*|SERVER_.*|JAVA_TOOL_OPTIONS|JDK_JAVA_OPTIONS|_JAVA_OPTIONS)"));
        return b;
    }
    private Process spawn(String name, List<String> command, Path directory) throws Exception {
        Process p = builder(name, command, directory).start(); processes.put(name, p); manifest(); return p;
    }

    private Map<String, Object> createOrder() throws Exception {
        UUID product = UUID.randomUUID();
        stock.update("INSERT INTO inventory_stock VALUES (?,1000,0)", product);
        UUID id = UUID.fromString(post(http, orderPort, List.of(item(product, 2))).path("orderId").asText());
        pay.update("INSERT INTO payment_simulation_policy VALUES (?,'AUTHORIZED')", id);
        awaitStatus(orders, id, "CONFIRMED");
        String inventory = stock.queryForObject("SELECT status FROM inventory_reservations WHERE order_id=?", String.class, id);
        String payment = pay.queryForObject("SELECT status FROM payment_authorizations WHERE order_id=?", String.class, id);
        if (!"RESERVED".equals(inventory) || !"AUTHORIZED".equals(payment)) throw new IllegalStateException("Owner state mismatch");
        var events = new LinkedHashMap<String, List<String>>();
        for (var e : Map.of("order-cell", orders, "inventory-cell", stock, "payment-cell", pay).entrySet())
            events.put(e.getKey(), e.getValue().queryForList("SELECT event_id FROM outbox_event WHERE aggregate_id=? ORDER BY created_at", UUID.class, id)
                    .stream().map(UUID::toString).toList());
        var result = new LinkedHashMap<String, Object>();
        result.put("orderId", id.toString()); result.put("eventIds", events);
        result.put("sqlCrossCheck", Map.of("order", "CONFIRMED", "inventory", inventory, "payment", payment));
        result.put("scenario", "HAPPY_PATH_SIMULATION"); result.put("observedAt", Instant.now().toString());
        result.put("windowFrom", Instant.now().minusSeconds(300).toString());
        result.put("inboxGroup", ops.groups.get("inventory-cell"));
        var inbox = events.get("order-cell").stream().filter(event -> stock.queryForObject(
                "SELECT count(*) FROM processed_event WHERE consumer_group=? AND event_id=?", Long.class,
                ops.groups.get("inventory-cell"), UUID.fromString(event)) > 0).toList();
        if (inbox.isEmpty()) throw new IllegalStateException("Known Inventory group commit not verified");
        result.put("inventoryInboxEventIds", inbox);
        return result;
    }

    private void commands() throws Exception {
        try (var pending = Files.list(run.resolve("commands"))) {
            for (var path : pending.filter(p -> p.getFileName().toString().matches("[0-9a-f-]{36}\\.request")).sorted().limit(1).toList()) {
                String name = path.getFileName().toString().replace(".request", "");
                Path response = run.resolve("commands/" + name + ".response");
                // Fixed command only, no URLs, SQL, payload, credential, or arbitrary execution.
                if (!Files.readString(path).equals("CREATE_HAPPY_ORDER")) write(response, Map.of("error", "INVALID_COMMAND"));
                else {
                    try {
                        var sample = createOrder(); samples.add(sample);
                        if (samples.size() > 16) samples.removeFirst();
                        write(response, sample); manifest();
                    } catch (Exception e) { write(response, Map.of("error", "ORDER_NOT_VERIFIED", "log", "order-cell.log")); }
                }
                Files.move(path, path.resolveSibling(name + ".handled"));
            }
        }
    }

    private synchronized void manifest() throws Exception {
        var children = new ArrayList<Map<String, Object>>();
        for (var e : processes.entrySet()) children.add(identity(e.getKey(), e.getValue().toHandle()));
        var data = new LinkedHashMap<String, Object>();
        data.put("phase", phase); data.put("mode", "Preview"); data.put("security", "NO_M36_AUTH_LOCAL_SYNTHETIC_DATA_ONLY");
        data.put("supervisor", identity("supervisor", ProcessHandle.current())); data.put("children", children);
        data.put("runDirectory", run.toString()); data.put("sourceDirectory", root.toString());
        data.put("url", "http://127.0.0.1:3000"); data.put("consolePort", 9190); data.put("nextPort", 3000);
        data.put("orderPort", orderPort); data.put("inventoryPort", inventoryPort); data.put("ownerGrpcPorts", ops.ports);
        data.put("databasePorts", databases.stream().map(EmbeddedPostgres::getPort).toList());
        data.put("databaseDirectories", java.util.stream.IntStream.range(0, databases.size())
                .mapToObj(i -> run.resolve("postgres-" + i).toString()).toList());
        // pg_ctl exits after startup on Windows; the postmaster can be reparented.
        // Its own disposable data directory supplies identity, never a shared process search.
        for (int i = 0; i < databases.size(); i++) {
            Path pidFile = run.resolve("postgres-" + i + "/postmaster.pid");
            if (Files.isRegularFile(pidFile)) {
                long pid = Long.parseLong(Files.readAllLines(pidFile).getFirst().trim());
                ProcessHandle.of(pid).ifPresent(p -> nativeChildren.putIfAbsent(pid, p));
            }
        }
        var nativeIdentities = new ArrayList<Map<String, Object>>();
        for (var p : nativeChildren.values()) nativeIdentities.add(identity("owned-postgres-postmaster", p));
        data.put("nativeChildren", nativeIdentities);
        data.put("brokers", brokerAddress);
        data.put("kafkaDataDirectory", run.resolve("kafka-data").toString());
        data.put("groups", ops.groups); data.put("orders", samples); data.put("at", Instant.now().toString());
        data.put("projection", "NONE_EPHEMERAL_OWNER_REREADS");
        if (ops.certs != null) data.put("ownedCertificateDirectory", ops.certs.directory.toString());
        write(run.resolve("manifest.json"), data);
    }
    private static Map<String, Object> identity(String name, ProcessHandle p) throws Exception {
        var info = p.info();
        String fingerprint = FINGERPRINTS.get(p.pid());
        if (fingerprint == null) {
            if (info.commandLine().isPresent()) fingerprint = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(info.commandLine().get().getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            else if (p.isAlive()) {
                // Windows JDK 21 may omit commandLine. Hash it IN the local CIM probe;
                // arguments (including fixture TLS credentials) never leave that process.
                String script = "$c=(Get-CimInstance Win32_Process -Filter 'ProcessId=" + p.pid() + "').CommandLine;"
                        + "if($c){$h=[Security.Cryptography.SHA256]::Create();"
                        + "[BitConverter]::ToString($h.ComputeHash([Text.Encoding]::UTF8.GetBytes($c))).Replace('-','').ToLowerInvariant()}";
                var probe = new ProcessBuilder(Path.of(System.getenv("SystemRoot"), "System32/WindowsPowerShell/v1.0/powershell.exe").toString(),
                        "-NoProfile", "-Command", script).redirectError(ProcessBuilder.Redirect.DISCARD).start();
                if (!probe.waitFor(5, java.util.concurrent.TimeUnit.SECONDS)) { probe.destroyForcibly(); throw new IllegalStateException("Ownership probe timed out"); }
                fingerprint = new String(probe.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).trim();
            }
            if (fingerprint == null || !fingerprint.matches("[a-f0-9]{64}")) fingerprint = "UNAVAILABLE";
            if (!fingerprint.equals("UNAVAILABLE")) FINGERPRINTS.put(p.pid(), fingerprint);
        }
        return Map.of("service", name, "pid", p.pid(), "startTime", info.startInstant().orElse(Instant.EPOCH).toString(),
                "commandSha256", fingerprint, "alive", p.isAlive());
    }
    private static void write(Path path, Object value) throws Exception {
        Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
        Files.writeString(tmp, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(value));
        Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }
    private synchronized void close() {
        if (closed) return; closed = true;
        var owned = new ArrayList<>(processes.values()); Collections.reverse(owned);
        for (var p : owned) try { stop(p); } catch (Exception ignored) { }
        for (var db : databases) try { db.close(); } catch (Exception ignored) { }
        if (ops.certs != null) try { ops.certs.close(); } catch (Exception ignored) { }
        if (http != null) http.close();
        if (!phase.equals("FAILED")) phase = "STOPPED";
        try { manifest(); } catch (Exception ignored) { }
        try { write(run.resolve("cleanup.json"), Map.of("at", Instant.now().toString(),
                "allRecordedChildrenStopped", processes.values().stream().noneMatch(Process::isAlive),
                "allObservedNativeChildrenStopped", nativeChildren.values().stream().noneMatch(ProcessHandle::isAlive))); }
        catch (Exception ignored) { }
    }
}
