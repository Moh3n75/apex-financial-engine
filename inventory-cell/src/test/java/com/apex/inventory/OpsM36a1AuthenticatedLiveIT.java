package com.apex.inventory;

import static com.apex.inventory.OrderInventorySagaIT.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import com.apex.inventory.m36a.DisposableM36aIdentity;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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
 * M36A.1 authenticated live: disposable Identity + cells + M36A Console + Next OIDC + Playwright.
 * Extends OPS-04B business assertions; never enables fixture mode.
 */
@Tag("integration")
class OpsM36a1AuthenticatedLiveIT {
  static final String REQUEST = Ops03dRecentDiscoveryIT.REQUEST;
  static final String RELEASE = Ops03dRecentDiscoveryIT.RELEASE;
  static final String PAYMENT = Ops03dRecentDiscoveryIT.PAYMENT;
  static final int CONSOLE_PORT = 9190;
  static final int NEXT_PORT = 3000;

  @Test
  void authenticatedBrowserOidcLiveAcceptance() throws Exception {
    Path root =
        Path.of(System.getProperty("saga.repositoryRoot", ".")).toAbsolutePath().normalize();
    Path inventoryJar =
        Path.of(
                System.getProperty(
                    "saga.inventoryJar",
                    root.resolve("out/m36a1/jars/inventory-cell.jar").toString()))
            .toAbsolutePath()
            .normalize();
    Path orderJar =
        Path.of(
                System.getProperty(
                    "saga.orderJar", root.resolve("out/m36a1/jars/order-cell.jar").toString()))
            .toAbsolutePath()
            .normalize();
    Path paymentJar =
        Path.of(
                System.getProperty(
                    "saga.paymentJar", root.resolve("out/m36a1/jars/payment-cell.jar").toString()))
            .toAbsolutePath()
            .normalize();
    Path consoleJar =
        Path.of(
                System.getProperty(
                    "saga.consoleJar",
                    root.resolve("out/m36a1/jars/apex-operations-console-1.0-SNAPSHOT.jar")
                        .toString()))
            .toAbsolutePath()
            .normalize();
    Path identityJar =
        Path.of(
                System.getProperty(
                    "saga.identityJar",
                    root.resolve("out/m36a1/jars/apex-identity-server.jar").toString()))
            .toAbsolutePath()
            .normalize();
    for (Path jar : List.of(orderJar, inventoryJar, paymentJar, consoleJar, identityJar)) {
      assertThat(jar).as(jar.toString()).isRegularFile();
      assertThat(Files.size(jar)).as(jar + " Boot fat jar").isGreaterThan(1_000_000L);
    }

    Path out = root.resolve("out/m36a1");
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
    var streams =
        List.of(ORDER_TOPIC, INVENTORY_TOPIC, REQUEST, RELEASE, PAYMENT, "apex.order.legacy.events.v1");
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
    summary.put("headNote", "M36A.1 authenticated live UI bridge");
    summary.put("consolePort", CONSOLE_PORT);
    summary.put("nextPort", NEXT_PORT);
    String sessionSecret = "m36a1-session-" + UUID.randomUUID() + "-pad-32chars-min";

    try (var identity =
            new DisposableM36aIdentity(
                identityJar,
                out.resolve("identity"),
                root.resolve("apex-identity-server/src/main/resources/db/migration"));
        var ownedCerts = new Ops03bCertificates();
        var orderDb = EmbeddedPostgres.builder().setConnectConfig("sslmode", "disable").start();
        var invDb = EmbeddedPostgres.builder().setConnectConfig("sslmode", "disable").start();
        var payDb = EmbeddedPostgres.builder().setConnectConfig("sslmode", "disable").start();
        var http = HttpClient.newHttpClient()) {
      identity.start();
      summary.put("identity", identity.publicDescriptor());
      // Secrets stay out of the public receipt — written only to an ephemeral owned file.
      Path secrets = out.resolve("identity-secrets.env");
      Files.writeString(
          secrets,
          "APEX_OIDC_CLIENT_SECRET="
              + identity.secret
              + "\nAPEX_OPS_M36A_PASSWORD="
              + identity.password
              + "\nAPEX_OPS_SESSION_SECRET="
              + sessionSecret
              + "\n");
      secrets.toFile().deleteOnExit();

      ops.certs = ownedCerts;
      ops.apiClient = http;
      try {
        broker.afterPropertiesSet();
        String brokers = broker.getBrokersAsString();
        String og = "order-m36a1-" + UUID.randomUUID();
        String ig = "inventory-m36a1-" + UUID.randomUUID();
        String pg = "payment-m36a1-" + UUID.randomUUID();
        int op = freePort();
        int ip = freePort();
        Path ol = out.resolve("live-order.log");
        Path il = out.resolve("live-inventory.log");
        Path pl = out.resolve("live-payment.log");

        order =
            ops.launch(
                orderJar,
                orderDb,
                brokers,
                og,
                op,
                ol,
                ORDER_TOPIC,
                INVENTORY_TOPIC + "," + PAYMENT,
                "--spring.profiles.active=saga,three-cell-saga");
        inventory =
            ops.launch(
                inventoryJar,
                invDb,
                brokers,
                ig,
                ip,
                il,
                INVENTORY_TOPIC,
                ORDER_TOPIC + "," + RELEASE,
                "--spring.profiles.active=saga,three-cell-saga",
                "--apex.messaging.consumer.reliability.max-attempts=10");
        payment =
            ops.launch(
                paymentJar,
                payDb,
                brokers,
                pg,
                0,
                pl,
                PAYMENT,
                REQUEST,
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
          Ops04bLiveUiBridgeIT.applyRecentIndexes(ops, jdbc);
        }

        startM36aConsole(ops, ownedCerts.primary, "m32-client", CONSOLE_PORT, consoleJar, identity);
        String bearer = identity.mintUserAccessToken();
        assertThat(apiBearer(http, CONSOLE_PORT, "/ops/v1/recent/search/sources", bearer))
            .contains("OUTBOX_EVENT");
        assertConsoleNegatives(http, CONSOLE_PORT, bearer, identity);

        UUID product = UUID.randomUUID();
        stock.update("INSERT INTO inventory_stock VALUES (?,10,0)", product);
        UUID orderId = UUID.fromString(post(http, op, List.of(item(product, 2))).path("orderId").asText());
        pay.update("INSERT INTO payment_simulation_policy VALUES (?,'AUTHORIZED')", orderId);
        awaitStatus(orders, orderId, "CONFIRMED");
        assertThat(getStatus(http, op, orderId)).isEqualTo("CONFIRMED");

        List<UUID> orderEvents =
            orders.queryForList(
                "SELECT event_id FROM outbox_event WHERE aggregate_id=? ORDER BY created_at",
                UUID.class,
                orderId);
        assertThat(orderEvents).isNotEmpty();
        assertThat(orders.queryForObject("SELECT status FROM orders WHERE id=?", String.class, orderId))
            .isEqualTo("CONFIRMED");
        assertThat(
                stock.queryForObject(
                    "SELECT status FROM inventory_reservations WHERE order_id=?",
                    String.class,
                    orderId))
            .isEqualTo("RESERVED");
        assertThat(
                pay.queryForObject(
                    "SELECT status FROM payment_authorizations WHERE order_id=?",
                    String.class,
                    orderId))
            .isEqualTo("AUTHORIZED");

        await()
            .pollInterval(Duration.ofMillis(2100))
            .atMost(Duration.ofSeconds(90))
            .untilAsserted(
                () -> {
                  String body =
                      apiBearer(
                          http,
                          CONSOLE_PORT,
                          searchPath("/ops/v1/recent/events", "order-cell", "owner", windowStart),
                          bearer);
                  assertThat(body).contains(orderEvents.get(0).toString());
                });
        Thread.sleep(3000L);
        Instant windowTo = Instant.now().plusSeconds(30);

        Map<String, Object> hs =
            Ops04bLiveUiBridgeIT.baseHandshake(windowStart, windowTo, orderId, orderEvents, ig);
        hs.put("phase", "ready");
        hs.put("m36a", true);
        hs.put("issuer", identity.issuer);
        hs.put("loginName", "alice");
        writeJson(handshake, hs);
        summary.put("orderId", orderId.toString());
        summary.put(
            "sqlCrossCheck",
            Map.of("order", "CONFIRMED", "inventory", "RESERVED", "payment", "AUTHORIZED"));

        next =
            startNextM36a(
                root,
                CONSOLE_PORT,
                NEXT_PORT,
                out.resolve("live-next.log"),
                identity,
                sessionSecret);
        Process nextRef = next;
        await()
            .pollInterval(Duration.ofSeconds(1))
            .ignoreExceptions()
            .atMost(Duration.ofSeconds(120))
            .untilAsserted(
                () -> {
                  assertThat(nextRef.isAlive()).isTrue();
                  var conn =
                      (java.net.HttpURLConnection)
                          URI.create("http://127.0.0.1:" + NEXT_PORT + "/login").toURL().openConnection();
                  conn.setConnectTimeout(3000);
                  conn.setReadTimeout(3000);
                  assertThat(conn.getResponseCode()).isLessThan(500);
                  conn.disconnect();
                });

        int unauth =
            runPlaywright(
                root,
                handshake,
                "e2e/m36a-unauth.spec.ts",
                out.resolve("playwright-unauth.log"),
                "playwright.m36a.config.ts",
                identity.password,
                true);
        summary.put("playwrightUnauthExit", unauth);
        assertThat(unauth).as("m36a-unauth").isZero();

        int happy =
            runPlaywright(
                root,
                handshake,
                "e2e/m36a-live-happy.spec.ts",
                out.resolve("playwright-happy.log"),
                "playwright.m36a-live.config.ts",
                identity.password,
                true);
        summary.put("playwrightHappyExit", happy);
        assertThat(happy).as("m36a-live-happy").isZero();

        stop(payment);
        payment = null;
        hs.put("phase", "payment-stopped");
        hs.put("windowTo", Instant.now().plusSeconds(5).toString());
        writeJson(handshake, hs);
        Thread.sleep(15_000L);
        int stale =
            runPlaywright(
                root,
                handshake,
                "e2e/m36a-live-stale.spec.ts",
                out.resolve("playwright-stale.log"),
                "playwright.m36a-live.config.ts",
                identity.password,
                true);
        summary.put("playwrightStaleExit", stale);
        assertThat(stale).as("m36a-live-stale").isZero();

        // Independent business admission after Console/Next remain up but payment was stopped then
        // we already asserted STALE; restart payment and admit a new order without UI.
        payment =
            ops.launch(
                paymentJar,
                payDb,
                brokers,
                pg,
                0,
                out.resolve("live-payment-recovered.log"),
                PAYMENT,
                REQUEST,
                "--spring.profiles.active=three-cell-saga,simulation",
                "--apex.messaging.consumer.reliability.max-attempts=10");
        Ops03dRecentDiscoveryIT.paymentReady(payment, out.resolve("live-payment-recovered.log"));
        UUID product2 = UUID.randomUUID();
        stock.update("INSERT INTO inventory_stock VALUES (?,5,0)", product2);
        UUID after =
            UUID.fromString(post(http, op, List.of(item(product2, 1))).path("orderId").asText());
        pay.update("INSERT INTO payment_simulation_policy VALUES (?,'AUTHORIZED')", after);
        awaitStatus(orders, after, "CONFIRMED");
        summary.put("postOutageOrderId", after.toString());

        try (var admin = Admin.create(Map.of("bootstrap.servers", brokers))) {
          Ops03dRecentDiscoveryIT.assertNoDlt(admin, streams);
        }
        summary.put("verdict", "AUTHENTICATED_LIVE_E2E_PASS");
        Files.deleteIfExists(secrets);
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
                  if (ops.certs != null) ops.certs.close();
                  Files.writeString(
                      out.resolve("cleanup.json"),
                      JSON.writeValueAsString(
                          Map.of(
                              "children",
                              ops.children.stream().map(Process::pid).toList(),
                              "identityPid",
                              identity.pid(),
                              "allStopped",
                              ops.children.stream().noneMatch(Process::isAlive),
                              "at",
                              Instant.now().toString())));
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

  static void assertConsoleNegatives(
      HttpClient http, int port, String validBearer, DisposableM36aIdentity identity)
      throws Exception {
    Map<String, Object> matrix = new LinkedHashMap<>();
    // Anonymous
    var anon =
        http.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/ops/v1/overview"))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertThat(anon.statusCode()).isEqualTo(401);
    matrix.put("anonymous", anon.statusCode());
    // Wrong audience token is hard without another client; forged bearer
    var forged =
        http.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/ops/v1/overview"))
                .header("Authorization", "Bearer not-a-jwt")
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertThat(forged.statusCode()).isEqualTo(401);
    matrix.put("forgedBearer", forged.statusCode());
    // X-User / X-Tenant must not grant access
    var spoof =
        http.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/ops/v1/overview"))
                .header("X-User", "alice")
                .header("X-Tenant", identity.tenant.toString())
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertThat(spoof.statusCode()).isEqualTo(401);
    matrix.put("spoofedHeaders", spoof.statusCode());
    // Mutating method
    var post =
        http.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/ops/v1/overview"))
                .header("Authorization", "Bearer " + validBearer)
                .timeout(Duration.ofSeconds(5))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build(),
            HttpResponse.BodyHandlers.ofString());
    // M36A filter chain denyAll → 403; ReadOnlyFilter may yield 405. Either denies before data.
    assertThat(post.statusCode()).isIn(403, 405);
    matrix.put("postDenied", post.statusCode());
    // Valid bearer still works
    assertThat(apiBearer(http, port, "/ops/v1/overview", validBearer)).isNotBlank();
    matrix.put("validBearer", 200);
    Files.writeString(
        Path.of(System.getProperty("saga.repositoryRoot", "."))
            .toAbsolutePath()
            .resolve("out/m36a1/console-security-matrix.json"),
        JSON.writerWithDefaultPrettyPrinter().writeValueAsString(matrix));
  }

  static void startM36aConsole(
      Ops03dRecentDiscoveryIT ops,
      Path keys,
      String identityCell,
      int port,
      Path consoleJar,
      DisposableM36aIdentity id)
      throws Exception {
    ops.consolePort = port;
    var grants = new ArrayList<String>();
    for (String cell : ops.ports.keySet()) {
      grants.add(
          cell
              + "/WORKFLOW/"
              + (cell.equals("order-cell")
                  ? "order"
                  : cell.equals("inventory-cell") ? "reservation" : "authorization-simulation"));
      grants.add(cell + "/OUTBOX_EVENT/owner");
      grants.add(cell + "/KNOWN_GROUP_INBOX/" + ops.groups.get(cell));
    }
    var args =
        new ArrayList<>(
            List.of(
                "--server.port=" + port,
                "--server.address=127.0.0.1",
                "--apex.grpc.cell-id=" + identityCell,
                "--apex.grpc.client.enabled=true",
                "--apex.ops03d.search-enabled=true",
                "--apex.ops03d.environment=test",
                "--apex.ops03d.sources=" + String.join(",", grants),
                "--apex.ops.m36a.enabled=true",
                "--apex.ops.m36a.issuer=" + id.issuer,
                "--apex.ops.m36a.audience=" + DisposableM36aIdentity.AUDIENCE,
                "--apex.ops.m36a.required-scope=ops.console.read",
                "--apex.ops.m36a.tenant-id=" + id.tenant));
    for (var entry : ops.ports.entrySet()) {
      args.addAll(
          List.of(
              "--apex.grpc.destinations." + entry.getKey() + ".channel=" + entry.getKey(),
              "--spring.grpc.client.channel."
                  + entry.getKey()
                  + ".target=static://127.0.0.1:"
                  + entry.getValue(),
              "--spring.grpc.client.channel." + entry.getKey() + ".ssl.enabled=true",
              "--spring.grpc.client.channel." + entry.getKey() + ".ssl.bundle=m32"));
    }
    args.addAll(ops.certs.bundle(keys));
    var command =
        new ArrayList<>(
            List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx256m",
                "-jar",
                consoleJar.toString()));
    command.addAll(args);
    var builder =
        new ProcessBuilder(command)
            .redirectErrorStream(true)
            .redirectOutput(ops.root.resolve("out/m36a1/console.log").toFile());
    builder
        .environment()
        .keySet()
        .removeIf(
            k ->
                k.matches(
                    "(?i)(SPRING_.*|APEX_.*|CELL_.*|SERVER_.*|JAVA_TOOL_OPTIONS|JDK_JAVA_OPTIONS|_JAVA_OPTIONS)"));
    ops.console = builder.start();
    ops.children.add(ops.console);
    Path consoleLog = ops.root.resolve("out/m36a1/console.log");
    await()
        .ignoreExceptions()
        .atMost(Duration.ofSeconds(90))
        .untilAsserted(
            () -> {
              assertThat(ops.console.isAlive()).isTrue();
              assertThat(Files.readString(consoleLog))
                  .contains("Started ConsoleApplication");
              // Security filter is up: anonymous must be 401 (not connection refused).
              var probe =
                  HttpClient.newHttpClient()
                      .send(
                          HttpRequest.newBuilder(
                                  URI.create("http://127.0.0.1:" + port + "/ops/v1/overview"))
                              .timeout(Duration.ofSeconds(3))
                              .GET()
                              .build(),
                          HttpResponse.BodyHandlers.ofString());
              assertThat(probe.statusCode()).isEqualTo(401);
            });
  }

  static Process startNextM36a(
      Path root,
      int consolePort,
      int nextPort,
      Path log,
      DisposableM36aIdentity id,
      String sessionSecret)
      throws Exception {
    Path web = root.resolve("apps/operations-web");
    Path nextJs = web.resolve("node_modules/next/dist/bin/next");
    assertThat(nextJs).isRegularFile();
    Path node = Ops04bLiveUiBridgeIT.resolveNode();
    var command =
        List.of(
            node.toString(),
            nextJs.toString(),
            "start",
            "-H",
            "127.0.0.1",
            "-p",
            Integer.toString(nextPort));
    var builder =
        new ProcessBuilder(command)
            .directory(web.toFile())
            .redirectErrorStream(true)
            .redirectOutput(log.toFile());
    var env = builder.environment();
    env.put("APEX_OPERATIONS_API_ORIGIN", "http://127.0.0.1:" + consolePort);
    env.put("OPS04B_REQUIRE_LIVE", "true");
    env.put("APEX_OPS_UI_FIXTURE_MODE", "false");
    env.put("APEX_OPS_M36A_ENABLED", "true");
    env.put("APEX_OIDC_ISSUER", id.issuer);
    env.put("APEX_OIDC_CLIENT_ID", DisposableM36aIdentity.CLIENT_ID);
    env.put("APEX_OIDC_CLIENT_SECRET", id.secret);
    env.put("APEX_OIDC_REDIRECT_URI", DisposableM36aIdentity.OPS_REDIRECT);
    env.put("APEX_OIDC_SCOPES", "openid profile ops.console.read");
    env.put("APEX_OIDC_AUDIENCE", DisposableM36aIdentity.AUDIENCE);
    env.put("APEX_OPS_SESSION_SECRET", sessionSecret);
    env.put("APEX_OPS_COOKIE_SECURE", "false");
    Ops04bLiveUiBridgeIT.prependLocalNode(env);
    return builder.start();
  }

  static int runPlaywright(
      Path root,
      Path handshake,
      String spec,
      Path log,
      String config,
      String password,
      boolean m36a)
      throws Exception {
    Path web = root.resolve("apps/operations-web");
    Path pwCmd = web.resolve("node_modules/.bin/playwright.cmd");
    List<String> command;
    if (Files.isRegularFile(pwCmd)) {
      command = List.of(pwCmd.toString(), "test", "-c", config, spec);
    } else {
      command =
          List.of(
              "cmd.exe",
              "/c",
              Ops04bLiveUiBridgeIT.resolvePnpm(),
              "exec",
              "playwright",
              "test",
              "-c",
              config,
              spec);
    }
    var builder =
        new ProcessBuilder(command)
            .directory(web.toFile())
            .redirectErrorStream(true)
            .redirectOutput(log.toFile());
    var env = builder.environment();
    env.put("OPS04B_HANDSHAKE", handshake.toAbsolutePath().toString());
    env.put("OPS04B_LIVE", "1");
    if(System.getProperty("controlcenter.membership.expected")!=null)env.put("APEX_MEMBERSHIP_EXPECTED",System.getProperty("controlcenter.membership.expected"));
    env.put("OPS04B_NEXT_ORIGIN", "http://127.0.0.1:" + NEXT_PORT);
    env.put("OPS04B_REQUIRE_LIVE", "true");
    env.put("APEX_OPS_UI_FIXTURE_MODE", "false");
    if (m36a) {
      env.put("APEX_OPS_M36A_E2E", "true");
      env.put("APEX_OPS_M36A_LOGIN", "alice");
      env.put("APEX_OPS_M36A_PASSWORD", password);
    }
    Ops04bLiveUiBridgeIT.prependLocalNode(env);
    Process process = builder.start();
    assertThat(process.waitFor(12, TimeUnit.MINUTES)).as("playwright " + spec).isTrue();
    return process.exitValue();
  }

  static String apiBearer(HttpClient http, int port, String path, String bearer) throws Exception {
    return http.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .header("Authorization", "Bearer " + bearer)
                .timeout(Duration.ofSeconds(8))
                .GET()
                .build(),
            HttpResponse.BodyHandlers.ofString())
        .body();
  }

  static String searchPath(String route, String owner, String scope, Instant from) {
    return route
        + "?environment=test&owner="
        + owner
        + "&scope="
        + scope
        + "&from="
        + from
        + "&to="
        + Instant.now()
        + "&limit=16";
  }

  static void assertLoopbackPortFree(int port) throws Exception {
    try (ServerSocket socket = new ServerSocket()) {
      socket.setReuseAddress(false);
      socket.bind(new InetSocketAddress("127.0.0.1", port));
    } catch (Exception e) {
      throw new AssertionError(
          "Loopback port "
              + port
              + " is busy; M36A.1 refuses to kill unrelated processes. Free the port and retry.",
          e);
    }
  }

  static void writeJson(Path path, Object value) throws Exception {
    Files.createDirectories(path.getParent());
    Files.writeString(
        path,
        JSON.writerWithDefaultPrettyPrinter().writeValueAsString(value),
        StandardCharsets.UTF_8);
  }
}
