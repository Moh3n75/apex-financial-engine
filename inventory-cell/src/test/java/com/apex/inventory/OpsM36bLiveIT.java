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
 * M36B live matrix: resource grants, JWKS rotation, owner ACL under JWT, 2×Next session continuity,
 * OIDC negatives, privacy canaries. LOCAL_RESTRICTED_ONLY.
 */
@Tag("integration")
class OpsM36bLiveIT {
  static final String REQUEST = Ops03dRecentDiscoveryIT.REQUEST;
  static final String RELEASE = Ops03dRecentDiscoveryIT.RELEASE;
  static final String PAYMENT = Ops03dRecentDiscoveryIT.PAYMENT;
  static final int CONSOLE_PORT = 9190;
  static final int NEXT_A = 3000;
  static final int NEXT_B = 3001;

  @Test
  void m36bLocalRestrictedLiveMatrix() throws Exception {
    Path root =
        Path.of(System.getProperty("saga.repositoryRoot", ".")).toAbsolutePath().normalize();
    Path inventoryJar = jar(root, "inventory-cell.jar");
    Path orderJar = jar(root, "order-cell.jar");
    Path paymentJar = jar(root, "payment-cell.jar");
    Path consoleJar = jar(root, "apex-operations-console-1.0-SNAPSHOT.jar");
    Path identityJar = jar(root, "apex-identity-server.jar");
    for (Path j : List.of(orderJar, inventoryJar, paymentJar, consoleJar, identityJar)) {
      assertThat(j).isRegularFile();
      assertThat(Files.size(j)).isGreaterThan(1_000_000L);
    }

    Path out = root.resolve("out/m36b");
    Files.createDirectories(out);
    Path handshake = out.resolve("live-handshake.json");
    Path receipt = out.resolve("live-matrix-receipt.json");
    Files.deleteIfExists(handshake);
    Files.deleteIfExists(out.resolve("live-done.flag"));

    assertLoopbackPortFree(CONSOLE_PORT);
    assertLoopbackPortFree(NEXT_A);
    assertLoopbackPortFree(NEXT_B);

    var ops = new Ops03dRecentDiscoveryIT();
    ops.root = root;
    Process order = null, inventory = null, payment = null, nextA = null, nextB = null, nextBad = null;
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
    Map<String, Object> matrix = new LinkedHashMap<>();
    matrix.put("milestone", "M36B");
    matrix.put("safetyBoundary", "LOCAL_RESTRICTED_ONLY");
    String sessionSecret = "m36b-session-" + UUID.randomUUID() + "-pad-32chars-min";
    String wrongSecret = "m36b-WRONG-" + UUID.randomUUID() + "-pad-32chars-min!!";

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
      matrix.put("identity", identity.publicDescriptor());
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
        String og = "order-m36b-" + UUID.randomUUID();
        String ig = "inventory-m36b-" + UUID.randomUUID();
        String pg = "payment-m36b-" + UUID.randomUUID();
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

        startM36bConsole(
            ops, ownedCerts.primary, "m32-client", CONSOLE_PORT, consoleJar, identity, out);

        String aliceBearer = identity.mintUserAccessToken();
        String bobBearer = identity.mintBobAccessToken();
        String kidBefore = identity.activeKid;

        // --- Resource scope positive (alice → order-cell) ---
        UUID product = UUID.randomUUID();
        stock.update("INSERT INTO inventory_stock VALUES (?,10,0)", product);
        UUID orderId =
            UUID.fromString(post(http, op, List.of(item(product, 2))).path("orderId").asText());
        pay.update("INSERT INTO payment_simulation_policy VALUES (?,'AUTHORIZED')", orderId);
        awaitStatus(orders, orderId, "CONFIRMED");
        List<UUID> orderEvents =
            orders.queryForList(
                "SELECT event_id FROM outbox_event WHERE aggregate_id=? ORDER BY created_at",
                UUID.class,
                orderId);
        assertThat(orderEvents).isNotEmpty();
        final String aliceTokenForEvents = aliceBearer;
        await()
            .pollInterval(Duration.ofMillis(2100))
            .atMost(Duration.ofSeconds(90))
            .untilAsserted(
                () -> {
                  var res =
                      statusBearer(
                          http,
                          CONSOLE_PORT,
                          searchPath("/ops/v1/recent/events", "order-cell", "owner", windowStart),
                          aliceTokenForEvents);
                  assertThat(res.status()).isEqualTo(200);
                  assertThat(res.body()).contains(orderEvents.get(0).toString());
                });
        recordGate(matrix, "RESOURCE_SCOPE_POSITIVE", "PASS", "alice order-cell events matched SQL");

        // --- Resource scope negative (bob + payment-cell + unknown) ---
        int bobDenied =
            statusBearer(
                    http,
                    CONSOLE_PORT,
                    searchPath("/ops/v1/recent/events", "order-cell", "owner", windowStart),
                    bobBearer)
                .status();
        int alicePaymentDenied =
            statusBearer(
                    http,
                    CONSOLE_PORT,
                    searchPath("/ops/v1/recent/events", "payment-cell", "owner", windowStart),
                    aliceBearer)
                .status();
        int unknownOwner =
            statusBearer(
                    http,
                    CONSOLE_PORT,
                    searchPath("/ops/v1/recent/events", "no-such-cell", "owner", windowStart),
                    aliceBearer)
                .status();
        assertThat(bobDenied).isEqualTo(403);
        assertThat(alicePaymentDenied).isEqualTo(403);
        assertThat(unknownOwner).isEqualTo(403);
        // Grant inventory is config-backed (sources), not snapshot-poller readiness.
        String sourcesAlice =
            apiBearer(http, CONSOLE_PORT, "/ops/v1/recent/search/sources", aliceBearer);
        assertThat(sourcesAlice).contains("order-cell");
        assertThat(sourcesAlice).contains("inventory-cell");
        assertThat(sourcesAlice).doesNotContain("payment-cell");
        String sourcesBob =
            apiBearer(http, CONSOLE_PORT, "/ops/v1/recent/search/sources", bobBearer);
        assertThat(sourcesBob).isEqualTo("[]");
        String overviewAlice = apiBearer(http, CONSOLE_PORT, "/ops/v1/overview", aliceBearer);
        assertThat(overviewAlice).doesNotContain("payment-cell");
        String overviewBob = apiBearer(http, CONSOLE_PORT, "/ops/v1/overview", bobBearer);
        assertThat(overviewBob).doesNotContain("order-cell");
        assertThat(overviewBob).doesNotContain("payment-cell");
        recordGate(
            matrix,
            "RESOURCE_SCOPE_NEGATIVE",
            "PASS",
            Map.of(
                "bobOrder", bobDenied,
                "alicePayment", alicePaymentDenied,
                "unknownOwner", unknownOwner,
                "sourcesAliceFiltered", true,
                "sourcesBobEmpty", true));

        // --- No tenant fiction ---
        int tenantFilter =
            statusBearer(
                    http,
                    CONSOLE_PORT,
                    searchPath("/ops/v1/recent/events", "order-cell", "owner", windowStart)
                        + "&tenantFilter="
                        + identity.tenant,
                    aliceBearer)
                .status();
        assertThat(tenantFilter).isEqualTo(403);
        recordGate(matrix, "NO_TENANT_FICTION", "PASS", "tenantFilter=403");
        recordGate(
            matrix,
            "TENANT_ROW_ISOLATION",
            "BLOCKED",
            "owner SQL lacks trustworthy cross-kind tenant predicates");

        // --- Audience/claims negatives ---
        Map<String, Object> claims = new LinkedHashMap<>();
        claims.put("anonymous", statusGet(http, CONSOLE_PORT, "/ops/v1/overview").status());
        claims.put(
            "forged",
            statusBearer(http, CONSOLE_PORT, "/ops/v1/overview", "not-a-jwt").status());
        claims.put(
            "spoofHeaders",
            statusGet(
                    http,
                    CONSOLE_PORT,
                    "/ops/v1/overview",
                    "X-User",
                    "alice",
                    "X-Tenant",
                    identity.tenant.toString())
                .status());
        assertThat((Integer) claims.get("anonymous")).isEqualTo(401);
        assertThat((Integer) claims.get("forged")).isEqualTo(401);
        assertThat((Integer) claims.get("spoofHeaders")).isEqualTo(401);
        recordGate(matrix, "AUDIENCE_CLAIMS", "PASS", claims);

        // --- JWKS real rotation (Console stays up) ---
        assertThat(
                statusBearer(http, CONSOLE_PORT, "/ops/v1/overview", aliceBearer).status())
            .isEqualTo(200);
        identity.rotateToNewSigningKey("b");
        String newBearer = identity.mintUserAccessToken();
        await()
            .pollInterval(Duration.ofSeconds(2))
            .atMost(Duration.ofSeconds(90))
            .untilAsserted(
                () ->
                    assertThat(
                            statusBearer(http, CONSOLE_PORT, "/ops/v1/overview", newBearer)
                                .status())
                        .isEqualTo(200));
        identity.publishActiveOnlyJwks();
        String retiredSigned =
            identity.mintOfflineAccessToken(
                identity.retiredSigningKey, identity.user, Duration.ofMinutes(5));
        await()
            .pollInterval(Duration.ofSeconds(2))
            .atMost(Duration.ofSeconds(90))
            .untilAsserted(
                () ->
                    assertThat(
                            statusBearer(http, CONSOLE_PORT, "/ops/v1/overview", retiredSigned)
                                .status())
                        .isEqualTo(401));
        assertThat(statusBearer(http, CONSOLE_PORT, "/ops/v1/overview", newBearer).status())
            .isEqualTo(200);
        recordGate(
            matrix,
            "JWKS_ROTATION",
            "PASS",
            Map.of(
                "kidBefore",
                kidBefore,
                "kidAfter",
                identity.activeKid,
                "newKidAccepted",
                true,
                "retiredKidRejected",
                true,
                "jwksCacheTtlMs",
                15_000,
                "immediateRevocation",
                "NOT_SUPPORTED"));
        aliceBearer = newBearer;

        // --- Owner ACL independent of human JWT (wrong SAN) ---
        stop(ops.console);
        ops.console = null;
        startM36bConsole(
            ops, ownedCerts.wrongSan, "m32-client", CONSOLE_PORT, consoleJar, identity, out);
        var wrongSanView =
            statusBearer(
                http,
                CONSOLE_PORT,
                searchPath("/ops/v1/recent/events", "order-cell", "owner", windowStart),
                aliceBearer);
        assertThat(wrongSanView.status()).isEqualTo(200);
        assertThat(wrongSanView.body())
            .matches("(?s).*(PERMISSION_GAP|SOURCE_UNAVAILABLE|MISSING).*");
        assertThat(wrongSanView.body()).doesNotContain(orderEvents.get(0).toString());
        recordGate(
            matrix,
            "OWNER_ACL_WITH_HUMAN_AUTH",
            "PASS",
            "valid JWT + wrong SAN → no event disclosure");

        // Restore good Console peer for browser path
        stop(ops.console);
        ops.console = null;
        startM36bConsole(
            ops, ownedCerts.primary, "m32-client", CONSOLE_PORT, consoleJar, identity, out);

        // Observer peer lacks RECENT_SEARCH contract
        stop(ops.console);
        ops.console = null;
        startM36bConsole(
            ops, ownedCerts.observer, "m32-observer", CONSOLE_PORT, consoleJar, identity, out);
        var observerView =
            statusBearer(
                http,
                CONSOLE_PORT,
                searchPath("/ops/v1/recent/events", "order-cell", "owner", windowStart),
                aliceBearer);
        assertThat(observerView.status()).isEqualTo(200);
        assertThat(observerView.body()).contains("PERMISSION_GAP");
        stop(ops.console);
        ops.console = null;
        startM36bConsole(
            ops, ownedCerts.primary, "m32-client", CONSOLE_PORT, consoleJar, identity, out);

        Instant windowTo = Instant.now().plusSeconds(30);
        Map<String, Object> hs =
            Ops04bLiveUiBridgeIT.baseHandshake(windowStart, windowTo, orderId, orderEvents, ig);
        hs.put("phase", "ready");
        hs.put("m36b", true);
        hs.put("issuer", identity.issuer);
        hs.put("loginName", "alice");
        hs.put("nextA", NEXT_A);
        hs.put("nextB", NEXT_B);
        writeJson(handshake, hs);
        matrix.put("orderId", orderId.toString());

        String grantsJson =
            resourceGrantsJson(identity.user.toString(), ig);

        nextA =
            startNextM36b(
                root,
                CONSOLE_PORT,
                NEXT_A,
                out.resolve("live-next-a.log"),
                identity,
                sessionSecret,
                grantsJson);
        nextB =
            startNextM36b(
                root,
                CONSOLE_PORT,
                NEXT_B,
                out.resolve("live-next-b.log"),
                identity,
                sessionSecret,
                grantsJson);
        awaitReady(nextA, NEXT_A);
        awaitReady(nextB, NEXT_B);

        int continuity =
            runPlaywright(
                root,
                handshake,
                "e2e/m36b-session-continuity.spec.ts",
                out.resolve("playwright-session.log"),
                "playwright.m36b.config.ts",
                identity.password,
                NEXT_A,
                NEXT_B);
        matrix.put("playwrightSessionExit", continuity);
        recordGate(
            matrix,
            "SESSION_CONTINUITY",
            continuity == 0 ? "PASS" : "FAIL",
            "2x Next shared sealing key");
        assertThat(continuity).as("SESSION_CONTINUITY playwright").isZero();

        int oidcNeg =
            runPlaywright(
                root,
                handshake,
                "e2e/m36b-oidc-negatives.spec.ts",
                out.resolve("playwright-oidc-neg.log"),
                "playwright.m36b.config.ts",
                identity.password,
                NEXT_A,
                NEXT_B);
        matrix.put("playwrightOidcNegExit", oidcNeg);
        recordGate(
            matrix,
            "OIDC_NEGATIVES",
            oidcNeg == 0 ? "PASS" : "FAIL",
            "live browser/Identity negatives executed");

        int resourceBrowser =
            runPlaywright(
                root,
                handshake,
                "e2e/m36b-resource-browser.spec.ts",
                out.resolve("playwright-resource.log"),
                "playwright.m36b.config.ts",
                identity.password,
                NEXT_A,
                NEXT_B);
        matrix.put("playwrightResourceExit", resourceBrowser);
        assertThat(resourceBrowser).isZero();

        // Wrong session key on a third ephemeral Next → deny
        int nextBadPort = freePort();
        nextBad =
            startNextM36b(
                root,
                CONSOLE_PORT,
                nextBadPort,
                out.resolve("live-next-wrong-key.log"),
                identity,
                wrongSecret,
                grantsJson);
        awaitReady(nextBad, nextBadPort);
        // Cookie from A cannot be decrypted by wrong-key instance — probe via Playwright helper env
        hs.put("nextWrongKey", nextBadPort);
        writeJson(handshake, hs);
        int wrongKey =
            runPlaywright(
                root,
                handshake,
                "e2e/m36b-wrong-session-key.spec.ts",
                out.resolve("playwright-wrong-key.log"),
                "playwright.m36b.config.ts",
                identity.password,
                NEXT_A,
                nextBadPort);
        matrix.put("playwrightWrongKeyExit", wrongKey);
        assertThat(wrongKey).isZero();
        stopQuiet(nextBad);
        nextBad = null;

        // Revocation semantics: document TTL; logout clears cookie only
        recordGate(
            matrix,
            "REVOCATION_SEMANTICS",
            "PASS",
            Map.of(
                "immediateJwtRevoke",
                "NOT_SUPPORTED",
                "maxStalePermissionExposure",
                "access JWT remaining TTL (fixture ≤5m)",
                "logout",
                "clears BFF cookie; Identity end_session best-effort"));

        // Privacy canary: Playwright specs already assert no JWT/secret leak in HTML/network bodies.
        // Additional log scan of Next A stdout for secret material (no HTTP dependency on tired Node).
        String nextALog = Files.readString(out.resolve("live-next-a.log"));
        assertThat(nextALog).doesNotContain(identity.secret);
        assertThat(nextALog).doesNotContain(sessionSecret);
        assertThat(nextALog.toLowerCase()).doesNotContain("client_secret=" + identity.secret);
        recordGate(
            matrix,
            "AUDIT_PRIVACY",
            "PASS",
            Map.of(
                "browserHtmlCanary",
                "playwright assertNoTokenLeakInHtml",
                "nextLogSecretScan",
                true,
                "durableAudit",
                "BLOCKED"));
        recordGate(matrix, "DURABLE_AUDIT", "BLOCKED", "no Business Cell TX audit sink");
        recordGate(
            matrix,
            "RESTRICTED_HTTPS_EDGE",
            "NOT_VERIFIED",
            "loopback HTTP retained; no disposable TLS edge this run");
        recordGate(matrix, "PUBLIC_INGRESS", "BLOCKED", "not in scope");
        recordGate(matrix, "GENERAL_MULTI_TENANT_ISOLATION", "BLOCKED", "see TENANT_ROW_ISOLATION");
        recordGate(matrix, "IMMEDIATE_REVOCATION", "NOT_SUPPORTED", "expiry-bound only");
        recordGate(matrix, "OPERATIONS_CONTROL_PLANE", "BLOCKED", "read-only ops preserved");

        try (var admin = Admin.create(Map.of("bootstrap.servers", brokers))) {
          Ops03dRecentDiscoveryIT.assertNoDlt(admin, streams);
        }

        boolean mandatory =
            "PASS".equals(gateStatus(matrix, "RESOURCE_SCOPE_POSITIVE"))
                && "PASS".equals(gateStatus(matrix, "RESOURCE_SCOPE_NEGATIVE"))
                && "PASS".equals(gateStatus(matrix, "SESSION_CONTINUITY"))
                && "PASS".equals(gateStatus(matrix, "JWKS_ROTATION"))
                && "PASS".equals(gateStatus(matrix, "OWNER_ACL_WITH_HUMAN_AUTH"))
                && "PASS".equals(gateStatus(matrix, "OIDC_NEGATIVES"))
                && "PASS".equals(gateStatus(matrix, "AUDIT_PRIVACY"))
                && continuity == 0
                && oidcNeg == 0
                && wrongKey == 0;
        matrix.put(
            "verdict",
            mandatory ? "M36B_LOCAL_RESTRICTED_ACCEPTED_PENDING_FULL" : "M36B_PARTIAL");
        matrix.put(
            "note",
            "COMBINED_FULL_REGRESSION must be proven separately on frozen source before final ACCEPT");
        Files.deleteIfExists(secrets);
      } finally {
        stopQuiet(nextBad);
        stopQuiet(nextB);
        stopQuiet(nextA);
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
              }
            }
          }
        }
      }
    } finally {
      writeJson(receipt, matrix);
      Files.writeString(out.resolve("live-done.flag"), Instant.now().toString());
    }
  }

  static Path jar(Path root, String name) {
    String prop =
        switch (name) {
          case "inventory-cell.jar" -> "saga.inventoryJar";
          case "order-cell.jar" -> "saga.orderJar";
          case "payment-cell.jar" -> "saga.paymentJar";
          case "apex-operations-console-1.0-SNAPSHOT.jar" -> "saga.consoleJar";
          case "apex-identity-server.jar" -> "saga.identityJar";
          default -> "saga.jar." + name;
        };
    return Path.of(System.getProperty(prop, root.resolve("out/m36b/jars/" + name).toString()))
        .toAbsolutePath()
        .normalize();
  }

  static String resourceGrantsJson(String aliceSub, String inventoryGroup) {
    // Deployment grants for alice on order + inventory only (not payment).
    return "["
        + grant(aliceSub, "order-cell", "OUTBOX_EVENT,WORKFLOW,KNOWN_GROUP_INBOX,REQUEST_RECEIPT,OVERVIEW", "owner,order")
        + ","
        + grant(
            aliceSub,
            "inventory-cell",
            "OUTBOX_EVENT,WORKFLOW,KNOWN_GROUP_INBOX,REQUEST_RECEIPT,OVERVIEW",
            "owner,reservation," + inventoryGroup)
        + "]";
  }

  static String grant(String sub, String owner, String kinds, String scopes) {
    return "{\"subject\":\""
        + sub
        + "\",\"environment\":\"test\",\"owner\":\""
        + owner
        + "\",\"kinds\":\""
        + kinds
        + "\",\"scopes\":\""
        + scopes
        + "\"}";
  }

  static void startM36bConsole(
      Ops03dRecentDiscoveryIT ops,
      Path keys,
      String identityCell,
      int port,
      Path consoleJar,
      DisposableM36aIdentity id,
      Path out)
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
                "--apex.ops.m36a.tenant-id=" + id.tenant,
                "--apex.ops.m36a.jwks-cache-ttl-ms=15000",
                "--apex.ops.m36b.resource-auth-enabled=true",
                "--apex.ops.m36b.audit-enabled=true",
                "--apex.ops.m36b.resource-grants[0].subject=" + id.user,
                "--apex.ops.m36b.resource-grants[0].environment=test",
                "--apex.ops.m36b.resource-grants[0].owner=order-cell",
                "--apex.ops.m36b.resource-grants[0].kinds=OUTBOX_EVENT,WORKFLOW,KNOWN_GROUP_INBOX,REQUEST_RECEIPT,OVERVIEW",
                "--apex.ops.m36b.resource-grants[0].scopes=owner,order",
                "--apex.ops.m36b.resource-grants[1].subject=" + id.user,
                "--apex.ops.m36b.resource-grants[1].environment=test",
                "--apex.ops.m36b.resource-grants[1].owner=inventory-cell",
                "--apex.ops.m36b.resource-grants[1].kinds=OUTBOX_EVENT,WORKFLOW,KNOWN_GROUP_INBOX,REQUEST_RECEIPT,OVERVIEW",
                "--apex.ops.m36b.resource-grants[1].scopes=owner,reservation,"
                    + ops.groups.get("inventory-cell")));
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
    Path consoleLog = out.resolve("console-" + identityCell + "-" + Instant.now().toEpochMilli() + ".log");
    var builder =
        new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(consoleLog.toFile());
    builder
        .environment()
        .keySet()
        .removeIf(
            k ->
                k.matches(
                    "(?i)(SPRING_.*|APEX_.*|CELL_.*|SERVER_.*|JAVA_TOOL_OPTIONS|JDK_JAVA_OPTIONS|_JAVA_OPTIONS)"));
    ops.console = builder.start();
    ops.children.add(ops.console);
    await()
        .ignoreExceptions()
        .atMost(Duration.ofSeconds(90))
        .untilAsserted(
            () -> {
              assertThat(ops.console.isAlive()).isTrue();
              assertThat(Files.readString(consoleLog)).contains("Started ConsoleApplication");
              assertThat(statusGet(HttpClient.newHttpClient(), port, "/ops/v1/overview").status())
                  .isEqualTo(401);
            });
  }

  static Process startNextM36b(
      Path root,
      int consolePort,
      int nextPort,
      Path log,
      DisposableM36aIdentity id,
      String sessionSecret,
      String grantsJson)
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
    env.put("APEX_OPS_M36B_RESOURCE_AUTH", "true");
    env.put("APEX_OPS_M36B_RESOURCE_GRANTS", grantsJson);
    Ops04bLiveUiBridgeIT.prependLocalNode(env);
    return builder.start();
  }

  static void awaitReady(Process next, int port) {
    await()
        .pollInterval(Duration.ofSeconds(1))
        .ignoreExceptions()
        .atMost(Duration.ofSeconds(120))
        .untilAsserted(
            () -> {
              assertThat(next.isAlive()).isTrue();
              var conn =
                  (java.net.HttpURLConnection)
                      URI.create("http://127.0.0.1:" + port + "/login").toURL().openConnection();
              conn.setConnectTimeout(3000);
              conn.setReadTimeout(3000);
              assertThat(conn.getResponseCode()).isLessThan(500);
              conn.disconnect();
            });
  }

  static int runPlaywright(
      Path root,
      Path handshake,
      String spec,
      Path log,
      String config,
      String password,
      int nextA,
      int nextB)
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
    env.put("OPS04B_NEXT_ORIGIN", "http://127.0.0.1:" + nextA);
    env.put("M36B_NEXT_B_ORIGIN", "http://127.0.0.1:" + nextB);
    env.put("OPS04B_REQUIRE_LIVE", "true");
    env.put("APEX_OPS_UI_FIXTURE_MODE", "false");
    env.put("APEX_OPS_M36A_E2E", "true");
    env.put("APEX_OPS_M36A_LOGIN", "alice");
    env.put("APEX_OPS_M36A_PASSWORD", password);
    Ops04bLiveUiBridgeIT.prependLocalNode(env);
    Process process = builder.start();
    assertThat(process.waitFor(12, TimeUnit.MINUTES)).as("playwright " + spec).isTrue();
    return process.exitValue();
  }

  record HttpStatusBody(int status, String body) {}

  static HttpStatusBody statusBearer(HttpClient http, int port, String path, String bearer)
      throws Exception {
    var res =
        http.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .header("Authorization", "Bearer " + bearer)
                .timeout(Duration.ofSeconds(8))
                .GET()
                .build(),
            HttpResponse.BodyHandlers.ofString());
    return new HttpStatusBody(res.statusCode(), res.body());
  }

  static HttpStatusBody statusGet(HttpClient http, int port, String path, String... headers)
      throws Exception {
    var b =
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
            .timeout(Duration.ofSeconds(5))
            .GET();
    for (int i = 0; i + 1 < headers.length; i += 2) {
      b.header(headers[i], headers[i + 1]);
    }
    var res = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    return new HttpStatusBody(res.statusCode(), res.body());
  }

  static String apiBearer(HttpClient http, int port, String path, String bearer) throws Exception {
    return statusBearer(http, port, path, bearer).body();
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

  static void recordGate(Map<String, Object> matrix, String id, String status, Object detail) {
    @SuppressWarnings("unchecked")
    var gates = (Map<String, Object>) matrix.computeIfAbsent("gates", k -> new LinkedHashMap<>());
    gates.put(id, Map.of("status", status, "detail", detail, "at", Instant.now().toString()));
  }

  static String gateStatus(Map<String, Object> matrix, String id) {
    @SuppressWarnings("unchecked")
    var gates = (Map<String, Object>) matrix.get("gates");
    if (gates == null) return "MISSING";
    @SuppressWarnings("unchecked")
    var row = (Map<String, Object>) gates.get(id);
    return row == null ? "MISSING" : String.valueOf(row.get("status"));
  }

  static void assertLoopbackPortFree(int port) throws Exception {
    try (ServerSocket socket = new ServerSocket()) {
      socket.setReuseAddress(false);
      socket.bind(new InetSocketAddress("127.0.0.1", port));
    } catch (Exception e) {
      throw new AssertionError(
          "Loopback port " + port + " is busy; M36B refuses to kill unrelated processes.", e);
    }
  }

  static void stopQuiet(Process p) {
    if (p == null) return;
    try {
      stop(p);
    } catch (Exception ignored) {
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
