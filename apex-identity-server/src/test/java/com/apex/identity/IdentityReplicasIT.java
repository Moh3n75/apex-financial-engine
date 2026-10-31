package com.apex.identity;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import com.nimbusds.jose.*;
import com.nimbusds.jose.crypto.*;
import com.nimbusds.jose.jwk.*;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.*;
import com.sun.net.httpserver.HttpServer;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.*;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.oauth2.core.*;
import org.springframework.security.oauth2.jwt.*;
import org.springframework.security.oauth2.server.authorization.client.*;
import org.springframework.security.oauth2.server.authorization.settings.*;
import tools.jackson.databind.json.JsonMapper;

/** Real HTTP browser + independently packaged replicas; no shared infra or production seed. */
@Tag("integration")
class IdentityReplicasIT {
  static final JsonMapper JSON = JsonMapper.builder().build();
  static final String REDIRECT = "http://127.0.0.1:19999/callback";

  static String enc(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  static String form(Map<String, String> fields) {
    return fields.entrySet().stream()
        .map(e -> enc(e.getKey()) + "=" + enc(e.getValue()))
        .collect(java.util.stream.Collectors.joining("&"));
  }

  static Map<String, String> query(String location) {
    var result = new HashMap<String, String>();
    var q = URI.create(location).getRawQuery();
    if (q != null)
      for (var pair : q.split("&")) {
        var p = pair.split("=", 2);
        result.put(
            URLDecoder.decode(p[0], StandardCharsets.UTF_8),
            p.length == 1 ? "" : URLDecoder.decode(p[1], StandardCharsets.UTF_8));
      }
    return result;
  }

  static String hidden(String html, String name) {
    var tags = Pattern.compile("<input[^>]*>").matcher(html);
    while (tags.find()) {
      String tag = tags.group();
      if (tag.contains("name=\"" + name + "\"")) {
        var v = Pattern.compile("value=\"([^\"]*)\"").matcher(tag);
        if (v.find()) return v.group(1).replace("&amp;", "&");
      }
    }
    throw new AssertionError("Missing form field: " + name);
  }

  static int port() throws Exception {
    try (var s = new java.net.ServerSocket(0)) {
      return s.getLocalPort();
    }
  }

  static String hash(String value) throws Exception {
    return HexFormat.of()
        .formatHex(
            MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void twoPackagedReplicasEnforceOidcAndPersistentCredentialLifecycles() throws Exception {
    try (var f = new Fixture()) {
      f.start();
      var discovery = f.get(f.plain, f.issuer + "/.well-known/openid-configuration");
      assertThat(discovery.statusCode()).isEqualTo(200);
      var metadata = JSON.readTree(discovery.body());
      var grantTypes = new ArrayList<String>();
      metadata.path("grant_types_supported").forEach(n -> grantTypes.add(n.asText()));
      assertThat(grantTypes)
          .containsExactlyInAnyOrder("authorization_code", "client_credentials", "refresh_token");
      assertThat(metadata.has("registration_endpoint")).isFalse();
      assertThat(metadata.has("device_authorization_endpoint")).isFalse();
      assertThat(metadata.has("pushed_authorization_request_endpoint")).isFalse();
      assertThat(metadata.path("issuer").asText()).isEqualTo(f.issuer);
      for (String field :
          List.of(
              "authorization_endpoint",
              "token_endpoint",
              "jwks_uri",
              "userinfo_endpoint",
              "revocation_endpoint",
              "introspection_endpoint"))
        assertThat(metadata.path(field).asText()).startsWith(f.issuer + "/");
      f.publicJwks();

      var browser = f.browser();
      var attacked = f.authorize(browser, "public", true);
      assertThat(f.redeem(f.nodes[0], "web", attacked.code(), attacked.verifier()).statusCode())
          .isEqualTo(400);
      // Security 7.1 invalidates a code presented by the wrong authenticated client.
      assertThat(f.redeem(f.nodes[1], "public", attacked.code(), attacked.verifier()).statusCode())
          .isEqualTo(400);
      var grant = f.authorize(browser, "public", false);
      var tokenNode = f.other(grant);
      var tokens = f.redeem(tokenNode, "public", grant.code(), grant.verifier());
      assertThat(tokens.statusCode()).isEqualTo(200);
      var user = JSON.readTree(tokens.body());
      assertThat(user.has("refresh_token")).isFalse();
      var access = f.decoder("apex-api").decode(user.path("access_token").asText());
      assertThat(access.getSubject()).isEqualTo(f.user.toString());
      assertThat(access.getClaimAsString("tenant_id")).isEqualTo(f.tenant.toString());
      assertThat(access.getClaimAsString("principal_type")).isEqualTo("user");
      var id = f.decoder("public").decode(user.path("id_token").asText());
      assertThat(Duration.between(id.getIssuedAt(), id.getExpiresAt()))
          .isEqualTo(Duration.ofMinutes(5));
      assertThat(id.getClaimAsString("nonce")).isEqualTo(grant.nonce());
      Fixture.expected(grant.nonce(), id.getClaimAsString("nonce"));
      assertThatThrownBy(() -> Fixture.expected("forged-nonce", id.getClaimAsString("nonce")))
          .isInstanceOf(IllegalArgumentException.class);
      assertThat(access.getId()).isNotBlank();
      assertThat(access.getClaimAsString("client_id")).isEqualTo("public");
      assertThat(Duration.between(access.getIssuedAt(), access.getExpiresAt()))
          .isLessThanOrEqualTo(Duration.ofMinutes(5));
      var info = f.getBearer(f.issuer + "/userinfo", user.path("access_token").asText());
      f.record(
          "userinfo-diagnostic",
          Map.of(
              "status",
              info.statusCode(),
              "responseBytes",
              info.body().length(),
              "challenge",
              info.headers().firstValue("www-authenticate").orElse("none")));
      if (info.statusCode() != 200)
        Files.writeString(
            f.output.resolve("userinfo-error.txt"),
            info.body()
                .replaceAll(
                    "[A-Za-z0-9_-]{20,}\\.[A-Za-z0-9_-]{20,}\\.[A-Za-z0-9_-]{20,}",
                    "[REDACTED_JWT]"));
      assertThat(info.statusCode())
          .withFailMessage(
              "UserInfo status=%s, challenge=%s",
              info.statusCode(), info.headers().firstValue("www-authenticate").orElse("none"))
          .isEqualTo(200);
      assertThat(JSON.readTree(info.body()).path("sub").asText()).isEqualTo(f.user.toString());
      assertThat(
              f.getBearer(
                      f.nodes[1 - tokenNode.index].url + "/userinfo",
                      user.path("access_token").asText())
                  .statusCode())
          .isEqualTo(200);
      assertThat(
              f.jdbc.queryForObject(
                  "SELECT count(*) FROM oauth2_authorization_consent", Long.class))
          .isEqualTo(1);
      assertThat(f.redeem(f.nodes[0], "public", grant.code(), grant.verifier()).statusCode())
          .isEqualTo(400);
      f.record(
          "public-pkce-oidc",
          Map.of(
              "sub",
              access.getSubject(),
              "kid",
              access.getHeaders().get("kid"),
              "nonceVerified",
              true,
              "noPublicRefresh",
              true));
      f.record(
          "cross-replica-code-exchange",
          Map.of(
              "codeHash",
              hash(grant.code()),
              "authorizationPid",
              f.nodes[1 - tokenNode.index].process.pid(),
              "tokenPid",
              tokenNode.process.pid(),
              "status",
              200,
              "accessHash",
              hash(user.path("access_token").asText())));

      var bad = f.authorize(browser, "public", false);
      assertThat(f.redeem(f.nodes[0], "public", bad.code(), "wrong".repeat(12)).statusCode())
          .isEqualTo(400);
      var noVerifier =
          f.post(
              f.plain,
              f.nodes[0].url + "/oauth2/token",
              Map.of(
                  "client_id",
                  "public",
                  "grant_type",
                  "authorization_code",
                  "code",
                  bad.code(),
                  "redirect_uri",
                  REDIRECT),
              null);
      assertThat(noVerifier.statusCode()).isEqualTo(400);
      assertThat(
              f.authorizeInvalid(browser, Map.of("redirect_uri", "https://evil.invalid/callback"))
                  .statusCode())
          .isBetween(400, 499);
      assertThat(
              f.authorizeInvalid(browser, Map.of("redirect_uri", "http://127.0.0.1:19998/callback"))
                  .statusCode())
          .isEqualTo(400);
      var scope = f.authorizeInvalid(browser, Map.of("scope", "openid admin"));
      assertThat(scope.statusCode()).isIn(302, 400);
      assertThat(scope.headers().firstValue("location").orElse(scope.body()))
          .doesNotContain("code=");
      var plainPkce = f.authorizeInvalid(browser, Map.of("code_challenge_method", "plain"));
      assertThat(plainPkce.headers().firstValue("location").orElse(plainPkce.body()))
          .doesNotContain("code=");
      var implicit = f.authorizeInvalid(browser, Map.of("response_type", "token"));
      assertThat(implicit.headers().firstValue("location").orElse(implicit.body()))
          .doesNotContain("code=");
      var expired = f.authorize(browser, "public", false);
      f.jdbc.update(
          "UPDATE oauth2_authorization SET"
              + " authorization_code_issued_at=?,authorization_code_expires_at=? WHERE"
              + " authorization_code_value=?",
          java.sql.Timestamp.from(Instant.now().minusSeconds(120)),
          java.sql.Timestamp.from(Instant.now().minusSeconds(60)),
          expired.code());
      assertThat(f.redeem(f.nodes[1], "public", expired.code(), expired.verifier()).statusCode())
          .isEqualTo(400);
      assertThat(
              f.post(
                      f.plain,
                      f.nodes[0].url + "/oauth2/token",
                      Map.of("grant_type", "password", "username", "alice"),
                      null)
                  .statusCode())
          .isBetween(400, 499);
      assertThat(
              f.post(
                      f.plain,
                      f.nodes[0].url + "/oauth2/token",
                      Map.of(
                          "client_id",
                          "unknown",
                          "grant_type",
                          "authorization_code",
                          "code",
                          "absent"),
                      null)
                  .statusCode())
          .isBetween(400, 499);
      f.csrfAndWrongPassword();
      f.record(
          "negative-oauth",
          Map.of(
              "wrongVerifier",
              true,
              "missingVerifier",
              true,
              "redirect",
              true,
              "scope",
              true,
              "plainPkce",
              true,
              "passwordGrant",
              true,
              "csrf",
              true));

      var concurrent = f.authorize(browser, "public", false);
      var codeResults =
          f.concurrent(
              () -> f.redeem(f.nodes[0], "public", concurrent.code(), concurrent.verifier()),
              () -> f.redeem(f.nodes[1], "public", concurrent.code(), concurrent.verifier()));
      assertThat(codeResults.stream().map(HttpResponse::statusCode))
          .containsExactlyInAnyOrder(200, 400);
      f.record(
          "concurrent-code",
          Map.of("statuses", codeResults.stream().map(HttpResponse::statusCode).toList()));

      var web = f.authorize(browser, "web", false);
      var webTokens = JSON.readTree(f.redeem(f.nodes[1], "web", web.code(), web.verifier()).body());
      assertThat(webTokens.path("refresh_token").asText()).isNotBlank();
      String refresh = webTokens.path("refresh_token").asText();
      var refreshed =
          f.concurrent(() -> f.refresh(f.nodes[0], refresh), () -> f.refresh(f.nodes[1], refresh));
      assertThat(refreshed.stream().map(HttpResponse::statusCode))
          .containsExactlyInAnyOrder(200, 400);
      var rotated =
          JSON.readTree(
              refreshed.stream()
                  .filter(r -> r.statusCode() == 200)
                  .findFirst()
                  .orElseThrow()
                  .body());
      assertThat(rotated.path("refresh_token").asText().equals(refresh)).isFalse();
      assertThat(f.refresh(f.nodes[0], refresh).statusCode()).isEqualTo(400);
      f.revoke(f.nodes[1], rotated.path("refresh_token").asText(), "web");
      assertThat(f.refresh(f.nodes[0], rotated.path("refresh_token").asText()).statusCode())
          .isEqualTo(400);
      f.record(
          "concurrent-refresh-revocation",
          Map.of(
              "statuses",
              refreshed.stream().map(HttpResponse::statusCode).toList(),
              "oldReplayDenied",
              true));

      var persisted = f.authorize(browser, "web", false);
      var oldPids = List.of(f.nodes[0].process.pid(), f.nodes[1].process.pid());
      f.nodes[0].restart("a");
      f.nodes[1].restart("a");
      assertThat(f.nodes[0].process.pid()).isNotIn(oldPids);
      var after =
          JSON.readTree(f.redeem(f.nodes[1], "web", persisted.code(), persisted.verifier()).body());
      assertThat(after.path("access_token").asText()).isNotBlank();
      f.authorize(
          browser, "public", false); // JDBC session/consent survives both restarts, no login.
      f.record(
          "persistent-restart",
          Map.of(
              "oldPids",
              oldPids,
              "newPids",
              List.of(f.nodes[0].process.pid(), f.nodes[1].process.pid()),
              "session",
              true,
              "code",
              true,
              "consent",
              true));

      String jwt = after.path("access_token").asText();
      assertThat(JSON.readTree(f.introspect(f.nodes[0], jwt).body()).path("active").asBoolean())
          .isTrue();
      f.revoke(f.nodes[1], jwt, "web");
      assertThat(JSON.readTree(f.introspect(f.nodes[0], jwt).body()).path("active").asBoolean())
          .isFalse();
      assertThat(f.decoder("apex-api").decode(jwt).getSubject()).isEqualTo(f.user.toString());
      f.record(
          "jwt-revocation-caveat",
          Map.of("introspectionActive", false, "offlineJwtStillValidUntilExpiry", true));

      var preDisable = f.authorize(browser, "web", false);
      var existingUserTokens =
          JSON.readTree(
              f.redeem(f.nodes[0], "web", preDisable.code(), preDisable.verifier()).body());
      var disabled = f.authorize(browser, "web", false);
      f.jdbc.update("UPDATE identity_user SET enabled=false WHERE user_id=?", f.user);
      assertThat(f.authorizeInvalid(browser, Map.of()).statusCode()).isEqualTo(403);
      assertThat(f.redeem(f.nodes[1], "web", disabled.code(), disabled.verifier()).statusCode())
          .isEqualTo(400);
      assertThat(
              f.refresh(f.nodes[0], existingUserTokens.path("refresh_token").asText()).statusCode())
          .isEqualTo(400);
      assertThat(
              JSON.readTree(
                      f.introspect(f.nodes[1], existingUserTokens.path("access_token").asText())
                          .body())
                  .path("active")
                  .asBoolean())
          .isFalse();
      f.loginDenied(f.browser(), f.password);
      f.jdbc.update("UPDATE identity_user SET enabled=true WHERE user_id=?", f.user);
      assertThatThrownBy(
              () ->
                  f.jdbc.update(
                      "UPDATE identity_user SET tenant_id=? WHERE user_id=?",
                      UUID.randomUUID(),
                      f.user))
          .isInstanceOf(Exception.class);

      var machineA = f.machine(f.nodes[0]);
      assertThat(machineA.statusCode()).isEqualTo(200);
      var machine = JSON.readTree(machineA.body());
      assertThat(machine.has("id_token")).isFalse();
      assertThat(machine.has("refresh_token")).isFalse();
      var service = f.decoder("apex-api").decode(machine.path("access_token").asText());
      assertThat(service.getSubject()).isEqualTo("machine");
      assertThat(service.getClaimAsString("principal_type")).isEqualTo("service");
      var disallowed =
          f.post(
              f.plain,
              f.nodes[0].url + "/oauth2/token",
              Map.of("grant_type", "password"),
              "machine:" + f.secret);
      assertThat(disallowed.statusCode()).isEqualTo(400);
      assertThat(JSON.readTree(disallowed.body()).path("error").asText())
          .isEqualTo("unsupported_grant_type");
      var exchange =
          f.post(
              f.plain,
              f.nodes[0].url + "/oauth2/token",
              Map.of("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange"),
              "machine:" + f.secret);
      assertThat(exchange.statusCode()).isEqualTo(400);
      assertThat(JSON.readTree(exchange.body()).path("error").asText())
          .isEqualTo("unsupported_grant_type");
      assertThat(
              f.post(
                      f.plain,
                      f.nodes[1].url + "/oauth2/token",
                      Map.of("grant_type", "client_credentials", "scope", "api.read"),
                      "machine:bad-secret")
                  .statusCode())
          .isEqualTo(401);
      assertThat(
              f.post(
                      f.plain,
                      f.nodes[1].url + "/oauth2/token",
                      Map.of("grant_type", "client_credentials", "scope", "openid"),
                      "machine:" + f.secret)
                  .statusCode())
          .isBetween(400, 499);
      assertThat(
              f.post(
                      f.plain,
                      f.nodes[1].url + "/oauth2/token",
                      Map.of(
                          "grant_type",
                          "client_credentials",
                          "client_id",
                          "machine",
                          "client_secret",
                          f.secret),
                      null)
                  .statusCode())
          .isEqualTo(401);
      var foreign = UUID.randomUUID();
      f.jdbc.update("INSERT INTO identity_tenant VALUES(?)", foreign);
      f.jdbc.update(
          "UPDATE identity_client_policy SET tenant_id=? WHERE registered_client_id='machine'",
          foreign);
      assertThat(f.machine(f.nodes[0]).statusCode()).isEqualTo(503);
      f.jdbc.update(
          "UPDATE identity_client_policy SET tenant_id=? WHERE registered_client_id='machine'",
          f.tenant);
      var registry = new JdbcRegisteredClientRepository(f.jdbc);
      var approvedMachine = registry.findById("machine");
      registry.save(
          RegisteredClient.from(approvedMachine)
              .tokenSettings(
                  TokenSettings.withSettings(approvedMachine.getTokenSettings().getSettings())
                      .accessTokenFormat(OAuth2TokenFormat.REFERENCE)
                      .build())
              .build());
      assertThat(f.machine(f.nodes[0]).statusCode()).isEqualTo(503);
      registry.save(approvedMachine);
      f.record(
          "disabled-user-and-client-policy",
          Map.of(
              "existingSessionAuthorizeDenied",
              true,
              "disabledCodeRefreshDenied",
              true,
              "disabledIntrospectionInactive",
              true,
              "foreignClientTenantDenied",
              true));
      f.record(
          "client-credentials",
          Map.of(
              "status",
              200,
              "sub",
              service.getSubject(),
              "principalType",
              service.getClaimAsString("principal_type"),
              "aud",
              service.getAudience(),
              "noIdOrRefreshToken",
              true,
              "badSecretAndMethodDenied",
              true));
      f.jwtNegatives(machine.path("access_token").asText());

      f.commitFailureRollsBack(browser);
      f.nodes[0].restart(
          "b"); // staged public B was already published; mixed active keys, same public set.
      var b = JSON.readTree(f.machine(f.nodes[0]).body()).path("access_token").asText();
      assertThat(f.decoder("apex-api").decode(b).getHeaders().get("kid")).isEqualTo("b");
      assertThat(
              f.decoder("apex-api")
                  .decode(machine.path("access_token").asText())
                  .getHeaders()
                  .get("kid"))
          .isEqualTo("a");
      var mixedGrant = f.authorize(browser, "web", false);
      var mixedTokens =
          JSON.readTree(
              f.redeem(f.nodes[0], "web", mixedGrant.code(), mixedGrant.verifier()).body());
      var mixedAccess = mixedTokens.path("access_token").asText();
      assertThat(f.decoder("apex-api").decode(mixedAccess).getHeaders().get("kid")).isEqualTo("b");
      assertThat(f.getBearer(f.nodes[1].url + "/userinfo", mixedAccess).statusCode())
          .isEqualTo(200);
      f.record(
          "mixed-key-cross-replica-userinfo",
          Map.of(
              "signingPid",
              f.nodes[0].process.pid(),
              "verifyingPid",
              f.nodes[1].process.pid(),
              "kid",
              "b",
              "status",
              200));
      f.nodes[1].restart("b");
      f.publicJwks();
      f.record(
          "rotation",
          Map.of(
              "activeKid",
              "b",
              "oldKidVerifies",
              "a",
              "publicThumbprints",
              List.of(
                  f.a.toPublicJWK().computeThumbprint().toString(),
                  f.b.toPublicJWK().computeThumbprint().toString())));
      f.noKeyStartupFails();
      f.noConfigStartupFails();
      f.unprovisionedStartupFails();
      f.logout(browser);
      f.record(
          "owner-sql",
          Map.of(
              "users",
              f.jdbc.queryForObject("SELECT count(*) FROM identity_user", Long.class),
              "clients",
              f.jdbc.queryForObject("SELECT count(*) FROM oauth2_registered_client", Long.class),
              "authorizations",
              f.jdbc.queryForObject("SELECT count(*) FROM oauth2_authorization", Long.class),
              "consents",
              f.jdbc.queryForObject(
                  "SELECT count(*) FROM oauth2_authorization_consent", Long.class)));
    }
  }

  record Grant(String code, String verifier, String nonce) {}

  static final class Fixture implements AutoCloseable {
    final Path output = Path.of("target/identity-evidence").toAbsolutePath();
    final List<Map<String, Object>> evidence = new ArrayList<>();
    final List<Process> children = new ArrayList<>();
    final EmbeddedPostgres pg;
    final JdbcTemplate jdbc;
    final UUID tenant = UUID.randomUUID(), user = UUID.randomUUID();
    final String password = "P-" + UUID.randomUUID(), secret = "S-" + UUID.randomUUID();
    final Set<String> confidentialValues = new HashSet<>();
    final Map<String, Integer> codeOrigins = new ConcurrentHashMap<>();
    final RSAKey a, b;
    final Path keys;
    final Node[] nodes = new Node[2];
    final HttpClient plain = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    final HttpServer edge;
    final ExecutorService proxyThreads = Executors.newFixedThreadPool(8);
    final AtomicInteger next = new AtomicInteger();
    final String issuer;

    Fixture() throws Exception {
      Files.createDirectories(output);
      keys = Files.createTempDirectory("m35-keys-");
      a =
          new RSAKeyGenerator(2048)
              .keyID("a")
              .keyUse(KeyUse.SIGNATURE)
              .algorithm(JWSAlgorithm.RS256)
              .generate();
      b =
          new RSAKeyGenerator(2048)
              .keyID("b")
              .keyUse(KeyUse.SIGNATURE)
              .algorithm(JWSAlgorithm.RS256)
              .generate();
      Files.writeString(
          keys.resolve("a.json"), new JWKSet(List.of(a, b.toPublicJWK())).toString(false));
      Files.writeString(
          keys.resolve("b.json"), new JWKSet(List.of(a.toPublicJWK(), b)).toString(false));
      pg =
          EmbeddedPostgres.builder()
              .setServerConfig("fsync", "on")
              .setServerConfig("synchronous_commit", "on")
              .start();
      jdbc = new JdbcTemplate(pg.getPostgresDatabase());
      // Token transactions must explicitly reload after lock acquisition, even if the DB's
      // default would otherwise retain an older snapshot on a waiting concurrent redemption.
      jdbc.execute("ALTER DATABASE postgres SET default_transaction_isolation='repeatable read'");
      Flyway.configure()
          .dataSource(pg.getPostgresDatabase())
          .locations("classpath:db/migration")
          .load()
          .migrate();
      jdbc.update("INSERT INTO identity_tenant VALUES(?)", tenant);
      var encoder = PasswordEncoderFactories.createDelegatingPasswordEncoder();
      jdbc.update(
          "INSERT INTO identity_user(user_id,tenant_id,login_name,password_hash,enabled)"
              + " VALUES(?,?,'alice',?,true)",
          user,
          tenant,
          encoder.encode(password));
      var clients = new JdbcRegisteredClientRepository(jdbc);
      for (String name : List.of("public", "web", "machine")) {
        var builder =
            RegisteredClient.withId(name)
                .clientId(name)
                .clientName(name)
                .scope("api.read")
                .clientSettings(
                    ClientSettings.builder()
                        .requireProofKey(!name.equals("machine"))
                        .requireAuthorizationConsent(!name.equals("machine"))
                        .build())
                .tokenSettings(
                    TokenSettings.builder()
                        .accessTokenTimeToLive(Duration.ofMinutes(2))
                        .authorizationCodeTimeToLive(Duration.ofMinutes(1))
                        .refreshTokenTimeToLive(Duration.ofHours(1))
                        .reuseRefreshTokens(false)
                        .build());
        if (name.equals("public"))
          builder.clientAuthenticationMethod(ClientAuthenticationMethod.NONE);
        else
          builder
              .clientAuthenticationMethod(ClientAuthenticationMethod.CLIENT_SECRET_BASIC)
              .clientSecret(encoder.encode(secret));
        if (name.equals("machine"))
          builder.authorizationGrantType(AuthorizationGrantType.CLIENT_CREDENTIALS);
        else {
          builder
              .authorizationGrantType(AuthorizationGrantType.AUTHORIZATION_CODE)
              .redirectUri(REDIRECT)
              .scope("openid")
              .scope("profile");
          if (name.equals("web"))
            builder.authorizationGrantType(AuthorizationGrantType.REFRESH_TOKEN);
        }
        clients.save(builder.build());
        jdbc.update("INSERT INTO identity_client_policy VALUES(?,?,?)", name, tenant, "apex-api");
      }
      edge = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      issuer = "http://127.0.0.1:" + edge.getAddress().getPort();
      nodes[0] = new Node(this, port(), 0);
      nodes[1] = new Node(this, port(), 1);
      edge.setExecutor(proxyThreads);
      edge.createContext(
          "/",
          exchange -> {
            try {
              var live =
                  Arrays.stream(nodes)
                      .filter(n -> n.process != null && n.process.isAlive())
                      .toList();
              var node = live.get(Math.floorMod(next.getAndIncrement(), live.size()));
              var req =
                  HttpRequest.newBuilder(URI.create(node.url + exchange.getRequestURI()))
                      .timeout(Duration.ofSeconds(12));
              exchange
                  .getRequestHeaders()
                  .forEach(
                      (k, v) -> {
                        if (!Set.of(
                                "host", "content-length", "connection", "upgrade", "http2-settings")
                            .contains(k.toLowerCase())) v.forEach(value -> req.header(k, value));
                      });
              var raw = exchange.getRequestBody().readAllBytes();
              req.method(exchange.getRequestMethod(), HttpRequest.BodyPublishers.ofByteArray(raw));
              var reply = plain.send(req.build(), HttpResponse.BodyHandlers.ofByteArray());
              var location = reply.headers().firstValue("location").orElse("");
              if (location.startsWith(REDIRECT)) {
                var code = query(location).get("code");
                if (code != null) codeOrigins.put(hash(code), node.index);
              }
              reply
                  .headers()
                  .map()
                  .forEach(
                      (k, v) -> {
                        if (!Set.of("content-length", "connection", "transfer-encoding")
                            .contains(k.toLowerCase())) exchange.getResponseHeaders().put(k, v);
                      });
              exchange.sendResponseHeaders(reply.statusCode(), reply.body().length);
              exchange.getResponseBody().write(reply.body());
            } catch (Exception failure) {
              exchange.sendResponseHeaders(503, -1);
            } finally {
              exchange.close();
            }
          });
    }

    void start() throws Exception {
      nodes[0].restart("a");
      nodes[1].restart("a");
      edge.start();
      record(
          "topology",
          Map.of(
              "issuer",
              issuer,
              "pgPort",
              pg.getPort(),
              "pids",
              List.of(nodes[0].process.pid(), nodes[1].process.pid()),
              "ports",
              List.of(nodes[0].port, nodes[1].port)));
    }

    void record(String stage, Map<String, Object> fields) {
      var row = new LinkedHashMap<String, Object>(fields);
      row.put("stage", stage);
      evidence.add(row);
    }

    Node other(Grant grant) throws Exception {
      Integer origin = codeOrigins.get(hash(grant.code()));
      assertThat(origin).isNotNull();
      return nodes[1 - origin];
    }

    static void expected(String expected, String actual) {
      if (!Objects.equals(expected, actual))
        throw new IllegalArgumentException("Client state/nonce mismatch");
    }

    HttpClient browser() {
      return HttpClient.newBuilder()
          .cookieHandler(new CookieManager(null, CookiePolicy.ACCEPT_ALL))
          .connectTimeout(Duration.ofSeconds(2))
          .build();
    }

    HttpResponse<String> get(HttpClient client, String url) throws Exception {
      return client.send(
          HttpRequest.newBuilder(URI.create(url))
              .header("Accept", "text/html")
              .timeout(Duration.ofSeconds(12))
              .GET()
              .build(),
          HttpResponse.BodyHandlers.ofString());
    }

    HttpResponse<String> getBearer(String url, String token) throws Exception {
      return plain.send(
          HttpRequest.newBuilder(URI.create(url))
              .header("Authorization", "Bearer " + token)
              .timeout(Duration.ofSeconds(8))
              .build(),
          HttpResponse.BodyHandlers.ofString());
    }

    HttpResponse<String> post(HttpClient client, String url, Map<String, String> data, String basic)
        throws Exception {
      var r =
          HttpRequest.newBuilder(URI.create(url))
              .timeout(Duration.ofSeconds(12))
              .header("Content-Type", "application/x-www-form-urlencoded");
      if (basic != null)
        r.header(
            "Authorization",
            "Basic " + Base64.getEncoder().encodeToString(basic.getBytes(StandardCharsets.UTF_8)));
      String encoded = form(data);
      if (url.endsWith("/oauth2/authorize") && data.containsKey("scope")) {
        var other = new LinkedHashMap<>(data);
        other.remove("scope");
        encoded = form(other);
        for (String scope : data.get("scope").split(" ")) encoded += "&scope=" + enc(scope);
      }
      var response =
          client.send(
              r.POST(HttpRequest.BodyPublishers.ofString(encoded)).build(),
              HttpResponse.BodyHandlers.ofString());
      if (url.endsWith("/oauth2/token") && response.statusCode() == 200) {
        var tokenBody = JSON.readTree(response.body());
        synchronized (confidentialValues) {
          for (String field : List.of("access_token", "refresh_token", "id_token"))
            if (tokenBody.hasNonNull(field)) confidentialValues.add(tokenBody.path(field).asText());
        }
      }
      return response;
    }

    Map<String, String> authFields(String client, String verifier, String nonce, String state)
        throws Exception {
      return new LinkedHashMap<>(
          Map.of(
              "client_id",
              client,
              "response_type",
              "code",
              "redirect_uri",
              REDIRECT,
              "scope",
              "openid profile api.read",
              "state",
              state,
              "nonce",
              nonce,
              "code_challenge",
              Base64.getUrlEncoder()
                  .withoutPadding()
                  .encodeToString(
                      MessageDigest.getInstance("SHA-256")
                          .digest(verifier.getBytes(StandardCharsets.US_ASCII))),
              "code_challenge_method",
              "S256"));
    }

    Grant authorize(HttpClient browser, String client, boolean login) throws Exception {
      String verifier = UUID.randomUUID() + "-" + UUID.randomUUID(),
          nonce = UUID.randomUUID().toString(),
          state = UUID.randomUUID().toString();
      var response =
          get(
              browser,
              issuer + "/oauth2/authorize?" + form(authFields(client, verifier, nonce, state)));
      if (login) {
        assertThat(response.statusCode()).isEqualTo(302);
        var page =
            get(
                browser,
                URI.create(issuer)
                    .resolve(response.headers().firstValue("location").orElseThrow())
                    .toString());
        var logged =
            post(
                browser,
                issuer + "/login",
                Map.of(
                    "username",
                    "alice",
                    "password",
                    password,
                    "_csrf",
                    hidden(page.body(), "_csrf")),
                null);
        assertThat(logged.statusCode()).isEqualTo(302);
        response = get(browser, logged.headers().firstValue("location").orElseThrow());
      }
      if (response.statusCode() == 200) {
        var consent = new LinkedHashMap<String, String>();
        consent.put("client_id", client);
        consent.put("state", hidden(response.body(), "state"));
        consent.put("scope", "api.read profile");
        response = post(browser, issuer + "/oauth2/authorize", consent, null);
      }
      assertThat(response.statusCode()).isEqualTo(302);
      var fields = query(response.headers().firstValue("location").orElseThrow());
      assertThat(fields.get("state")).isEqualTo(state);
      expected(state, fields.get("state"));
      assertThatThrownBy(() -> expected("forged-state", fields.get("state")))
          .isInstanceOf(IllegalArgumentException.class);
      assertThat(fields.get("code"))
          .withFailMessage("Authorization error category: %s", fields.get("error"))
          .isNotBlank();
      assertThat(fields.get("state")).isNotEqualTo("forged-state");
      return new Grant(fields.get("code"), verifier, nonce);
    }

    HttpResponse<String> authorizeInvalid(HttpClient b, Map<String, String> override)
        throws Exception {
      var fields = authFields("public", "x".repeat(60), "n", "s");
      fields.putAll(override);
      return get(b, issuer + "/oauth2/authorize?" + form(fields));
    }

    HttpResponse<String> redeem(Node node, String client, String code, String verifier)
        throws Exception {
      return post(
          plain,
          node.url + "/oauth2/token",
          Map.of(
              "grant_type",
              "authorization_code",
              "client_id",
              client,
              "redirect_uri",
              REDIRECT,
              "code",
              code,
              "code_verifier",
              verifier),
          client.equals("web") ? "web:" + secret : null);
    }

    HttpResponse<String> refresh(Node node, String token) throws Exception {
      return post(
          plain,
          node.url + "/oauth2/token",
          Map.of("grant_type", "refresh_token", "refresh_token", token),
          "web:" + secret);
    }

    HttpResponse<String> machine(Node node) throws Exception {
      return post(
          plain,
          node.url + "/oauth2/token",
          Map.of("grant_type", "client_credentials", "scope", "api.read"),
          "machine:" + secret);
    }

    HttpResponse<String> introspect(Node node, String token) throws Exception {
      return post(plain, node.url + "/oauth2/introspect", Map.of("token", token), "web:" + secret);
    }

    void revoke(Node node, String token, String client) throws Exception {
      assertThat(
              post(
                      plain,
                      node.url + "/oauth2/revoke",
                      Map.of("token", token),
                      client + ":" + secret)
                  .statusCode())
          .isEqualTo(200);
    }

    List<HttpResponse<String>> concurrent(
        Callable<HttpResponse<String>> a, Callable<HttpResponse<String>> b) throws Exception {
      try (var pool = Executors.newFixedThreadPool(2)) {
        var gate = new CountDownLatch(1);
        var one =
            pool.submit(
                () -> {
                  gate.await();
                  return a.call();
                });
        var two =
            pool.submit(
                () -> {
                  gate.await();
                  return b.call();
                });
        gate.countDown();
        return List.of(one.get(20, TimeUnit.SECONDS), two.get(20, TimeUnit.SECONDS));
      }
    }

    NimbusJwtDecoder decoder(String aud) {
      var d = NimbusJwtDecoder.withJwkSetUri(issuer + "/oauth2/jwks").build();
      d.setJwtValidator(
          new DelegatingOAuth2TokenValidator<>(
              JwtValidators.createDefaultWithIssuer(issuer),
              jwt ->
                  jwt.getAudience().contains(aud)
                      ? OAuth2TokenValidatorResult.success()
                      : OAuth2TokenValidatorResult.failure(new OAuth2Error("invalid_token"))));
      return d;
    }

    void publicJwks() throws Exception {
      var r = get(plain, issuer + "/oauth2/jwks");
      assertThat(r.statusCode()).isEqualTo(200);
      var set = JWKSet.parse(r.body());
      assertThat(set.getKeys()).hasSize(2);
      assertThat(set.getKeys()).allMatch(k -> !k.isPrivate());
    }

    void loginDenied(HttpClient b, String pass) throws Exception {
      var page = get(b, issuer + "/login");
      var reply =
          post(
              b,
              issuer + "/login",
              Map.of("username", "alice", "password", pass, "_csrf", hidden(page.body(), "_csrf")),
              null);
      assertThat(reply.headers().firstValue("location").orElse("")).contains("error");
    }

    void csrfAndWrongPassword() throws Exception {
      var b = browser();
      assertThat(
              post(b, issuer + "/login", Map.of("username", "alice", "password", password), null)
                  .statusCode())
          .isEqualTo(403);
      loginDenied(browser(), "wrong");
    }

    void jwtNegatives(String valid) throws Exception {
      assertThatThrownBy(() -> decoder("other-audience").decode(valid))
          .isInstanceOf(JwtException.class);
      for (String kind : List.of("issuer", "expired", "future", "unknown-key", "tampered")) {
        var claims =
            new JWTClaimsSet.Builder()
                .issuer(kind.equals("issuer") ? "https://wrong.invalid" : issuer)
                .subject("machine")
                .audience("apex-api")
                .issueTime(new Date())
                .expirationTime(Date.from(Instant.now().plusSeconds(60)))
                .notBeforeTime(Date.from(Instant.now().minusSeconds(1)));
        if (kind.equals("expired"))
          claims.expirationTime(Date.from(Instant.now().minusSeconds(180)));
        if (kind.equals("future")) claims.notBeforeTime(Date.from(Instant.now().plusSeconds(300)));
        var signed =
            new SignedJWT(
                new JWSHeader.Builder(JWSAlgorithm.RS256)
                    .keyID(kind.equals("unknown-key") ? "unknown" : "a")
                    .build(),
                claims.build());
        signed.sign(
            new RSASSASigner(kind.equals("tampered") ? new RSAKeyGenerator(2048).generate() : a));
        assertThatThrownBy(() -> decoder("apex-api").decode(signed.serialize()))
            .as(kind)
            .isInstanceOf(JwtException.class);
      }
      record(
          "independent-jwt-validation",
          Map.of(
              "badAudience",
              true,
              "badIssuer",
              true,
              "expired",
              true,
              "notBefore",
              true,
              "unknownKid",
              true,
              "badSignature",
              true));
    }

    void commitFailureRollsBack(HttpClient browser) throws Exception {
      var grant = authorize(browser, "public", false);
      long before =
          jdbc.queryForObject(
              "SELECT count(*) FROM oauth2_authorization WHERE access_token_value IS NOT NULL",
              Long.class);
      jdbc.execute(
          "CREATE FUNCTION m35_commit_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF"
              + " NEW.access_token_value IS NOT NULL AND OLD.access_token_value IS NULL THEN RAISE"
              + " EXCEPTION 'isolated commit fault'; END IF; RETURN NEW; END $$");
      jdbc.execute(
          "CREATE CONSTRAINT TRIGGER m35_commit_fault AFTER UPDATE ON oauth2_authorization"
              + " DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION m35_commit_fault()");
      var reply = redeem(nodes[0], "public", grant.code(), grant.verifier());
      assertThat(reply.statusCode()).isEqualTo(503);
      assertThat(reply.body()).doesNotContain("access_token");
      assertThat(
              jdbc.queryForObject(
                  "SELECT count(*) FROM oauth2_authorization WHERE access_token_value IS NOT NULL",
                  Long.class))
          .isEqualTo(before);
      jdbc.execute("DROP TRIGGER m35_commit_fault ON oauth2_authorization");
      jdbc.execute("DROP FUNCTION m35_commit_fault()");
      assertThat(redeem(nodes[1], "public", grant.code(), grant.verifier()).statusCode())
          .isEqualTo(200);
      record(
          "commit-boundary", Map.of("failedReply", 503, "rolledBack", true, "sameCodeRetry", 200));
    }

    void noKeyStartupFails() throws Exception {
      nodes[0].stop();
      var args = nodes[0].args("b");
      args.removeIf(s -> s.startsWith("--apex.identity.key-file="));
      args.add("--apex.identity.key-file=" + keys.resolve("missing.json"));
      var child = launch(args, output.resolve("missing-key.log"));
      assertThat(child.waitFor(40, TimeUnit.SECONDS)).isTrue();
      assertThat(child.exitValue()).isNotZero();
      nodes[0].restart("b");
      record("missing-key", Map.of("exit", child.exitValue(), "noGeneratedFallback", true));
    }

    void noConfigStartupFails() throws Exception {
      var child = launch(List.of(), output.resolve("no-config.log"));
      assertThat(child.waitFor(30, TimeUnit.SECONDS)).isTrue();
      assertThat(child.exitValue()).isNotZero();
      assertThat(Files.readString(output.resolve("no-config.log")))
          .doesNotContain("Tomcat started");
      record("unconfigured-startup", Map.of("exit", child.exitValue(), "noListener", true));
    }

    void unprovisionedStartupFails() throws Exception {
      // A second empty logical DB in this same fixture-owned PG, not an existing deployment DB.
      jdbc.execute("CREATE DATABASE identity_unprovisioned");
      String url = "jdbc:postgresql://127.0.0.1:" + pg.getPort() + "/identity_unprovisioned";
      var args = nodes[0].args("b");
      args.removeIf(
          arg -> arg.startsWith("--spring.datasource.url=") || arg.startsWith("--server.port="));
      args.add("--spring.datasource.url=" + url);
      args.add("--server.port=" + port());
      var log = output.resolve("unprovisioned.log");
      var child = launch(args, log);
      assertThat(child.waitFor(40, TimeUnit.SECONDS)).isTrue();
      assertThat(child.exitValue()).isNotZero();
      assertThat(Files.readString(log))
          .contains("Explicit offline user/client provisioning required")
          .doesNotContain("Tomcat started", "Using generated security password");
      var empty =
          new JdbcTemplate(
              new org.springframework.jdbc.datasource.DriverManagerDataSource(url, "postgres", ""));
      assertThat(empty.queryForObject("SELECT count(*) FROM identity_user", Long.class)).isZero();
      assertThat(empty.queryForObject("SELECT count(*) FROM oauth2_registered_client", Long.class))
          .isZero();
      record(
          "empty-registry-startup",
          Map.of("exit", child.exitValue(), "users", 0, "clients", 0, "noFallback", true));
    }

    void logout(HttpClient browser) throws Exception {
      var page = get(browser, issuer + "/logout");
      assertThat(page.statusCode()).isEqualTo(200);
      var reply =
          post(browser, issuer + "/logout", Map.of("_csrf", hidden(page.body(), "_csrf")), null);
      assertThat(reply.statusCode()).isEqualTo(302);
      var request = authFields("public", "x".repeat(60), "nonce", "state");
      var denied = get(browser, issuer + "/oauth2/authorize?" + form(request));
      assertThat(denied.headers().firstValue("location").orElse("")).endsWith("/login");
      record("csrf-logout", Map.of("status", 302, "sessionNoLongerAuthenticated", true));
    }

    Process launch(List<String> args, Path log) throws Exception {
      var command =
          new ArrayList<>(
              List.of(
                  Path.of(
                          System.getProperty("java.home"),
                          "bin",
                          System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java")
                      .toString(),
                  "-Xmx256m",
                  "-jar",
                  Path.of("target/apex-identity-server.jar").toAbsolutePath().toString()));
      command.addAll(args);
      var pb = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile());
      pb.environment()
          .keySet()
          .removeIf(
              k ->
                  k.matches(
                      "^(SPRING_|APEX_|IDENTITY_|SERVER_|JAVA_TOOL_OPTIONS|JDK_JAVA_OPTIONS|_JAVA_OPTIONS).*"));
      var p = pb.start();
      children.add(p);
      return p;
    }

    public void close() throws Exception {
      var errors = new ArrayList<Exception>();
      edge.stop(0);
      proxyThreads.shutdownNow();
      for (var p : children)
        try {
          p.destroy();
          if (!p.waitFor(10, TimeUnit.SECONDS)) {
            p.destroyForcibly();
            if (!p.waitFor(10, TimeUnit.SECONDS))
              throw new IllegalStateException("Child stop failed");
          }
        } catch (Exception e) {
          errors.add(e);
        }
      confidentialValues.add(password);
      confidentialValues.add(secret);
      try (var logs = Files.list(output)) {
        for (Path log : logs.filter(p -> p.toString().endsWith(".log")).toList()) {
          String text = Files.readString(log);
          boolean exposed =
              confidentialValues.stream().anyMatch(text::contains) || text.contains("{bcrypt}$2");
          if (exposed) errors.add(new IllegalStateException("Confidential log value detected"));
        }
      }
      try {
        pg.close();
      } catch (Exception e) {
        errors.add(e);
      }
      try (var files = Files.list(keys)) {
        for (var path : files.toList()) Files.delete(path);
      }
      Files.delete(keys);
      record(
          "teardown",
          Map.of(
              "pids",
              children.stream().map(Process::pid).toList(),
              "allStopped",
              children.stream().noneMatch(Process::isAlive),
              "cleanupFailures",
              errors.size()));
      Files.writeString(
          output.resolve("acceptance.json"),
          JSON.writerWithDefaultPrettyPrinter().writeValueAsString(evidence));
      if (!errors.isEmpty()) throw errors.getFirst();
    }
  }

  static final class Node {
    final Fixture f;
    final int port, index;
    final String url;
    Process process;
    int starts;

    Node(Fixture f, int port, int index) {
      this.f = f;
      this.port = port;
      this.index = index;
      url = "http://127.0.0.1:" + port;
    }

    List<String> args(String key) {
      return new ArrayList<>(
          List.of(
              "--spring.profiles.active=isolated-test",
              "--apex.identity.allow-loopback-http=true",
              "--apex.identity.issuer=" + f.issuer,
              "--apex.identity.tenant-id=" + f.tenant,
              "--apex.identity.key-file=" + f.keys.resolve(key + ".json"),
              "--apex.identity.active-kid=" + key,
              "--server.address=127.0.0.1",
              "--server.port=" + port,
              "--server.servlet.session.cookie.secure=false",
              "--spring.datasource.url=" + f.pg.getJdbcUrl("postgres", "postgres"),
              "--spring.datasource.username=postgres",
              "--spring.datasource.password=",
              "--spring.main.banner-mode=off"));
    }

    void stop() throws Exception {
      if (process != null) {
        process.destroy();
        if (!process.waitFor(10, TimeUnit.SECONDS)) {
          process.destroyForcibly();
          assertThat(process.waitFor(10, TimeUnit.SECONDS)).isTrue();
        }
      }
    }

    void restart(String key) throws Exception {
      stop();
      var log = f.output.resolve("node-" + index + "-" + (starts++) + ".log");
      process = f.launch(args(key), log);
      await()
          .atMost(Duration.ofSeconds(45))
          .untilAsserted(
              () -> {
                assertThat(process.isAlive()).withFailMessage("Inspect %s", log).isTrue();
                assertThat(Files.readString(log)).contains("Started IdentityApplication");
              });
    }
  }
}
