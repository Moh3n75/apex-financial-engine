package com.apex.inventory.m36a;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.KeyUse;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.flywaydb.core.Flyway;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.oauth2.core.ClientAuthenticationMethod;
import org.springframework.security.oauth2.server.authorization.client.JdbcRegisteredClientRepository;
import org.springframework.security.oauth2.server.authorization.client.RegisteredClient;
import org.springframework.security.oauth2.server.authorization.settings.ClientSettings;
import org.springframework.security.oauth2.server.authorization.settings.TokenSettings;
import tools.jackson.databind.json.JsonMapper;

/**
 * Single-node disposable Identity for M36A.1 authenticated live acceptance.
 *
 * <p>Owns EmbeddedPostgres + offline-seeded {@code apex-ops-web}/{@code alice} + one packaged
 * IdentityApplication. Issuer URL is the loopback node itself (no dual-replica edge).
 */
public final class DisposableM36aIdentity implements AutoCloseable {
  public static final String CLIENT_ID = "apex-ops-web";
  public static final String OPS_REDIRECT = "http://127.0.0.1:3000/api/auth/callback";
  public static final String AUDIENCE = "apex-operations-console";
  static final JsonMapper JSON = JsonMapper.builder().build();

  public final UUID tenant = UUID.randomUUID();
  public final UUID user = UUID.randomUUID();
  /** Second user for resource-grant negatives (same tenant, ops.console.read, no resource grants). */
  public final UUID bobUser = UUID.randomUUID();
  public final String password = "P-" + UUID.randomUUID();
  public final String bobPassword = "P-" + UUID.randomUUID();
  public final String secret = "S-" + UUID.randomUUID();
  public final String issuer;
  public final int port;
  public final Path keys;
  public final Path output;
  public final Path identityJar;
  public final EmbeddedPostgres pg;
  public final JdbcTemplate jdbc;
  public RSAKey signingKey;
  /** Retained after rotation so retired-kid signatures can be forged for negative JWKS tests. */
  public RSAKey retiredSigningKey;
  public String activeKid = "a";
  private final List<Process> children = new ArrayList<>();
  private Process process;
  private final Path log;

  public DisposableM36aIdentity(Path identityJar, Path outputDir, Path flywayMigrations)
      throws Exception {
    this.identityJar = identityJar.toAbsolutePath().normalize();
    assertThat(this.identityJar).as("identity jar").isRegularFile();
    this.output = outputDir.toAbsolutePath().normalize();
    Files.createDirectories(this.output);
    Path migrations = flywayMigrations.toAbsolutePath().normalize();
    assertThat(migrations).as("identity flyway migrations").isDirectory();
    this.keys = Files.createTempDirectory("m36a1-keys-");
    this.signingKey =
        new RSAKeyGenerator(2048)
            .keyID("a")
            .keyUse(KeyUse.SIGNATURE)
            .algorithm(JWSAlgorithm.RS256)
            .generate();
    Files.writeString(
        keys.resolve("a.json"), new JWKSet(List.of(signingKey)).toString(false));
    this.pg =
        EmbeddedPostgres.builder()
            .setConnectConfig("sslmode", "disable")
            .setServerConfig("fsync", "on")
            .setServerConfig("synchronous_commit", "on")
            .start();
    this.jdbc = new JdbcTemplate(pg.getPostgresDatabase());
    jdbc.execute("ALTER DATABASE postgres SET default_transaction_isolation='repeatable read'");
    // Prefer filesystem migrations so inventory-cell (test-jar consumer) need not unpack the Boot jar.
    Flyway.configure()
        .dataSource(pg.getPostgresDatabase())
        .locations("filesystem:" + migrations)
        .load()
        .migrate();
    seed();
    this.port = freePort();
    this.issuer = "http://127.0.0.1:" + port;
    this.log = output.resolve("identity-node.log");
  }

  private void seed() {
    var encoder = PasswordEncoderFactories.createDelegatingPasswordEncoder();
    jdbc.update("INSERT INTO identity_tenant VALUES(?)", tenant);
    jdbc.update(
        "INSERT INTO identity_user(user_id,tenant_id,login_name,password_hash,enabled)"
            + " VALUES(?,?,'alice',?,true)",
        user,
        tenant,
        encoder.encode(password));
    jdbc.update(
        "INSERT INTO identity_user(user_id,tenant_id,login_name,password_hash,enabled)"
            + " VALUES(?,?,'bob',?,true)",
        bobUser,
        tenant,
        encoder.encode(bobPassword));
    var clients = new JdbcRegisteredClientRepository(jdbc);
    clients.save(
        RegisteredClient.withId(CLIENT_ID)
            .clientId(CLIENT_ID)
            .clientName(CLIENT_ID)
            .clientAuthenticationMethod(ClientAuthenticationMethod.CLIENT_SECRET_BASIC)
            .clientSecret(encoder.encode(secret))
            .authorizationGrantType(AuthorizationGrantType.AUTHORIZATION_CODE)
            .authorizationGrantType(AuthorizationGrantType.REFRESH_TOKEN)
            .redirectUri(OPS_REDIRECT)
            .scope("openid")
            .scope("profile")
            .scope("ops.console.read")
            .clientSettings(
                ClientSettings.builder()
                    .requireProofKey(true)
                    .requireAuthorizationConsent(true)
                    .build())
            .tokenSettings(
                TokenSettings.builder()
                    .accessTokenTimeToLive(Duration.ofMinutes(5))
                    .authorizationCodeTimeToLive(Duration.ofMinutes(1))
                    .refreshTokenTimeToLive(Duration.ofHours(1))
                    .reuseRefreshTokens(false)
                    .build())
            .build());
    jdbc.update(
        "INSERT INTO identity_client_policy VALUES(?,?,?)", CLIENT_ID, tenant, AUDIENCE);
  }

  public void start() throws Exception {
    var args =
        List.of(
            "--spring.profiles.active=isolated-test",
            "--apex.identity.allow-loopback-http=true",
            "--apex.identity.issuer=" + issuer,
            "--apex.identity.tenant-id=" + tenant,
            "--apex.identity.key-file=" + keys.resolve("a.json"),
            "--apex.identity.active-kid=a",
            "--server.address=127.0.0.1",
            "--server.port=" + port,
            "--server.servlet.session.cookie.secure=false",
            "--spring.datasource.url=" + pg.getJdbcUrl("postgres", "postgres"),
            "--spring.datasource.username=postgres",
            "--spring.datasource.password=",
            "--spring.main.banner-mode=off");
    var command =
        new ArrayList<>(
            List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx256m",
                "-jar",
                identityJar.toString()));
    command.addAll(args);
    var pb = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile());
    pb.environment()
        .keySet()
        .removeIf(
            k ->
                k.matches(
                    "^(SPRING_|APEX_|IDENTITY_|SERVER_|JAVA_TOOL_OPTIONS|JDK_JAVA_OPTIONS|_JAVA_OPTIONS).*"));
    process = pb.start();
    children.add(process);
    await()
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(
            () -> {
              assertThat(process.isAlive()).as(() -> safeRead(log)).isTrue();
              assertThat(Files.readString(log)).contains("Started IdentityApplication");
            });
  }

  /** Authorization-code + PKCE + login + consent → access token for Console audience. */
  public String mintUserAccessToken() throws Exception {
    return mintUserAccessToken("alice", password);
  }

  public String mintBobAccessToken() throws Exception {
    return mintUserAccessToken("bob", bobPassword);
  }

  public String mintUserAccessToken(String loginName, String loginPassword) throws Exception {
    var browser =
        HttpClient.newBuilder()
            .cookieHandler(new java.net.CookieManager(null, java.net.CookiePolicy.ACCEPT_ALL))
            .connectTimeout(Duration.ofSeconds(3))
            .build();
    String verifier = UUID.randomUUID() + "-" + UUID.randomUUID();
    String nonce = UUID.randomUUID().toString();
    String state = UUID.randomUUID().toString();
    String challenge =
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(
                MessageDigest.getInstance("SHA-256")
                    .digest(verifier.getBytes(StandardCharsets.US_ASCII)));
    var fields =
        new LinkedHashMap<>(
            Map.of(
                "client_id",
                CLIENT_ID,
                "response_type",
                "code",
                "redirect_uri",
                OPS_REDIRECT,
                "scope",
                "openid profile ops.console.read",
                "state",
                state,
                "nonce",
                nonce,
                "code_challenge",
                challenge,
                "code_challenge_method",
                "S256"));
    var response = get(browser, issuer + "/oauth2/authorize?" + form(fields));
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
                loginName,
                "password",
                loginPassword,
                "_csrf",
                hidden(page.body(), "_csrf")),
            null);
    assertThat(logged.statusCode()).isEqualTo(302);
    response = get(browser, logged.headers().firstValue("location").orElseThrow());
    if (response.statusCode() == 200) {
      var consent = new LinkedHashMap<String, String>();
      consent.put("client_id", CLIENT_ID);
      consent.put("state", hidden(response.body(), "state"));
      consent.put("scope", "ops.console.read profile");
      response = post(browser, issuer + "/oauth2/authorize", consent, null);
    }
    assertThat(response.statusCode()).isEqualTo(302);
    var q = query(response.headers().firstValue("location").orElseThrow());
    assertThat(q.get("state")).isEqualTo(state);
    assertThat(q.get("code")).isNotBlank();
    var token =
        post(
            browser,
            issuer + "/oauth2/token",
            Map.of(
                "grant_type",
                "authorization_code",
                "client_id",
                CLIENT_ID,
                "redirect_uri",
                OPS_REDIRECT,
                "code",
                q.get("code"),
                "code_verifier",
                verifier),
            CLIENT_ID + ":" + secret);
    assertThat(token.statusCode()).isEqualTo(200);
    return JSON.readTree(token.body()).path("access_token").asText();
  }

  /**
   * Restart Identity with overlap JWKS: prior active key published as public-only + new private
   * active kid. Console must stay up; JWKS refresh is the under-test path.
   */
  public void rotateToNewSigningKey(String newKid) throws Exception {
    RSAKey next =
        new RSAKeyGenerator(2048)
            .keyID(newKid)
            .keyUse(KeyUse.SIGNATURE)
            .algorithm(JWSAlgorithm.RS256)
            .generate();
    Path file = keys.resolve(newKid + "-set.json");
    Files.writeString(
        file, new JWKSet(List.of(signingKey.toPublicJWK(), next)).toString(false));
    retiredSigningKey = signingKey;
    signingKey = next;
    activeKid = newKid;
    restartWithKeyFile(file, newKid);
  }

  /** Drop retired public key from JWKS (active private only). */
  public void publishActiveOnlyJwks() throws Exception {
    Path file = keys.resolve(activeKid + "-only.json");
    Files.writeString(file, new JWKSet(List.of(signingKey)).toString(false));
    restartWithKeyFile(file, activeKid);
  }

  private void restartWithKeyFile(Path keyFile, String kid) throws Exception {
    if (process != null) {
      process.destroy();
      if (!process.waitFor(20, TimeUnit.SECONDS)) {
        process.destroyForcibly();
        process.waitFor(10, TimeUnit.SECONDS);
      }
      children.remove(process);
      process = null;
    }
    Path restartLog = output.resolve("identity-node-" + kid + ".log");
    var args =
        List.of(
            "--spring.profiles.active=isolated-test",
            "--apex.identity.allow-loopback-http=true",
            "--apex.identity.issuer=" + issuer,
            "--apex.identity.tenant-id=" + tenant,
            "--apex.identity.key-file=" + keyFile,
            "--apex.identity.active-kid=" + kid,
            "--server.address=127.0.0.1",
            "--server.port=" + port,
            "--server.servlet.session.cookie.secure=false",
            "--spring.datasource.url=" + pg.getJdbcUrl("postgres", "postgres"),
            "--spring.datasource.username=postgres",
            "--spring.datasource.password=",
            "--spring.main.banner-mode=off");
    var command =
        new ArrayList<>(
            List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx256m",
                "-jar",
                identityJar.toString()));
    command.addAll(args);
    var pb =
        new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(restartLog.toFile());
    pb.environment()
        .keySet()
        .removeIf(
            k ->
                k.matches(
                    "^(SPRING_|APEX_|IDENTITY_|SERVER_|JAVA_TOOL_OPTIONS|JDK_JAVA_OPTIONS|_JAVA_OPTIONS).*"));
    process = pb.start();
    children.add(process);
    await()
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(
            () -> {
              assertThat(process.isAlive()).as(() -> safeRead(restartLog)).isTrue();
              assertThat(Files.readString(restartLog)).contains("Started IdentityApplication");
            });
  }

  /**
   * Offline-signed access JWT using a retained RSA key (for retired-kid / wrong-kid negatives).
   * Not an Identity issuance path.
   */
  public String mintOfflineAccessToken(RSAKey key, UUID subject, Duration ttl) throws Exception {
    var now = java.time.Instant.now();
    var header =
        new com.nimbusds.jose.JWSHeader.Builder(JWSAlgorithm.RS256)
            .keyID(key.getKeyID())
            .type(com.nimbusds.jose.JOSEObjectType.JWT)
            .build();
    var claims =
        new com.nimbusds.jwt.JWTClaimsSet.Builder()
            .issuer(issuer)
            .subject(subject.toString())
            .audience(AUDIENCE)
            .issueTime(java.util.Date.from(now))
            .expirationTime(java.util.Date.from(now.plus(ttl)))
            .notBeforeTime(java.util.Date.from(now.minusSeconds(1)))
            .claim("tenant_id", tenant.toString())
            .claim("principal_type", "user")
            .claim("scope", "ops.console.read")
            .claim("client_id", CLIENT_ID)
            .build();
    var jwt = new com.nimbusds.jwt.SignedJWT(header, claims);
    jwt.sign(new com.nimbusds.jose.crypto.RSASSASigner(key.toPrivateKey()));
    return jwt.serialize();
  }

  public Map<String, Object> publicDescriptor() {
    return Map.of(
        "issuer",
        issuer,
        "tenantId",
        tenant.toString(),
        "clientId",
        CLIENT_ID,
        "audience",
        AUDIENCE,
        "redirectUri",
        OPS_REDIRECT,
        "loginName",
        "alice",
        "port",
        port,
        "pid",
        process == null ? -1 : process.pid(),
        "activeKid",
        activeKid);
  }

  public long pid() {
    return process == null ? -1 : process.pid();
  }

  @Override
  public void close() throws Exception {
    var errors = new ArrayList<Exception>();
    for (var p : children) {
      try {
        p.destroy();
        if (!p.waitFor(10, TimeUnit.SECONDS)) {
          p.destroyForcibly();
          p.waitFor(10, TimeUnit.SECONDS);
        }
      } catch (Exception e) {
        errors.add(e);
      }
    }
    try {
      pg.close();
    } catch (Exception e) {
      errors.add(e);
    }
    try {
      try (var files = Files.list(keys)) {
        for (var path : files.toList()) Files.deleteIfExists(path);
      }
      Files.deleteIfExists(keys);
    } catch (Exception e) {
      errors.add(e);
    }
    if (!errors.isEmpty()) throw errors.getFirst();
  }

  static int freePort() throws Exception {
    try (var s = new java.net.ServerSocket(0)) {
      return s.getLocalPort();
    }
  }

  static String enc(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  static String form(Map<String, String> fields) {
    return fields.entrySet().stream()
        .map(e -> enc(e.getKey()) + "=" + enc(e.getValue()))
        .reduce((a, b) -> a + "&" + b)
        .orElse("");
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

  static HttpResponse<String> get(HttpClient client, String url) throws Exception {
    return client.send(
        HttpRequest.newBuilder(URI.create(url))
            .header("Accept", "text/html")
            .timeout(Duration.ofSeconds(15))
            .GET()
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }

  static HttpResponse<String> post(
      HttpClient client, String url, Map<String, String> data, String basic) throws Exception {
    var r =
        HttpRequest.newBuilder(URI.create(url))
            .timeout(Duration.ofSeconds(15))
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
    return client.send(
        r.POST(HttpRequest.BodyPublishers.ofString(encoded)).build(),
        HttpResponse.BodyHandlers.ofString());
  }

  static String safeRead(Path log) {
    try {
      if (!Files.isRegularFile(log)) return "<missing>";
      String text = Files.readString(log);
      return text.substring(0, Math.min(text.length(), 4000));
    } catch (Exception e) {
      return "<unreadable>";
    }
  }
}
