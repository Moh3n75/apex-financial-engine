package com.apex.operations.console.security.m36b;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.web.server.ResponseStatusException;

/**
 * Fail-closed deployment-scoped resource grants. Does not implement tenant row isolation on owner
 * SQL.
 */
public final class OpsM36bResourceAuthorizer {
  public static final String REASON_ALLOWED = "ALLOWED";
  public static final String REASON_UNAUTHENTICATED = "UNAUTHENTICATED";
  public static final String REASON_DENY_GRANT = "DENY_GRANT";
  public static final String REASON_UNSUPPORTED_TENANT_ROW = "UNSUPPORTED_TENANT_ROW";

  private final OpsM36bProperties props;
  private final OpsM36bAccessAudit audit;

  public OpsM36bResourceAuthorizer(OpsM36bProperties props, OpsM36bAccessAudit audit) {
    this.props = props;
    this.audit = audit;
  }

  public void require(
      String environment, String owner, String kind, String scope, String correlationId) {
    if (!props.isResourceAuthEnabled()) {
      return;
    }
    Jwt jwt = currentJwt();
    if (jwt == null) {
      audit(null, null, environment, owner, kind, scope, false, REASON_UNAUTHENTICATED, correlationId);
      // Same status for known/unknown owners — no existence disclosure.
      throw new ResponseStatusException(HttpStatus.FORBIDDEN);
    }
    String sub = jwt.getSubject();
    String tenant = claimAsString(jwt, "tenant_id");
    if (environment == null
        || environment.isBlank()
        || owner == null
        || owner.isBlank()
        || kind == null
        || kind.isBlank()
        || scope == null
        || scope.isBlank()) {
      audit(sub, tenant, environment, owner, kind, scope, false, REASON_DENY_GRANT, correlationId);
      throw new ResponseStatusException(HttpStatus.FORBIDDEN);
    }
    boolean allowed =
        props.getResourceGrants().stream()
            .anyMatch(g -> matches(g, sub, environment, owner, kind, scope));
    audit(
        sub,
        tenant,
        environment,
        owner,
        kind,
        scope,
        allowed,
        allowed ? REASON_ALLOWED : REASON_DENY_GRANT,
        correlationId);
    if (!allowed) {
      throw new ResponseStatusException(HttpStatus.FORBIDDEN);
    }
  }

  /** Overview/sources: only owners the principal may see under OVERVIEW or any granted kind. */
  public boolean ownerVisible(String environment, String owner) {
    if (!props.isResourceAuthEnabled()) {
      return true;
    }
    Jwt jwt = currentJwt();
    if (jwt == null) {
      return false;
    }
    String sub = jwt.getSubject();
    return props.getResourceGrants().stream()
        .anyMatch(
            g ->
                subjectMatches(g.getSubject(), sub)
                    && eq(g.getEnvironment(), environment)
                    && eq(g.getOwner(), owner));
  }

  public void denyUnsupportedTenantRowFilter(String correlationId) {
    Jwt jwt = currentJwt();
    String sub = jwt == null ? null : jwt.getSubject();
    String tenant = jwt == null ? null : claimAsString(jwt, "tenant_id");
    audit(
        sub,
        tenant,
        "",
        "",
        "TENANT_ROW",
        "",
        false,
        REASON_UNSUPPORTED_TENANT_ROW,
        correlationId);
    throw new ResponseStatusException(HttpStatus.FORBIDDEN);
  }

  static boolean matches(
      OpsM36bProperties.ResourceGrant g,
      String sub,
      String environment,
      String owner,
      String kind,
      String scope) {
    if (!subjectMatches(g.getSubject(), sub)) {
      return false;
    }
    if (!eq(g.getEnvironment(), environment) || !eq(g.getOwner(), owner)) {
      return false;
    }
    Set<String> kinds = splitUpper(g.getKinds());
    Set<String> scopes = splitLower(g.getScopes());
    return kinds.contains(kind.trim().toUpperCase(Locale.ROOT))
        && scopes.contains(scope.trim().toLowerCase(Locale.ROOT));
  }

  static boolean subjectMatches(String grantSubject, String sub) {
    if (grantSubject == null || grantSubject.isBlank() || sub == null) {
      return false;
    }
    if ("*".equals(grantSubject.trim())) {
      return true;
    }
    return grantSubject.trim().equals(sub);
  }

  static Set<String> splitUpper(String raw) {
    if (raw == null || raw.isBlank()) {
      return Set.of();
    }
    return Arrays.stream(raw.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .map(s -> s.toUpperCase(Locale.ROOT))
        .collect(Collectors.toSet());
  }

  static Set<String> splitLower(String raw) {
    if (raw == null || raw.isBlank()) {
      return Set.of();
    }
    return Arrays.stream(raw.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .map(s -> s.toLowerCase(Locale.ROOT))
        .collect(Collectors.toSet());
  }

  private static boolean eq(String a, String b) {
    return a != null && b != null && a.trim().equalsIgnoreCase(b.trim());
  }

  private Jwt currentJwt() {
    Authentication auth = SecurityContextHolder.getContext().getAuthentication();
    if (auth instanceof JwtAuthenticationToken token) {
      return token.getToken();
    }
    return null;
  }

  private void audit(
      String sub,
      String tenant,
      String environment,
      String owner,
      String kind,
      String scope,
      boolean allowed,
      String reason,
      String correlationId) {
    if (!props.isAuditEnabled()) {
      return;
    }
    try {
      audit.record(
          new OpsM36bAccessAudit.Entry(
              Instant.now(),
              hashSub(sub),
              tenant == null ? "" : tenant,
              nullToEmpty(environment),
              nullToEmpty(owner),
              nullToEmpty(kind),
              nullToEmpty(scope),
              allowed,
              reason,
              nullToEmpty(correlationId)));
    } catch (RuntimeException ignored) {
      // Audit must not break authorization path after decision.
    }
  }

  static String hashSub(String sub) {
    if (sub == null || sub.isBlank()) {
      return "";
    }
    try {
      byte[] dig =
          MessageDigest.getInstance("SHA-256").digest(sub.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(dig).substring(0, 16);
    } catch (Exception e) {
      return "hash-unavailable";
    }
  }

  private static String claimAsString(Jwt jwt, String name) {
    Object v = jwt.getClaim(name);
    return v == null ? null : v.toString();
  }

  private static String nullToEmpty(String v) {
    return v == null ? "" : v;
  }

  public static String newCorrelationId() {
    return UUID.randomUUID().toString();
  }
}
