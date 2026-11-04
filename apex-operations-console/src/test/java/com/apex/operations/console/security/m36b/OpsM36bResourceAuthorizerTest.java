package com.apex.operations.console.security.m36b;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.web.server.ResponseStatusException;

class OpsM36bResourceAuthorizerTest {
  @AfterEach
  void clear() {
    SecurityContextHolder.clearContext();
  }

  @Test
  void allowsExactGrantAndDeniesOtherOwnerWithoutDisclosureDifference() {
    var props = props("user-a", "order-cell", "OUTBOX_EVENT", "owner");
    var audit = new InMemoryOpsM36bAccessAudit();
    var auth = new OpsM36bResourceAuthorizer(props, audit);
    authenticate("user-a", "11111111-1111-1111-1111-111111111111");
    auth.require("test", "order-cell", "OUTBOX_EVENT", "owner", "c1");
    assertThatThrownBy(() -> auth.require("test", "payment-cell", "OUTBOX_EVENT", "owner", "c2"))
        .isInstanceOf(ResponseStatusException.class)
        .extracting(ex -> ((ResponseStatusException) ex).getStatusCode())
        .isEqualTo(HttpStatus.FORBIDDEN);
    assertThat(audit.snapshot()).hasSize(2);
    assertThat(audit.snapshot().get(0).allowed()).isTrue();
    assertThat(audit.snapshot().get(1).allowed()).isFalse();
    assertThat(audit.snapshot().get(1).reasonCode())
        .isEqualTo(OpsM36bResourceAuthorizer.REASON_DENY_GRANT);
  }

  @Test
  void starSubjectGrantsDeploymentOperator() {
    var props = props("*", "inventory-cell", "WORKFLOW", "reservation");
    var auth = new OpsM36bResourceAuthorizer(props, new InMemoryOpsM36bAccessAudit());
    authenticate("anyone", "11111111-1111-1111-1111-111111111111");
    auth.require("test", "inventory-cell", "WORKFLOW", "reservation", "c");
  }

  @Test
  void unsupportedTenantRowFilterIsForbidden() {
    var props = props("*", "order-cell", "OUTBOX_EVENT", "owner");
    var auth = new OpsM36bResourceAuthorizer(props, new InMemoryOpsM36bAccessAudit());
    authenticate("user-a", "11111111-1111-1111-1111-111111111111");
    assertThatThrownBy(() -> auth.denyUnsupportedTenantRowFilter("c"))
        .isInstanceOf(ResponseStatusException.class);
  }

  private static OpsM36bProperties props(String subject, String owner, String kind, String scope) {
    var props = new OpsM36bProperties();
    props.setResourceAuthEnabled(true);
    var g = new OpsM36bProperties.ResourceGrant();
    g.setSubject(subject);
    g.setEnvironment("test");
    g.setOwner(owner);
    g.setKinds(kind);
    g.setScopes(scope);
    props.getResourceGrants().add(g);
    return props;
  }

  private static void authenticate(String sub, String tenant) {
    Instant now = Instant.now();
    Jwt jwt =
        new Jwt(
            "t",
            now,
            now.plusSeconds(60),
            Map.of("alg", "none"),
            Map.of(
                "sub",
                sub,
                "tenant_id",
                tenant,
                "principal_type",
                "user",
                "scope",
                "ops.console.read",
                "aud",
                List.of("apex-operations-console")));
    SecurityContextHolder.getContext()
        .setAuthentication(new JwtAuthenticationToken(jwt, List.of()));
  }
}
