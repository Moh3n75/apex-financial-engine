package com.apex.operations.console;

import static org.assertj.core.api.Assertions.*;
import com.apex.operations.contracts.OwnerSnapshot;
import com.apex.operations.console.security.m36b.*;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;

class TopologyApiTest {
  private final Instant now = Instant.parse("2026-11-09T10:00:00Z");

  private EvidenceCache.View view(String env, String owner, String instance, UUID epoch, Instant seen) {
    var cache = new EvidenceCache();
    cache.accept(new OwnerSnapshot(1, env, owner, instance, epoch, seen, 1000,
        "EXPLICIT_KEYS_PARTIAL_NO_HISTORY", List.of(), List.of(), false, 0, 0), seen);
    return cache.view(owner, now);
  }

  @Test void missingOwnerIsDeclaredWithoutInventingInstanceHealthOrTraffic() {
    var graph = TopologyApi.project(List.of(new EvidenceCache().view("order-cell", now)), now, (e,o) -> true);
    assertThat(graph.nodes()).hasSize(2).allSatisfy(n -> {
      assertThat(n.environment()).isEqualTo("unknown");
      assertThat(n.health()).isEqualTo("UNKNOWN");
      assertThat(n.lastSeen()).isNull();
    });
    assertThat(graph.connections()).singleElement().satisfies(edge -> {
      assertThat(edge.protocol()).isEqualTo("GRPC");
      assertThat(edge.lastSeen()).isNull();
      assertThat(edge.evidence()).allSatisfy(e -> assertThat(e.kind()).isEqualTo("DECLARED"));
    });
    assertThat(graph.capabilities().observedInteractions()).isEqualTo("NOT_CONFIGURED");
  }

  @Test void staleInstanceRetainsEpochAndLastSeenWithoutPromotingHealth() {
    UUID epoch = UUID.randomUUID();
    Instant seen = now.minusSeconds(10);
    var graph = TopologyApi.project(List.of(view("local", "order-cell", "one", epoch, seen)), now, (e,o) -> true);
    assertThat(graph.nodes().stream().filter(n -> n.kind().equals("INSTANCE"))).singleElement().satisfies(n -> {
      assertThat(n.id()).contains(epoch.toString());
      assertThat(n.freshness()).isEqualTo("STALE");
      assertThat(n.lastSeen()).isEqualTo(seen);
      assertThat(n.health()).isEqualTo("UNKNOWN");
      assertThat(n.evidence().getFirst().kind()).isEqualTo("OBSERVED");
    });
    assertThat(graph.connections().getFirst().lastSeen()).isNull();
  }

  @Test void authorizationUsesActualEnvironmentAndRemovesHiddenNodesAndEdges() {
    var props = new OpsM36bProperties();
    props.setResourceAuthEnabled(true);
    var auth = new OpsM36bResourceAuthorizer(props, new InMemoryOpsM36bAccessAudit());
    var graph = TopologyApi.project(List.of(view("prod", "order-cell", "one", UUID.randomUUID(), now)),
        now, auth::ownerVisible);
    assertThat(graph.nodes()).isEmpty();
    assertThat(graph.connections()).isEmpty();

    var views = List.of(view("test", "order-cell", "one", UUID.randomUUID(), now),
        view("prod", "order-cell", "one", UUID.randomUUID(), now));
    graph = TopologyApi.project(views, now, (e,o) -> e.equals("test"));
    assertThat(graph.nodes()).hasSize(3).allSatisfy(n -> assertThat(n.environment()).isEqualTo("test"));
    var ids = graph.nodes().stream().map(com.apex.operations.contracts.TopologySnapshot.Node::id).toList();
    assertThat(graph.connections()).singleElement().satisfies(edge -> {
      assertThat(ids).contains(edge.source(), edge.target());
    });
    var grant = new OpsM36bProperties.ResourceGrant();
    grant.setSubject("operator"); grant.setEnvironment("prod"); grant.setOwner("order-cell");
    grant.setKinds("OVERVIEW"); grant.setScopes("owner"); props.getResourceGrants().add(grant);
    var jwt = Jwt.withTokenValue("test").header("alg", "none").subject("operator").build();
    SecurityContextHolder.getContext().setAuthentication(new JwtAuthenticationToken(jwt));
    try {
      graph = TopologyApi.project(views, now, auth::ownerVisible);
      assertThat(graph.nodes()).hasSize(3).allSatisfy(n -> assertThat(n.environment()).isEqualTo("prod"));
      assertThat(TopologyApi.project(List.of(new EvidenceCache().view("order-cell", now)), now,
          auth::ownerVisible).nodes()).isEmpty();
    } finally { SecurityContextHolder.clearContext(); }
  }

  @Test void conflictAndClockUncertaintyArePreservedAndEmptyInventoryStaysEmpty() {
    var row = view("local", "order-cell", "one", UUID.randomUUID(), now.plusSeconds(10));
    row = new EvidenceCache.View(row.source(), row.receivedAt(), row.snapshot(), row.freshness(), "CONFLICTING", List.of());
    var graph = TopologyApi.project(List.of(row), now, (e,o) -> true);
    assertThat(graph.nodes().stream().filter(n -> n.kind().equals("INSTANCE"))).singleElement().satisfies(n -> {
      assertThat(n.freshness()).isEqualTo("CLOCK_UNCERTAIN");
      assertThat(n.uncertainty()).isEqualTo("CONFLICTING");
    });
    assertThat(TopologyApi.project(List.of(), now, (e,o) -> true).nodes()).isEmpty();
  }
}
