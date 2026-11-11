package com.apex.operations.console;

import static org.assertj.core.api.Assertions.*;
import com.apex.operations.contracts.*;
import com.apex.operations.contracts.ContractManifest.*;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;

class ContractTopologyTest {
  private final Instant now = Instant.parse("2026-11-09T10:00:00Z");
  private ContractManifest manifest(String env, String service, String cluster, String topic, int version, boolean producer) {
    var contract = new Contract("SAMPLE", version);
    return new ContractManifest(1, env, service, service, "one", UUID.randomUUID(), now, "FIXTURE", cluster,
        producer ? List.of(new Publication(contract, topic, "PUBLISHER_ROUTING")) : List.of(),
        producer ? List.of() : List.of(topic), producer ? "" : "sample-group",
        producer ? List.of() : List.of(contract), List.of(new Rpc("QUERY", contract)), List.of());
  }
  private ContractDiscoveryPoller.View view(ContractManifest m) { return new ContractDiscoveryPoller.View(m.environment(), m.service(), m, "AVAILABLE"); }
  private com.apex.operations.contracts.TopologySnapshot join(List<ContractManifest> manifests, java.util.function.BiPredicate<String,String> auth) {
    return ContractTopology.join(TopologyApi.project(List.of(), now, auth), manifests.stream().map(this::view).toList(), auth);
  }
  @Test void exactMatchUsesExplicitTopicAndGroupNodesAndNeverInventsGrpcCallers() {
    var graph = join(List.of(manifest("test", "producer", "cluster", "events", 1, true), manifest("test", "consumer", "cluster", "events", 1, false)), (e,o) -> true);
    assertThat(graph.nodes()).extracting(n -> n.kind()).contains("CELL", "SERVICE", "KAFKA_TOPIC", "CONSUMER_GROUP");
    assertThat(graph.connections()).hasSize(3).allSatisfy(edge -> {
      assertThat(edge.protocol()).isEqualTo("KAFKA"); assertThat(edge.lastSeen()).isNull();
      assertThat(edge.evidence()).allSatisfy(proof -> assertThat(proof.kind()).isEqualTo("DECLARED"));
    });
    assertThat(graph.descriptors()).hasSize(3);
    assertThat(graph.unresolved()).allSatisfy(u->assertThat(u.reason()).isEqualTo("NO_VISIBLE_GRPC_CALLER_DECLARATION"));
    assertThat(graph.providers()).anySatisfy(p -> assertThat(p.sourceMode()).isEqualTo("FIXTURE"));
  }
  @Test void typeVersionTopicEnvironmentAndClusterMustAllMatch() {
    var producer = manifest("test", "producer", "cluster", "events", 1, true);
    for (var consumer : List.of(manifest("test", "consumer", "cluster", "events", 2, false),
        manifest("test", "consumer", "cluster", "other", 1, false),
        manifest("prod", "consumer", "cluster", "events", 1, false),
        manifest("test", "consumer", "other", "events", 1, false))) {
      var graph = join(List.of(producer, consumer), (e,o) -> true);
      assertThat(graph.connections()).hasSize(1);
      assertThat(graph.unresolved()).anySatisfy(u -> assertThat(u.reason()).contains("NO_VISIBLE_EXACT"));
    }
    assertThat(join(List.of(manifest("test", "producer", "unknown", "events", 1, true),
        manifest("test", "consumer", "unknown", "events", 1, false)), (e,o) -> true).connections()).isEmpty();
  }
  @Test void hiddenProducerCannotAffectConsumerLinksOrProvenance() {
    var graph = join(List.of(manifest("test", "hidden", "cluster", "events", 1, true),
        manifest("test", "consumer", "cluster", "events", 1, false)), (e,o) -> !o.equals("hidden"));
    assertThat(graph.connections()).isEmpty();
    assertThat(graph.nodes()).noneSatisfy(n -> assertThat(n.id()).contains("hidden"));
    assertThat(graph.providers()).noneSatisfy(p -> assertThat(p.name()).contains("hidden"));
    assertThat(graph.unresolved()).allSatisfy(u -> assertThat(u.reason()).doesNotContain("hidden"));
  }
  @Test void contractRefreshPreservesExistingSnapshotUncertaintyAndFreshness() {
    var snapshot = new OwnerSnapshot(1, "test", "producer", "one", UUID.randomUUID(), now.minusSeconds(10),
        1000, "EXPLICIT_KEYS_PARTIAL_NO_HISTORY", List.of(), List.of(), false, 0, 0);
    var view = new EvidenceCache.View("producer", now.minusSeconds(10), snapshot, "STALE", "CONFLICTING", List.of());
    var base = TopologyApi.project(List.of(view), now, (e,o) -> true);
    var graph = ContractTopology.join(base, List.of(view(manifest("test", "producer", "cluster", "events", 1, true))), (e,o) -> true);
    assertThat(graph.nodes().stream().filter(n -> n.id().equals("test:service:producer"))).singleElement().satisfies(n -> {
      assertThat(n.freshness()).isEqualTo("STALE"); assertThat(n.uncertainty()).isEqualTo("CONFLICTING");
      assertThat(n.evidence()).hasSize(2);
    });
    assertThat(graph.nodes().stream().filter(n -> n.kind().equals("INSTANCE"))).hasSize(1);
  }
  @Test void pollerRejectsCrossEnvironmentAndStaticManifestsAsLiveProviderResults() {
    var env = new org.springframework.mock.env.MockEnvironment()
        .withProperty("apex.ops.contract-discovery.owners", "producer")
        .withProperty("apex.ops.contract-discovery.environment", "test");
    var beans = new org.springframework.beans.factory.support.DefaultListableBeanFactory();
    beans.registerSingleton("gateway", org.mockito.Mockito.mock(com.apex.platform.grpc.api.QueryGateway.class));
    try (var poller = new ContractDiscoveryPoller(env, beans.getBeanProvider(com.apex.platform.grpc.api.QueryGateway.class))) {
      assertThatThrownBy(() -> poller.accept("producer", manifest("prod", "producer", "cluster", "events", 1, true), now)).isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> poller.accept("producer", manifest("test", "producer", "cluster", "events", 1, true), now)).isInstanceOf(IllegalArgumentException.class);
      assertThat(poller.views().getFirst().manifest()).isNull();
    }
  }
}
