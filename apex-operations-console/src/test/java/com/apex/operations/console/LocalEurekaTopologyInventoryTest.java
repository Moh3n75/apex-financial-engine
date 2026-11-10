package com.apex.operations.console;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import com.apex.operations.contracts.TopologySnapshot;
import com.apex.operations.contracts.TopologySnapshot.*;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.EurekaClientConfig;
import com.netflix.discovery.shared.Application;
import com.netflix.discovery.shared.Applications;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.mock.env.MockEnvironment;

class LocalEurekaTopologyInventoryTest {
  final DiscoveryClient client = mock(DiscoveryClient.class);
  final EurekaClientConfig config = mock(EurekaClientConfig.class);
  final DefaultListableBeanFactory beans = new DefaultListableBeanFactory();
  final MockEnvironment env = new MockEnvironment().withProperty("apex.ops.local-inventory.environment", "local");

  LocalEurekaTopologyInventoryTest() {
    beans.registerSingleton("eureka", client); beans.registerSingleton("config", config);
    when(config.getEurekaServerServiceUrls("defaultZone")).thenReturn(List.of("http://localhost:8761/eureka/"));
    when(client.getLastSuccessfulRegistryFetchTimePeriod()).thenReturn(1000L);
  }
  LocalEurekaTopologyInventory inventory() {
    return new LocalEurekaTopologyInventory(beans.getBeanProvider(EurekaClient.class), beans.getBeanProvider(EurekaClientConfig.class), env);
  }
  TopologySnapshot base() {
    return ContractTopology.join(TopologyApi.project(List.of(), Instant.now(), (e,o) -> true), List.of(), (e,o) -> true);
  }
  InstanceInfo instance(String app, String id, InstanceStatus status, Map<String,String> metadata) {
    return InstanceInfo.Builder.newBuilder().setAppName(app).setInstanceId(id).setHostName("private-host")
        .setIPAddr("10.0.0.1").setStatus(status).setMetadata(metadata).build();
  }
  Applications apps(InstanceInfo... instances) {
    var apps = new Applications();
    var groups = new LinkedHashMap<String, Application>();
    for (var instance : instances) groups.computeIfAbsent(instance.getAppName(), Application::new).addInstance(instance);
    groups.values().forEach(apps::addApplication); return apps;
  }

  @Test void upServicesAreAggregatedWithoutOwnersEndpointsEpochsHealthOrEdges() {
    when(client.getApplications()).thenReturn(apps(
        instance("ORDER-CELL", "one", InstanceStatus.UP, Map.of("private", "secret")),
        instance("ORDER-CELL", "two", InstanceStatus.UP, Map.of()),
        instance("PAYMENT-CELL", "three", InstanceStatus.DOWN, Map.of()),
        instance("APEX-API-GATEWAY", "four", InstanceStatus.UP, Map.of())));
    var graph = inventory().join(base(), (e,o) -> true);
    assertThat(graph.schemaVersion()).isEqualTo(2);
    assertThat(graph.nodes()).extracting(Node::id).containsExactly("local:service:apex-api-gateway", "local:service:order-cell");
    assertThat(graph.nodes()).allSatisfy(n -> {
      assertThat(n.kind()).isEqualTo("SERVICE"); assertThat(n.health()).isEqualTo("UNKNOWN");
      assertThat(n.epoch()).isNull(); assertThat(n.instanceId()).isNull();
      assertThat(n.evidence()).singleElement().satisfies(e -> {
        assertThat(e.source()).isEqualTo("EUREKA"); assertThat(e.kind()).isEqualTo("OBSERVED");
        assertThat(e.lastSeen()).isBefore(e.collectedAt());
      });
    });
    assertThat(graph.connections()).isEmpty(); assertThat(graph.memberships()).isEmpty();
    assertThat(graph.toString()).doesNotContain("private-host", "10.0.0.1", "secret");
    assertThat(graph.providers().getLast().status()).isEqualTo("AVAILABLE");
    verify(client, never()).getInstancesByVipAddress(anyString(), anyBoolean());
    var presence = graph.nodes().getFirst().evidence();
    assertThatThrownBy(() -> new Connection("invented", graph.nodes().getFirst().id(), graph.nodes().getFirst().id(),
        "local", "HTTP", "SERVICE", "UNKNOWN", "UNKNOWN", presence.getFirst().lastSeen(), presence))
        .hasMessage("REGISTRY_PRESENCE_IS_NOT_TRAFFIC");
  }
  @Test void removalAndEmptyRegistryReplacePresenceWhileStaleCacheRemainsExplicit() {
    when(client.getApplications()).thenReturn(apps(instance("ORDER-CELL", "one", InstanceStatus.UP, Map.of())));
    assertThat(inventory().join(base(), (e,o)->true).nodes()).hasSize(1);
    when(client.getLastSuccessfulRegistryFetchTimePeriod()).thenReturn(61000L);
    var stale = inventory().join(base(), (e,o)->true);
    assertThat(stale.nodes().getFirst().freshness()).isEqualTo("STALE");
    assertThat(stale.providers().getLast().status()).isEqualTo("STALE");
    when(client.getApplications()).thenReturn(new Applications());
    when(client.getLastSuccessfulRegistryFetchTimePeriod()).thenReturn(0L);
    var empty = inventory().join(base(), (e,o)->true);
    assertThat(empty.nodes()).isEmpty(); assertThat(empty.providers().getLast().status()).isEqualTo("AVAILABLE");
  }
  @Test void absentClientUnknownAgeAndTransportErrorsAreTruthfulNotFixtures() {
    when(client.getLastSuccessfulRegistryFetchTimePeriod()).thenReturn(Long.MAX_VALUE);
    assertThat(inventory().join(base(), (e,o)->true).providers().getLast().status()).isEqualTo("SOURCE_UNAVAILABLE");
    when(client.getLastSuccessfulRegistryFetchTimePeriod()).thenReturn(100L);
    when(client.getApplications()).thenThrow(new IllegalStateException("private endpoint secret"));
    var failed = inventory().join(base(), (e,o)->true);
    assertThat(failed.nodes()).isEmpty(); assertThat(failed.toString()).doesNotContain("private endpoint secret");
    beans.destroySingleton("eureka");
    assertThat(inventory().join(base(), (e,o)->true).providers().getLast().status()).isEqualTo("SOURCE_UNAVAILABLE");
  }
  @Test void visibilityAndEnvironmentAreAppliedBeforeAggregation() {
    when(client.getApplications()).thenReturn(apps(
        instance("ORDER-CELL", "one", InstanceStatus.UP, Map.of()),
        instance("PAYMENT-CELL", "two", InstanceStatus.UP, Map.of()),
        instance("CUSTOMER-CELL", "three", InstanceStatus.UP, Map.of("apex.environment", "prod"))));
    var graph = inventory().join(base(), (e,o)->e.equals("local") && o.equals("order-cell"));
    assertThat(graph.nodes()).extracting(Node::name).containsExactly("order-cell");
  }
  @Test void mergesProvenanceWithoutChangingDeclaredEdgesOrExistingFreshness() {
    var declaration = new Evidence("DECLARED", "CONFIGURATION", "test declaration", Instant.now(), null, "SERVICE");
    var original = new Node("local:service:order-cell", "SERVICE", "order-cell", "local", "local:service:order-cell",
        null, null, "SERVICE", "UNKNOWN", "STALE", "MANIFEST_STALE", null, List.of(declaration));
    var base = base();
    base = new TopologySnapshot(2, base.asOf(), base.coverage(), base.capabilities(), List.of(original), List.of(),
        base.providers(), base.unresolved(), base.descriptors(), base.memberships());
    when(client.getApplications()).thenReturn(apps(instance("ORDER-CELL", "one", InstanceStatus.UP, Map.of())));
    var graph = inventory().join(base, (e,o)->true);
    assertThat(graph.nodes()).singleElement().satisfies(n -> {
      assertThat(n.freshness()).isEqualTo("STALE"); assertThat(n.evidence()).extracting(Evidence::source).containsExactly("CONFIGURATION", "EUREKA");
    });
    assertThat(graph.connections()).isEqualTo(base.connections()); assertThat(graph.memberships()).isEqualTo(base.memberships());
  }
  @Test void localScopeAndLoopbackAreRequiredAndProviderIsOptIn() {
    env.withProperty("apex.ops.local-inventory.environment", "prod");
    assertThatThrownBy(this::inventory).hasMessageContaining("LOCAL_INVENTORY_REQUIRES");
    env.withProperty("apex.ops.local-inventory.environment", "local").withProperty("server.address", "0.0.0.0");
    assertThatThrownBy(this::inventory).hasMessageContaining("LOOPBACK");
    when(config.getEurekaServerServiceUrls("defaultZone")).thenReturn(List.of("http://remote.internal:8761/eureka/"));
    assertThat(LocalEurekaTopologyInventory.loopbackRegistry(config)).isFalse();
    new ApplicationContextRunner().withUserConfiguration(LocalEurekaTopologyInventory.class)
        .run(ctx -> assertThat(ctx).doesNotHaveBean(LocalEurekaTopologyInventory.class));
    new ApplicationContextRunner().withPropertyValues("spring.profiles.active=prod", "apex.ops.local-inventory.enabled=true")
        .withUserConfiguration(LocalEurekaTopologyInventory.class)
        .run(ctx -> assertThat(ctx).doesNotHaveBean(LocalEurekaTopologyInventory.class));
  }
  @Test void graphLimitReportsPartialRatherThanDroppingExistingNodes() {
    var instances = new ArrayList<InstanceInfo>();
    for (int i=0; i<205; i++) instances.add(instance("SERVICE-"+i, "one", InstanceStatus.UP, Map.of()));
    when(client.getApplications()).thenReturn(apps(instances.toArray(InstanceInfo[]::new)));
    var graph = inventory().join(base(), (e,o)->true);
    assertThat(graph.nodes()).hasSize(200); assertThat(graph.providers().getLast().status()).isEqualTo("PARTIAL");
  }

  @Test void preservesExistingConnectionEndpointsAndEpochVerifiedMembershipDuringMerge() {
    var now = Instant.now(); var epoch = UUID.randomUUID();
    var cache = new EvidenceCache();
    cache.accept(new com.apex.operations.contracts.OwnerSnapshot(1, "local", "order-cell", "one", epoch, now,
        1000, "EXPLICIT_KEYS_PARTIAL_NO_HISTORY", List.of(), List.of(), false, 0, 0), now);
    var prior = ContractTopology.join(TopologyApi.project(List.of(cache.view("order-cell", now)), now, (e,o)->true), List.of(), (e,o)->true);
    var instance = prior.nodes().stream().filter(n -> n.kind().equals("INSTANCE")).findFirst().orElseThrow();
    var member = new RuntimeMember(instance.id(), "local", "order-cell", "order-cell", "one", epoch,
        "test-registry-id", "test-registry", "EUREKA", now, "FRESH", "VERIFIED", "SCOPED_REGISTRY_COMPLETE", "AVAILABLE");
    prior = new TopologySnapshot(2, prior.asOf(), prior.coverage(), prior.capabilities(), prior.nodes(), prior.connections(),
        prior.providers(), prior.unresolved(), prior.descriptors(), List.of(member));
    when(client.getApplications()).thenReturn(apps(instance("ORDER-CELL", "no-epoch", InstanceStatus.UP, Map.of()),
        instance("INVENTORY-CELL", "another", InstanceStatus.UP, Map.of())));
    var graph = inventory().join(prior, (e,o)->true);
    assertThat(graph.nodes()).hasSize(prior.nodes().size()+1);
    assertThat(graph.connections()).isEqualTo(prior.connections()).hasSize(1);
    assertThat(graph.memberships()).containsExactly(member);
    assertThat(graph.nodes().stream().filter(n -> n.kind().equals("INSTANCE"))).containsExactly(instance);
  }
}
