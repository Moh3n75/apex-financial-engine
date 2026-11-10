package com.apex.operations.console;

import com.apex.operations.console.security.m36b.OpsM36bResourceAuthorizer;
import com.apex.operations.contracts.TopologySnapshot;
import com.apex.operations.contracts.TopologySnapshot.*;
import java.time.Instant;
import java.util.*;
import java.util.function.BiPredicate;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.web.bind.annotation.*;

/** Bounded projection of configured owner reads, not a service-discovery or traffic graph. */
@RestController
public final class TopologyApi {
  private final OwnerPoller owners;
  private final ObjectProvider<OpsM36bResourceAuthorizer> authorizer;
  private final ObjectProvider<ContractDiscoveryPoller> discovery;
  private final ObjectProvider<RuntimeMembershipPoller> membership;
  private final String environment;

  public TopologyApi(OwnerPoller owners, ObjectProvider<OpsM36bResourceAuthorizer> authorizer,
      ObjectProvider<ContractDiscoveryPoller> discovery,ObjectProvider<RuntimeMembershipPoller> membership,org.springframework.core.env.Environment env) {
    this.owners = owners;
    this.authorizer = authorizer;
    this.discovery = discovery;
    this.membership=membership;this.environment=env.getProperty("apex.ops.contract-discovery.environment","unknown");
  }

  @GetMapping("/ops/v1/topology")
  public TopologySnapshot topology(@RequestParam(defaultValue = "1") int schemaVersion) {
    if (schemaVersion != 1 && schemaVersion != 2) throw new org.springframework.web.server.ResponseStatusException(org.springframework.http.HttpStatus.BAD_REQUEST);
    var auth = authorizer.getIfAvailable();
    BiPredicate<String, String> visible = (environment, owner) -> auth == null || auth.ownerVisible(environment, owner);
    var base = project(owners.views(), Instant.now(), visible);
    var provider = discovery.getIfAvailable();
    var runtime=membership.getIfAvailable();
    if(schemaVersion==2&&runtime!=null)return RuntimeTopology.project(base,runtime.views(),environment,visible);
    return schemaVersion == 1 ? base : ContractTopology.join(base, provider == null ? List.of() : provider.views(), visible);
  }

  static TopologySnapshot project(List<EvidenceCache.View> views, Instant now,
                          BiPredicate<String, String> visible) {
    var nodes = new LinkedHashMap<String, Node>();
    var connections = new ArrayList<Connection>();
    for (var view : views) {
      var snapshot = view.snapshot();
      String environment = snapshot == null ? "unknown" : snapshot.environment();
      if (!visible.test(environment, view.source())) continue;
      String console = environment + ":service:apex-operations-console";
      String owner = environment + ":service:" + view.source();
      var declaration = new Evidence("DECLARED", "CONFIGURATION",
          "apex.ops03.owners / OwnerPoller OPS_OWNER_SNAPSHOT:1", now, null, "SERVICE");
      nodes.putIfAbsent(console, new Node(console, "SERVICE", "apex-operations-console",
          environment, console, null, null, "SERVICE", "UNKNOWN", "UNKNOWN",
          "CONFIGURATION_ONLY", null, List.of(declaration)));
      nodes.put(owner, new Node(owner, "SERVICE", view.source(), environment, owner,
          null, null, "SERVICE", "UNKNOWN", view.freshness(), view.uncertainty(),
          snapshot == null ? null : snapshot.observedAt(), List.of(declaration)));
      connections.add(new Connection(console + "->" + owner, console, owner, environment,
          "GRPC", "SERVICE", "OPS_OWNER_SNAPSHOT:1", "UNKNOWN", null, List.of(declaration)));
      if (snapshot != null) {
        String instance = owner + ":instance:" + snapshot.instance() + ":" + snapshot.epoch();
        var evidence = new Evidence("OBSERVED", "OWNER_SNAPSHOT", "OPS_OWNER_SNAPSHOT:1",
            view.receivedAt(), snapshot.observedAt(), "INSTANCE");
        nodes.put(instance, new Node(instance, "INSTANCE", snapshot.instance(), environment,
            owner, snapshot.instance(), snapshot.epoch(), "INSTANCE", "UNKNOWN",
            view.freshness(), view.uncertainty(), snapshot.observedAt(), List.of(evidence)));
      }
    }
    return new TopologySnapshot(1, now, "CONFIGURED_OWNERS_ONLY_PARTIAL_MEMBERSHIP_NO_TRAFFIC_HISTORY",
        new Capabilities("CONFIGURED_OWNER_QUERIES_ONLY", "OWNER_SNAPSHOTS_PARTIAL",
            "NOT_CONFIGURED", "NOT_CONFIGURED", "NOT_CONFIGURED", "NOT_CONFIGURED"),
        List.copyOf(nodes.values()), List.copyOf(connections), List.of(), List.of(), List.of());
  }
}
