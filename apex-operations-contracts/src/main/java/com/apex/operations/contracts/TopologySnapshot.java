package com.apex.operations.contracts;

import java.time.Instant;
import java.util.*;

/** Version 1 base shape remains available; version 2 adds providers, descriptors and unresolved facts. */
public record TopologySnapshot(int schemaVersion, Instant asOf, String coverage, Capabilities capabilities,
    List<Node> nodes, List<Connection> connections, List<Provider> providers,
    List<Unresolved> unresolved, List<Descriptor> descriptors,List<RuntimeMember> memberships) {
  public record RuntimeMember(String id,String environment,String cell,String service,String instanceId,UUID epoch,
      String registryId,String registryIdentity,String discoverySource,Instant lastSeen,String freshness,String membershipState,
      String coverage,String transportStatus) {}
  public TopologySnapshot(int version,Instant asOf,String coverage,Capabilities capabilities,List<Node> nodes,List<Connection> connections,
      List<Provider> providers,List<Unresolved> unresolved,List<Descriptor> descriptors) {
    this(version,asOf,coverage,capabilities,nodes,connections,providers,unresolved,descriptors,List.of());
  }
  public enum NodeKind { CELL, SERVICE, INSTANCE, KAFKA_TOPIC, CONSUMER_GROUP }
  public enum Protocol { HTTP, GRPC, KAFKA }
  public enum EvidenceKind { DECLARED, OBSERVED }
  public record Evidence(String kind, String source, String reference, Instant collectedAt,
                         Instant lastSeen, String scope) {
    public Evidence {
      EvidenceKind.valueOf(kind);
      if (kind.equals("DECLARED") && lastSeen != null) throw new IllegalArgumentException("DECLARED_LAST_SEEN");
      if (kind.equals("OBSERVED") && Set.of("CONFIGURATION", "CONTRACT_DISCOVERY").contains(source)) throw new IllegalArgumentException("DECLARATION_IS_NOT_OBSERVATION");
    }
  }
  public record Node(String id, String kind, String name, String environment, String serviceId,
                     String instanceId, UUID epoch, String scope, String health, String freshness,
                     String uncertainty, Instant lastSeen, List<Evidence> evidence) {
    public Node { NodeKind.valueOf(kind); evidence = List.copyOf(evidence); }
  }
  public record Connection(String id, String source, String target, String environment,
                           String protocol, String scope, String contract, String health,
                           Instant lastSeen, List<Evidence> evidence) {
    public Connection {
      Protocol.valueOf(protocol); evidence = List.copyOf(evidence);
      if (evidence.stream().anyMatch(e -> e.kind().equals("OBSERVED") && e.source().equals("EUREKA")))
        throw new IllegalArgumentException("REGISTRY_PRESENCE_IS_NOT_TRAFFIC");
      if (lastSeen != null && evidence.stream().noneMatch(e -> e.kind().equals("OBSERVED") && e.lastSeen() != null)) throw new IllegalArgumentException("TRAFFIC_REQUIRES_OBSERVATION");
    }
  }
  public record Capabilities(String declared, String instances, String observedInteractions,
                             String messaging, String traces, String runtimeHealth) {}
  public record Provider(String name, String status, String coverage, String sourceMode, Instant collectedAt) {}
  public record Unresolved(String serviceId, String contract, String reason) {}
  public record Descriptor(String serviceId, String protocol, String kind, String contract, List<Evidence> evidence) {}
  public TopologySnapshot {
    if (schemaVersion < 1 || schemaVersion > 2) throw new IllegalArgumentException("SCHEMA_VERSION");
    nodes = List.copyOf(nodes); connections = List.copyOf(connections); providers = List.copyOf(providers);
    unresolved = List.copyOf(unresolved); descriptors = List.copyOf(descriptors);
    memberships=List.copyOf(memberships);
    if(memberships.size()>128)throw new IllegalArgumentException("MEMBERSHIP_BOUND");
    if (nodes.size() > 200 || connections.size() > 400 || providers.size() > 32 || unresolved.size() > 256 || descriptors.size() > 512) throw new IllegalArgumentException("GRAPH_BOUND");
    var index = new HashMap<String, Node>();
    for (var node : nodes) if (index.put(node.id(), node) != null) throw new IllegalArgumentException("DUPLICATE_NODE");
    var ids = new HashSet<String>();
    for (var edge : connections) {
      var source = index.get(edge.source()); var target = index.get(edge.target());
      if (!ids.add(edge.id()) || source == null || target == null || !source.environment().equals(edge.environment())
          || !target.environment().equals(edge.environment())) throw new IllegalArgumentException("CONNECTION_SCOPE");
    }
  }
}
