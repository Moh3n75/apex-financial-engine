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
      if (kind.equals("OBSERVED") && Set.of("CONFIGURATION", "CONTRACT_DISCOVERY", "SPRING_CLOUD_GATEWAY").contains(source)) throw new IllegalArgumentException("DECLARATION_IS_NOT_OBSERVATION");
    }
  }
  public record Node(String id, String kind, String name, String environment, String serviceId,
                     String instanceId, UUID epoch, String scope, String health, String freshness,
                     String uncertainty, Instant lastSeen, List<Evidence> evidence) {
    public Node {
      NodeKind.valueOf(kind); evidence = List.copyOf(evidence);
      if (kind.equals("INSTANCE") && epoch==null && (!"INSTANCE".equals(scope) || instanceId==null
          || !"REGISTRY_INSTANCE_IDENTITY_UNVERIFIED_NO_EPOCH".equals(uncertainty) || !"UNKNOWN".equals(health)
          || evidence.stream().noneMatch(e -> e.kind().equals("OBSERVED") && e.source().equals("EUREKA")
              && e.reference().equals("UP_REGISTRATION_NO_EPOCH") && e.scope().equals("INSTANCE"))))
        throw new IllegalArgumentException("EPOCHLESS_INSTANCE_REQUIRES_REGISTRY_PROVENANCE");
    }
  }
  public record Connection(String id, String source, String target, String environment,
                           String protocol, String scope, String contract, String health,
                           Instant lastSeen, List<Evidence> evidence, Communication details) {
    public Connection(String id,String source,String target,String environment,String protocol,String scope,String contract,
        String health,Instant lastSeen,List<Evidence> evidence) {
      this(id,source,target,environment,protocol,scope,contract,health,lastSeen,evidence,null);
    }
    public Connection {
      Protocol.valueOf(protocol); evidence = List.copyOf(evidence);
      if (evidence.stream().anyMatch(e -> e.kind().equals("OBSERVED") && e.source().equals("EUREKA")))
        throw new IllegalArgumentException("REGISTRY_PRESENCE_IS_NOT_TRAFFIC");
      if (lastSeen != null && evidence.stream().noneMatch(e -> e.kind().equals("OBSERVED") && e.lastSeen() != null)) throw new IllegalArgumentException("TRAFFIC_REQUIRES_OBSERVATION");
      if(details!=null && ((details.http()!=null && !protocol.equals("HTTP")) || (!details.kafka().isEmpty() && !protocol.equals("KAFKA"))
          || (details.grpc()!=null && !protocol.equals("GRPC"))))throw new IllegalArgumentException("PROTOCOL_DETAILS");
    }
  }
  public record HttpRoute(String routeId,String destinationService,List<String> predicates,List<String> filters) {
    public HttpRoute { predicates=List.copyOf(predicates);filters=List.copyOf(filters);if(predicates.size()>16||filters.size()>16)throw new IllegalArgumentException("ROUTE_DETAILS_BOUND"); }
  }
  public record KafkaMatch(String cluster,String eventType,int eventVersion,String publisherServiceId,String topicId,
      String consumerGroupId,String consumerServiceId,String handlerContract,String matchStatus) {
    public KafkaMatch {
      var contract=new ContractManifest.Contract(eventType,eventVersion);
      if(cluster==null||!cluster.matches("[A-Za-z0-9._-]{1,64}")||publisherServiceId==null||topicId==null
          ||!Set.of("EXACT_DECLARED_CONTRACT_MATCH","NO_VISIBLE_CONSUMER_MATCH").contains(matchStatus))throw new IllegalArgumentException("KAFKA_MATCH");
      if(matchStatus.equals("EXACT_DECLARED_CONTRACT_MATCH")&&(consumerGroupId==null||consumerServiceId==null||!contract.key().equals(handlerContract)))throw new IllegalArgumentException("INCOMPLETE_KAFKA_MATCH");
    }
  }
  public record GrpcCall(String kind,String callerServiceId,String targetServiceId,String discoveryTarget,String behavior,String availability) {
    public GrpcCall {if(!Set.of("QUERY","REQUEST").contains(kind)||!Set.of("REQUEST_RESPONSE","SYNCHRONOUS_REQUEST_RESPONSE","ASYNC_REQUEST_RESPONSE").contains(behavior)||discoveryTarget==null||!discoveryTarget.matches("[A-Za-z0-9._-]{1,64}"))throw new IllegalArgumentException("GRPC_CALL");}
  }
  public record Communication(HttpRoute http,List<KafkaMatch> kafka,GrpcCall grpc) {
    public Communication { kafka=List.copyOf(kafka==null?List.of():kafka);if(kafka.size()>64)throw new IllegalArgumentException("MATCH_BOUND"); }
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
    for (var node : nodes) if (node.kind().equals("INSTANCE") && node.epoch()==null) {
      var parent=index.get(node.serviceId());
      if (schemaVersion!=2 || parent==null || !parent.kind().equals("SERVICE") || !parent.environment().equals(node.environment()))
        throw new IllegalArgumentException("REGISTRY_INSTANCE_SCOPE");
    }
    var ids = new HashSet<String>();
    for (var edge : connections) {
      var source = index.get(edge.source()); var target = index.get(edge.target());
      if (!ids.add(edge.id()) || source == null || target == null || !source.environment().equals(edge.environment())
          || !target.environment().equals(edge.environment())) throw new IllegalArgumentException("CONNECTION_SCOPE");
      if(edge.details()!=null) {
        var d=edge.details();
        if(d.http()!=null&&(!d.http().destinationService().equals(target.name())||!edge.contract().equals("ROUTE:"+d.http().routeId())))throw new IllegalArgumentException("HTTP_ROUTE_SCOPE");
        if(d.grpc()!=null&&(!d.grpc().callerServiceId().equals(edge.source())||!d.grpc().targetServiceId().equals(edge.target())))throw new IllegalArgumentException("GRPC_CALL_SCOPE");
        for(var match:d.kafka()) {
          if(!edge.contract().equals(match.eventType()+":"+match.eventVersion())||index.get(match.publisherServiceId())==null||!index.get(match.publisherServiceId()).kind().equals("SERVICE")
              ||index.get(match.topicId())==null||!index.get(match.topicId()).kind().equals("KAFKA_TOPIC"))throw new IllegalArgumentException("KAFKA_CONTRACT_SCOPE");
          for(var ref:Arrays.asList(match.publisherServiceId(),match.topicId(),match.consumerGroupId(),match.consumerServiceId()))if(ref!=null&&(index.get(ref)==null||!index.get(ref).environment().equals(edge.environment())))throw new IllegalArgumentException("KAFKA_ENVIRONMENT_SCOPE");
          if(!match.topicId().equals(edge.environment()+":topic:"+match.cluster()+":"+index.get(match.topicId()).name()))throw new IllegalArgumentException("KAFKA_CLUSTER_SCOPE");
        }
      }
    }
  }
}
