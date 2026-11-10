package com.apex.operations.console;

import com.apex.operations.contracts.*;
import com.apex.operations.contracts.TopologySnapshot.*;
import java.util.*;
import java.util.function.BiPredicate;

/** Joins only authorized declarations using environment + cluster + topic + type/version. */
final class ContractTopology {
  private ContractTopology() {}
  private record Match(String environment, String cluster, String topic, ContractManifest.Contract contract) {}

  static TopologySnapshot join(TopologySnapshot base, List<ContractDiscoveryPoller.View> input,
      BiPredicate<String, String> visible) {
    var nodes = new LinkedHashMap<String, Node>();
    base.nodes().forEach(n -> nodes.put(n.id(), n));
    var edges = new LinkedHashMap<String, Connection>();
    base.connections().forEach(e -> edges.put(e.id(), e));
    var providers = new ArrayList<Provider>();
    var unresolved = new ArrayList<Unresolved>();
    var descriptors = new ArrayList<Descriptor>();
    // Filter BEFORE joining, counting, or retaining provenance from another owner.
    var views = input.stream().filter(v -> visible.test(v.environment(), v.owner())).toList();
    var manifests = views.stream().filter(v -> v.manifest() != null
        && v.environment().equals(v.manifest().environment()) && v.owner().equals(v.manifest().service()))
        .map(ContractDiscoveryPoller.View::manifest).toList();
    var publications = new HashMap<Match, Set<ContractManifest>>();
    for (var manifest : manifests) if (!manifest.cluster().equals("unknown")) {
      for (var publication : manifest.publications()) publications.computeIfAbsent(
          new Match(manifest.environment(), manifest.cluster(), publication.topic(), publication.contract()),
          key -> new LinkedHashSet<>()).add(manifest);
    }
    for (var view : views) {
      var m = view.manifest();
      String providerName="contracts:"+view.environment()+":"+view.owner();
      if(providers.stream().noneMatch(p->p.name().equals(providerName)))providers.add(new Provider(providerName,view.status(),
          "DECLARED_CONTRACTS_ONLY_NOT_TRAFFIC",m==null?"UNKNOWN":m.sourceMode(),m==null?null:m.collectedAt()));
      else if(!view.status().equals("AVAILABLE"))for(int i=0;i<providers.size();i++)if(providers.get(i).name().equals(providerName)) {
        var p=providers.get(i);providers.set(i,new Provider(p.name(),"PARTIAL",p.coverage(),p.sourceMode(),p.collectedAt()));
      }
      if (m == null || !view.environment().equals(m.environment()) || !view.owner().equals(m.service())) continue;
      String service = m.environment() + ":service:" + m.service();
      var evidence = new Evidence("DECLARED", "CONTRACT_DISCOVERY", "REGISTERED_CONTRACTS_AND_EFFECTIVE_CONFIGURATION",
          m.collectedAt(), null, "SERVICE");
      var previous = nodes.get(service);
      if (previous == null) nodes.put(service, node(service, "SERVICE", m.service(), m, service, evidence, view.status()));
      else {
        var combined = new ArrayList<>(previous.evidence()); if(combined.size()<16)combined.add(evidence);
        nodes.put(service, new Node(previous.id(), previous.kind(), previous.name(), previous.environment(),
            previous.serviceId(), previous.instanceId(), previous.epoch(), previous.scope(), previous.health(),
            previous.freshness(), previous.uncertainty(), previous.lastSeen(), combined));
      }
      String cell = m.environment() + ":cell:" + m.cell();
      nodes.putIfAbsent(cell, node(cell, "CELL", m.cell(), m, null, evidence, view.status()));
      for (String gap : m.gaps()) unresolved.add(new Unresolved(service, "", gap));
      for (var rpc : m.grpcHandlers()) if(descriptors.stream().noneMatch(d->d.serviceId().equals(service)&&d.protocol().equals("GRPC")&&d.kind().equals(rpc.kind())&&d.contract().equals(rpc.contract().key())))descriptors.add(new Descriptor(service,"GRPC",rpc.kind(),rpc.contract().key(),List.of(evidence)));
      for (var handler : m.eventHandlers()) if(descriptors.stream().noneMatch(d->d.serviceId().equals(service)&&d.protocol().equals("KAFKA")&&d.contract().equals(handler.key())))descriptors.add(new Descriptor(service,"KAFKA","EVENT_HANDLER",handler.key(),List.of(evidence)));
      if (m.cluster().equals("unknown")) {
        if (!m.gaps().contains("KAFKA_CLUSTER_IDENTITY_UNKNOWN")) unresolved.add(new Unresolved(service, "", "KAFKA_CLUSTER_IDENTITY_UNKNOWN"));
        continue; // Unknown clusters must never be joined together.
      }
      for (var publication : m.publications()) {
        String topic = topicId(m, publication.topic());
        var routeEvidence = new Evidence("DECLARED", "CONTRACT_DISCOVERY", publication.provenance(), m.collectedAt(), null, "SERVICE");
        nodes.putIfAbsent(topic, node(topic, "KAFKA_TOPIC", publication.topic(), m, null, routeEvidence, view.status()));
        addEdge(edges, service, topic, m, publication.contract().key(), List.of(routeEvidence));
      }
    }
    for (var consumer : manifests) {
      String service = consumer.environment() + ":service:" + consumer.service();
      if (consumer.cluster().equals("unknown") || consumer.consumerGroup().isEmpty()) continue;
      for (var handler : consumer.eventHandlers()) {
        boolean matched = false;
        for (String subscription : consumer.subscriptions()) {
          var publishers = publications.getOrDefault(new Match(consumer.environment(), consumer.cluster(), subscription, handler), Set.of());
          if (publishers.isEmpty()) continue;
          matched = true;
          String topic = topicId(consumer, subscription);
          String group = consumer.environment() + ":group:" + consumer.cluster() + ":" + consumer.consumerGroup();
          var proof = new ArrayList<Evidence>();
          proof.add(new Evidence("DECLARED", "CONTRACT_DISCOVERY", "CONSUMER_SUBSCRIPTION_AND_REGISTERED_HANDLER", consumer.collectedAt(), null, "SERVICE"));
          // Do not attach hidden publisher names or timestamps: all publishers were authorized above.
          for (var p : publishers) proof.add(new Evidence("DECLARED", "CONTRACT_DISCOVERY", "EXACT_TOPIC_CONTRACT:" + p.service(), p.collectedAt(), null, "SERVICE"));
          nodes.putIfAbsent(group, node(group, "CONSUMER_GROUP", consumer.consumerGroup(), consumer, null, proof.getFirst(), "DECLARED"));
          addEdge(edges, topic, group, consumer, handler.key(), proof);
          addEdge(edges, group, service, consumer, handler.key(), proof);
        }
        if (!matched) unresolved.add(new Unresolved(service, handler.key(), "NO_VISIBLE_EXACT_TOPIC_CONTRACT_MATCH"));
      }
      for (String subscription : consumer.subscriptions()) {
        boolean matched = consumer.eventHandlers().stream().anyMatch(h -> publications.containsKey(
            new Match(consumer.environment(), consumer.cluster(), subscription, h)));
        if (!matched) unresolved.add(new Unresolved(service, "", "SUBSCRIPTION_WITHOUT_VISIBLE_CONTRACT_MATCH"));
      }
    }
    if (views.isEmpty()) providers.add(new Provider("contracts", "NO_VISIBLE_COVERAGE", "NO_VISIBLE_DECLARATIONS", "UNKNOWN", null));
    providers.add(new Provider("otel", "NOT_CONFIGURED", "NO_OBSERVED_INTERACTIONS", "UNKNOWN", null));
    providers.add(new Provider("kafka-admin", "NOT_CONFIGURED", "NO_BROKER_METADATA", "UNKNOWN", null));
    providers.add(new Provider("runtime-health", "NOT_CONFIGURED", "NO_HEALTH_PROVIDER", "UNKNOWN", null));
    // Explicit bounded partial output. Remove dangling edges after truncation.
    boolean truncated = nodes.size() > 200 || edges.size() > 400 || descriptors.size() > 512 || unresolved.size() > 256;
    var limitedNodes = nodes.values().stream().limit(200).toList();
    var ids = limitedNodes.stream().map(Node::id).collect(java.util.stream.Collectors.toSet());
    var limitedEdges = edges.values().stream().filter(e -> ids.contains(e.source()) && ids.contains(e.target())).limit(400).toList();
    if (truncated) providers.add(new Provider("graph-budget", "PARTIAL", "NODE_EDGE_DESCRIPTOR_LIMIT", "UNKNOWN", null));
    return new TopologySnapshot(2, base.asOf(), "AUTHORIZED_DECLARATIONS_PARTIAL_NO_TRAFFIC_HISTORY",
        new Capabilities(manifests.isEmpty() ? base.capabilities().declared() : "CONTRACT_DISCOVERY_PARTIAL",
            base.capabilities().instances(), "NOT_CONFIGURED", "DECLARATIONS_ONLY_NO_BROKER_METADATA", "NOT_CONFIGURED", "NOT_CONFIGURED"),
        limitedNodes, limitedEdges, providers, unresolved.stream().filter(u -> ids.contains(u.serviceId())).limit(256).toList(),
        descriptors.stream().filter(d -> ids.contains(d.serviceId())).limit(512).toList());
  }
  private static Node node(String id, String kind, String name, ContractManifest m, String service,
      Evidence evidence, String freshness) {
    return new Node(id, kind, name, m.environment(), service, null, null, "SERVICE", "UNKNOWN", freshness,
        "DECLARATION_NOT_RUNTIME_OBSERVATION", null, List.of(evidence));
  }
  private static String topicId(ContractManifest m, String topic) { return m.environment() + ":topic:" + m.cluster() + ":" + topic; }
  private static void addEdge(Map<String, Connection> edges, String source, String target,
      ContractManifest m, String contract, List<Evidence> proof) {
    String id = source + "->" + target + ":" + contract;
    edges.putIfAbsent(id, new Connection(id, source, target, m.environment(), "KAFKA", "SERVICE", contract, "UNKNOWN", null, proof));
  }
}
