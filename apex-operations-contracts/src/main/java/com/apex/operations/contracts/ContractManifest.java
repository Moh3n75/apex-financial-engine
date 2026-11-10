package com.apex.operations.contracts;

import java.time.Instant;
import java.util.*;

/** Effective declarations collected in a process. Never evidence of traffic or broker existence. */
public record ContractManifest(int schemaVersion, String environment, String service, String cell,
    String instance, UUID epoch, Instant collectedAt, String sourceMode, String cluster,
    List<Publication> publications, List<String> subscriptions, String consumerGroup,
    List<Contract> eventHandlers, List<Rpc> grpcHandlers, List<String> gaps) {
  public record Contract(String type, int version) {
    public Contract { token(type, "[A-Z][A-Z0-9_]{0,63}"); if (version < 1 || version > 10000) throw new IllegalArgumentException("VERSION"); }
    public String key() { return type + ":" + version; }
  }
  public record Publication(Contract contract, String topic, String provenance) {
    public Publication { Objects.requireNonNull(contract); token(topic, "[A-Za-z0-9._-]{1,249}");
      if (!Set.of("PUBLISHER_ROUTING", "PUBLISHED_CONTRACT_CATALOG").contains(provenance)) throw new IllegalArgumentException("PROVENANCE"); }
  }
  public record Rpc(String kind, Contract contract) {
    public Rpc { if (!Set.of("QUERY", "REQUEST").contains(kind)) throw new IllegalArgumentException("RPC_KIND"); Objects.requireNonNull(contract); }
  }
  public ContractManifest {
    if (schemaVersion != 1 || collectedAt == null || epoch == null) throw new IllegalArgumentException("SCHEMA");
    for (String value : List.of(environment, service, cell, instance, cluster)) token(value, "[A-Za-z0-9._-]{1,64}");
    if (!Set.of("EFFECTIVE_DECLARATION", "STATIC_DECLARATION", "FIXTURE").contains(sourceMode)) throw new IllegalArgumentException("SOURCE_MODE");
    token(consumerGroup, "[A-Za-z0-9._-]{0,200}");
    publications = bounded(publications); subscriptions = bounded(subscriptions); eventHandlers = bounded(eventHandlers);
    grpcHandlers = bounded(grpcHandlers); gaps = bounded(gaps);
    subscriptions.forEach(t -> token(t, "[A-Za-z0-9._-]{1,249}"));
    gaps.forEach(g -> token(g, "[A-Z_]{1,80}"));
  }
  private static <T> List<T> bounded(List<T> values) {
    if (values.size() > 64) throw new IllegalArgumentException("BOUND"); return List.copyOf(values);
  }
  private static void token(String value, String regex) {
    if (value == null || !value.matches(regex)) throw new IllegalArgumentException("IDENTIFIER");
  }
}
