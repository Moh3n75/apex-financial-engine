package com.apex.operations.console;

import com.apex.operations.contracts.*;
import com.apex.platform.grpc.api.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/** Finite allowlisted reads over the existing gRPC transport. No request-time fan-out. */
@Component
public final class ContractDiscoveryPoller implements AutoCloseable {
  public record View(String environment, String owner, ContractManifest manifest, String status) {}
  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
      Thread.ofPlatform().daemon().name("ops-contract-read").factory());
  private final Map<String, View> cache = new LinkedHashMap<>();
  private final QueryGateway gateway;
  private final String environment;
  private final boolean membershipEnabled;

  public ContractDiscoveryPoller(Environment env, ObjectProvider<QueryGateway> gateways) {
    gateway = gateways.getIfAvailable();
    membershipEnabled=env.getProperty("apex.ops.membership.enabled",Boolean.class,false);
    environment = env.getProperty("apex.ops.contract-discovery.environment", "unknown");
    if (!environment.matches("[A-Za-z0-9._-]{1,64}")) throw new IllegalArgumentException("ENVIRONMENT");
    String owners = env.getProperty("apex.ops.contract-discovery.owners", "");
    if (!owners.isBlank()) for (String owner : owners.split(",", -1)) {
      if (!owner.matches("[A-Za-z0-9._-]{1,64}") || cache.size() == 8 || cache.containsKey(owner)) throw new IllegalArgumentException("INVENTORY");
      cache.put(owner, new View(environment, owner, null, "MISSING"));
    }
    if (!cache.isEmpty() && (gateway == null || environment.equals("unknown"))) throw new IllegalStateException("CONTRACT_DISCOVERY_TRANSPORT_AND_ENVIRONMENT_REQUIRED");
  }
  @org.springframework.context.event.EventListener(org.springframework.boot.context.event.ApplicationReadyEvent.class)
  public void start() { if(!membershipEnabled)executor.scheduleWithFixedDelay(this::poll, 0, 10, TimeUnit.SECONDS); }

  void poll() {
    for (String owner : owners()) {
      try {
        var manifest = gateway.query(owner, new ContractDiscoveryQuery(), ContractManifest.class,
            new QueryOptions(UUID.randomUUID(), Duration.ofMillis(1000)));
        accept(owner, manifest, Instant.now());
      } catch (Exception unavailable) {
        synchronized (cache) {
          var previous = cache.get(owner);
          cache.put(owner, new View(environment, owner, previous.manifest(), "SOURCE_UNAVAILABLE"));
        }
      }
    }
  }
  private List<String> owners() { synchronized (cache) { return List.copyOf(cache.keySet()); } }
  void accept(String owner, ContractManifest manifest, Instant now) {
    if (!manifest.service().equals(owner) || !manifest.environment().equals(environment)
        || !manifest.sourceMode().equals("EFFECTIVE_DECLARATION")
        || manifest.collectedAt().isAfter(now.plusSeconds(1)) || manifest.collectedAt().isBefore(now.minusSeconds(30)))
      throw new IllegalArgumentException("MANIFEST_SCOPE_OR_CLOCK");
    synchronized (cache) {
      if (!cache.containsKey(owner)) throw new IllegalArgumentException("INVENTORY");
      cache.put(owner, new View(environment, owner, manifest, "AVAILABLE"));
    }
  }
  public List<View> views() {
    synchronized (cache) {
      return cache.values().stream().map(v -> v.manifest() != null && v.status().equals("AVAILABLE")
          && v.manifest().collectedAt().isBefore(Instant.now().minusSeconds(30))
          ? new View(v.environment(), v.owner(), v.manifest(), "STALE") : v).toList();
    }
  }
  @Override public void close() { executor.shutdownNow(); }
}
