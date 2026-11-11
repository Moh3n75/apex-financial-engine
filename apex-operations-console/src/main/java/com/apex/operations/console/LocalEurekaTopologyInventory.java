package com.apex.operations.console;

import com.apex.discovery.eureka.EurekaCellAutoConfiguration;
import com.apex.operations.contracts.TopologySnapshot;
import com.apex.operations.contracts.TopologySnapshot.*;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.EurekaClientConfig;
import java.net.URI;
import java.time.Instant;
import java.util.*;
import java.util.function.BiPredicate;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/** Local registry presence, independent of (and never a substitute for) secure runtime membership. */
@Component
@Profile("local")
@ConditionalOnProperty(name = "apex.ops.local-inventory.enabled", havingValue = "true")
public final class LocalEurekaTopologyInventory {
  private final ObjectProvider<EurekaClient> clients;
  private final ObjectProvider<EurekaClientConfig> configs;
  private final String environment;
  private final long maxAge;
  private final boolean includeInstances;

  public LocalEurekaTopologyInventory(ObjectProvider<EurekaClient> clients,
      ObjectProvider<EurekaClientConfig> configs, Environment env) {
    this.clients = clients;
    this.configs = configs;
    environment = env.getRequiredProperty("apex.ops.local-inventory.environment");
    maxAge = env.getProperty("apex.ops.local-inventory.max-stale-ms", Long.class, 60000L);
    includeInstances = env.getProperty("apex.ops.local-inventory.include-instances", Boolean.class, false);
    if (!Set.of("local", "local-dev").contains(environment)
        || !Set.of("127.0.0.1", "::1").contains(env.getProperty("server.address", "127.0.0.1"))
        || maxAge < 1000 || maxAge > 120000)
      throw new IllegalStateException("LOCAL_INVENTORY_REQUIRES_LOCAL_ENVIRONMENT_AND_LOOPBACK");
  }

  TopologySnapshot join(TopologySnapshot base, BiPredicate<String, String> visible) {
    if (base.providers().size() >= 32) return base; // No source can be added without room to report its coverage.
    var nodes = new LinkedHashMap<String, Node>();
    base.nodes().forEach(n -> nodes.put(n.id(), n));
    Instant now = Instant.now();
    Instant seen = null;
    String status = "SOURCE_UNAVAILABLE";
    String coverage = "NO_CONFIRMED_REGISTRY_REFRESH";
    try {
      var client = clients.getIfAvailable();
      var config = configs.getIfAvailable();
      if (client == null || config == null) throw new IllegalStateException("NO_EUREKA_CLIENT");
      if (!loopbackRegistry(config)) throw new IllegalStateException("LOCAL_REGISTRY_REQUIRED");
      long age = EurekaCellAutoConfiguration.registryAge(client);
      if (age < 0 || age == Long.MAX_VALUE) throw new IllegalStateException("UNKNOWN_REGISTRY_AGE");
      seen = now.minusMillis(age);
      boolean fresh = age <= maxAge;
      var applications = client.getApplications();
      if (applications == null) throw new IllegalStateException("NO_REGISTRY_SNAPSHOT");
      // Filter before aggregation. Untagged registrations belong only to this explicit local registry scope.
      var services = new TreeSet<String>();
      var registryInstances = new TreeMap<String, Set<String>>();
      boolean partial = false;
      for (var app : applications.getRegisteredApplications()) {
        for (var instance : app.getInstances()) {
          if (instance.getStatus() != InstanceStatus.UP) continue;
          String taggedEnv = instance.getMetadata().get("apex.environment");
          if (taggedEnv != null && !environment.equals(taggedEnv)) continue;
          String service = app.getName().toLowerCase(Locale.ROOT);
          if (!visible.test(environment, service)) continue;
          if (!service.matches("[a-z][a-z0-9-]{0,62}")) { partial = true; continue; }
          services.add(service);
          if (includeInstances && instance.getInstanceId()!=null)
            registryInstances.computeIfAbsent(service,key->new TreeSet<>()).add(instance.getInstanceId());
        }
      }
      for (String service : services) {
        String id = environment + ":service:" + service;
        var evidence = new Evidence("OBSERVED", "EUREKA", "UP_REGISTRATION_LOCAL_REGISTRY",
            now, seen, "SERVICE");
        var existing = nodes.get(id);
        if (existing != null) {
          if (!existing.kind().equals("SERVICE") || existing.evidence().size() >= 16) { partial = true; continue; }
          var evidenceList = new ArrayList<>(existing.evidence());
          evidenceList.removeIf(e -> e.source().equals("EUREKA") && e.reference().equals(evidence.reference()));
          evidenceList.add(evidence);
          // Registry freshness must not refresh a contract, manifest, health check or verified membership.
          nodes.put(id, new Node(existing.id(), existing.kind(), existing.name(), existing.environment(),
              existing.serviceId(), existing.instanceId(), existing.epoch(), existing.scope(), existing.health(),
              existing.freshness(), existing.uncertainty(), existing.lastSeen(), evidenceList));
        } else if (nodes.size() < 200) {
          nodes.put(id, new Node(id, "SERVICE", service, environment, id, null, null, "SERVICE", "UNKNOWN",
              fresh ? "FRESH" : "STALE", "LOCAL_REGISTRY_PRESENCE_NOT_HEALTH_OR_TRAFFIC", seen, List.of(evidence)));
        } else partial = true;
      }
      for (var entry : registryInstances.entrySet()) {
        String serviceId=environment+":service:"+entry.getKey();
        if (!nodes.containsKey(serviceId)) continue;
        for (String registryId : entry.getValue()) {
          if (base.memberships().stream().anyMatch(m -> m.environment().equals(environment)
              && m.service().equals(entry.getKey()) && m.registryId().equals(registryId))) continue;
          // Raw Eureka IDs often contain host names/addresses/ports. Expose an explicitly opaque alias.
          String alias=registryAlias(environment,entry.getKey(),registryId);
          String id=serviceId+":registry-instance:"+alias;
          if (nodes.size()>=200) { partial=true; continue; }
          var proof=new Evidence("OBSERVED","EUREKA","UP_REGISTRATION_NO_EPOCH",now,seen,"INSTANCE");
          nodes.putIfAbsent(id,new Node(id,"INSTANCE",alias,environment,serviceId,alias,null,"INSTANCE","UNKNOWN",
              fresh?"FRESH":"STALE","REGISTRY_INSTANCE_IDENTITY_UNVERIFIED_NO_EPOCH",seen,List.of(proof)));
        }
      }
      status = partial ? "PARTIAL" : fresh ? "AVAILABLE" : "STALE";
      coverage = (partial ? "BOUNDED_PARTIAL_" : "") + (fresh ? "UP_REGISTRATIONS_ONLY" : "CACHED_UP_REGISTRATIONS_ONLY")
          + "_LOCAL_SCOPE_NO_HEALTH_TRAFFIC_OR_VERIFIED_INSTANCES";
    } catch (RuntimeException unavailable) {
      // Do not leak registry URLs, transport errors, metadata, hosts, ports or credentials.
      nodes.clear(); base.nodes().forEach(n -> nodes.put(n.id(), n));
      seen = null;
    }
    var providers = new ArrayList<>(base.providers());
    providers.add(new Provider("eureka-local-inventory", status, coverage, "REGISTRY_OBSERVATION", seen));
    return new TopologySnapshot(2, now, "PARTIAL_REGISTRY_AND_CONFIGURED_SOURCES_NO_TRAFFIC_HISTORY",
        base.capabilities(), List.copyOf(nodes.values()), base.connections(), providers,
        base.unresolved(), base.descriptors(), base.memberships());
  }

  static String registryAlias(String environment,String service,String registryId) {
    try {
      byte[] digest=java.security.MessageDigest.getInstance("SHA-256").digest(
          (environment+"\n"+service+"\n"+registryId).getBytes(java.nio.charset.StandardCharsets.UTF_8));
      return "eureka-"+HexFormat.of().formatHex(digest).substring(0,24);
    } catch (java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
  }

  static boolean loopbackRegistry(EurekaClientConfig config) {
    try {
      var urls = config.getEurekaServerServiceUrls("defaultZone");
      return urls != null && !urls.isEmpty() && urls.stream().allMatch(raw -> {
        var uri = URI.create(raw);
        return Set.of("http", "https").contains(uri.getScheme())
            && Set.of("127.0.0.1", "localhost", "[::1]").contains(uri.getHost())
            && uri.getRawUserInfo() == null && uri.getRawQuery() == null && uri.getRawFragment() == null;
      });
    } catch (RuntimeException invalid) { return false; }
  }
}
