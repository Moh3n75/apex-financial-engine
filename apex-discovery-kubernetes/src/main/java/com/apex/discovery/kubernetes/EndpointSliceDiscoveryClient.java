package com.apex.discovery.kubernetes;

import com.apex.platform.discovery.*;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.LongSupplier;
import org.springframework.cloud.client.*;
import org.springframework.cloud.client.discovery.DiscoveryClient;

/**
 * Namespaced, allowlisted EndpointSlice polling. ClusterIP and unready/terminating Pods are
 * excluded.
 */
public final class EndpointSliceDiscoveryClient implements DiscoveryClient, AutoCloseable {
  private final KubernetesClient api;
  private final String namespace;
  private final DiscoveryProperties policy;
  private final long staleNanos;
  private final LongSupplier nanoTime;
  private final ScheduledExecutorService poller;
  private volatile Map<String, List<ServiceInstance>> snapshot = Map.of();
  private volatile long lastSuccess = Long.MIN_VALUE;

  public EndpointSliceDiscoveryClient(
      KubernetesClient api,
      String namespace,
      DiscoveryProperties policy,
      Duration maxStale,
      LongSupplier nanoTime,
      boolean poll) {
    policy.validate();
    this.api = api;
    this.namespace = namespace;
    this.policy = policy;
    this.nanoTime = nanoTime;
    if (namespace == null
        || !namespace.matches("[a-z0-9][a-z0-9-]{0,62}")
        || maxStale.compareTo(policy.getRefreshInterval().multipliedBy(2)) < 0
        || maxStale.compareTo(Duration.ofMinutes(2)) > 0)
      throw new IllegalArgumentException("Invalid scoped refresh/expiry");
    staleNanos = maxStale.toNanos();
    poller =
        Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("apex-endpointslices-", 0).factory());
    try {
      refresh();
      if (poll)
        poller.scheduleWithFixedDelay(
            () -> {
              try {
                refresh();
              } catch (RuntimeException unavailable) {
                /* expire, never invent endpoints */
              }
            },
            policy.getRefreshInterval().toMillis(),
            policy.getRefreshInterval().toMillis(),
            TimeUnit.MILLISECONDS);
    } catch (RuntimeException startup) {
      poller.shutdownNow();
      throw startup;
    }
  }

  public synchronized void refresh() {
    var next = new HashMap<String, List<ServiceInstance>>();
    for (var entry : policy.getTargets().entrySet()) {
      String logical = entry.getKey();
      var service = api.services().inNamespace(namespace).withName(logical).get();
      if (service == null
          || !namespace.equals(service.getMetadata().getNamespace())
          || !logical.equals(service.getMetadata().getLabels().get("apex.cell-id")))
        throw new IllegalStateException("Required namespaced Service identity missing");
      var annotations = service.getMetadata().getAnnotations();
      if (annotations == null
          || !entry.getValue().authority().equals(annotations.get("apex.grpc.authority")))
        throw new IllegalStateException("Service TLS authority mismatch");
      var replicas = new ArrayList<ServiceInstance>();
      var slices =
          api.discovery()
              .v1()
              .endpointSlices()
              .inNamespace(namespace)
              .withLabel("kubernetes.io/service-name", logical)
              .list()
              .getItems();
      for (var slice : slices) {
        if (!namespace.equals(slice.getMetadata().getNamespace())
            || !logical.equals(slice.getMetadata().getLabels().get("kubernetes.io/service-name")))
          throw new IllegalStateException("Cross-service EndpointSlice rejected");
        // v1 explicitly selects IPv4; dual-stack copies must not create duplicate Pod identities.
        if (!"IPv4".equals(slice.getAddressType())) continue;
        var ports = new HashMap<String, Integer>();
        for (var port : slice.getPorts()) {
          if (port.getName() != null && Set.of("http", "grpc").contains(port.getName())) {
            if (port.getPort() == null
                || (port.getProtocol() != null && !"TCP".equals(port.getProtocol()))
                || ports.putIfAbsent(port.getName(), port.getPort()) != null)
              throw new IllegalStateException("Invalid named EndpointSlice transport ports");
          }
        }
        if (!ports.keySet().containsAll(Set.of("http", "grpc")))
          throw new IllegalStateException("Both named ports required");
        for (var endpoint : slice.getEndpoints()) {
          var conditions = endpoint.getConditions();
          if (conditions == null
              || !Boolean.TRUE.equals(conditions.getReady())
              || Boolean.TRUE.equals(conditions.getTerminating())
              || Boolean.FALSE.equals(conditions.getServing())) continue;
          var pod = endpoint.getTargetRef();
          if (pod == null
              || !"Pod".equals(pod.getKind())
              || pod.getUid() == null
              || (pod.getNamespace() != null && !namespace.equals(pod.getNamespace())))
            throw new IllegalStateException("Endpoint Pod identity required");
          for (String address : endpoint.getAddresses()) {
            if (!address.matches("[0-9]{1,3}(\\.[0-9]{1,3}){3}")
                || java.util.Arrays.stream(address.split("\\."))
                    .anyMatch(part -> Integer.parseInt(part) > 255))
              throw new IllegalStateException(
                  "IPv4 EndpointSlice requires a literal Pod IPv4 address");
            new CellEndpoint(
                logical,
                logical + ":" + pod.getUid(),
                address,
                ports.get("http"),
                ports.get("grpc"),
                false,
                entry.getValue().authority());
            replicas.add(
                new DefaultServiceInstance(
                    logical + ":" + pod.getUid(),
                    logical,
                    address,
                    ports.get("http"),
                    false,
                    Map.of(
                        "apex.cell-id",
                        logical,
                        "apex.grpc.authority",
                        entry.getValue().authority(),
                        "apex.grpc.port",
                        ports.get("grpc").toString(),
                        "namespace",
                        namespace)));
          }
        }
      }
      next.put(logical, List.copyOf(replicas));
    }
    snapshot = Map.copyOf(next);
    lastSuccess = nanoTime.getAsLong();
  }

  public boolean fresh() {
    return lastSuccess != Long.MIN_VALUE && nanoTime.getAsLong() - lastSuccess <= staleNanos;
  }

  public String description() {
    return "APEX namespaced ready EndpointSlices (Fabric8, no Eureka)";
  }

  public List<String> getServices() {
    return policy.getTargets().keySet().stream().sorted().toList();
  }

  public List<ServiceInstance> getInstances(String id) {
    if (!policy.getTargets().containsKey(id))
      throw new IllegalArgumentException("Service not allowlisted");
    if (!fresh()) throw new IllegalStateException("EndpointSlice snapshot expired");
    return snapshot.getOrDefault(id, List.of());
  }

  public void close() {
    poller.shutdownNow();
  }
}
