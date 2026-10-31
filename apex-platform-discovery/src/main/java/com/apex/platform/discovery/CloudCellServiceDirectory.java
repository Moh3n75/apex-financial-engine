package com.apex.platform.discovery;

import java.util.*;
import java.util.function.BooleanSupplier;
import org.springframework.cloud.client.discovery.DiscoveryClient;

/** Spring Cloud owns provider caching/watch/leases; this adapter pins transport metadata. */
public final class CloudCellServiceDirectory implements CellServiceDirectory {
  private final DiscoveryClient cloud;
  private final DiscoveryProperties policy;
  private final BooleanSupplier fresh;

  public CloudCellServiceDirectory(
      DiscoveryClient cloud, DiscoveryProperties policy, BooleanSupplier fresh) {
    policy.validate();
    this.cloud = Objects.requireNonNull(cloud);
    this.policy = policy;
    this.fresh = Objects.requireNonNull(fresh);
  }

  @Override
  public List<CellEndpoint> endpoints(String logicalCell) {
    String logical = CellEndpoint.logicalName(logicalCell);
    var target = policy.getTargets().get(logical);
    if (target == null) throw new IllegalArgumentException("Destination is not allowlisted");
    if (!fresh.getAsBoolean()) throw new IllegalStateException("Discovery snapshot is stale");
    var ids = new HashSet<String>();
    var sockets = new HashSet<String>();
    var endpoints = new ArrayList<CellEndpoint>();
    for (var instance : cloud.getInstances(logical)) {
      var metadata = instance.getMetadata();
      if (!CellEndpoint.logicalName(instance.getServiceId()).equals(logical)
          || !logical.equals(metadata.get("apex.cell-id"))
          || !target.authority().equals(metadata.get("apex.grpc.authority")))
        throw new IllegalStateException("Discovered Cell identity/authority mismatch");
      String rawPort = metadata.getOrDefault("apex.grpc.port", metadata.get("port.grpc"));
      int grpcPort;
      try {
        grpcPort = Integer.parseInt(rawPort);
      } catch (RuntimeException bad) {
        throw new IllegalStateException("Missing valid named gRPC port");
      }
      var endpoint =
          new CellEndpoint(
              logical,
              instance.getInstanceId(),
              instance.getHost(),
              instance.getPort(),
              grpcPort,
              instance.isSecure(),
              target.authority());
      if (!ids.add(endpoint.instanceId())
          || !sockets.add(endpoint.host() + ":" + endpoint.grpcPort()))
        throw new IllegalStateException("Duplicate discovered endpoint identity");
      endpoints.add(endpoint);
    }
    return endpoints.stream().sorted(Comparator.comparing(CellEndpoint::instanceId)).toList();
  }
}
