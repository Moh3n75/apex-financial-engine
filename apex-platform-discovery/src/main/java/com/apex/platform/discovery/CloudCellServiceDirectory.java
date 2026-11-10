package com.apex.platform.discovery;

import java.util.*;
import java.util.function.BooleanSupplier;
import org.springframework.cloud.client.discovery.DiscoveryClient;

/** Spring Cloud owns provider caching/watch/leases; this adapter pins transport metadata. */
public final class CloudCellServiceDirectory implements CellServiceDirectory {
  private final DiscoveryClient cloud;
  private final DiscoveryProperties policy;
  private final BooleanSupplier fresh;
  private final String source,registryIdentity;

  public CloudCellServiceDirectory(
      DiscoveryClient cloud, DiscoveryProperties policy, BooleanSupplier fresh) {
    this(cloud,policy,fresh,"UNKNOWN","unknown");
  }
  public CloudCellServiceDirectory(DiscoveryClient cloud,DiscoveryProperties policy,BooleanSupplier fresh,String source,String registryIdentity) {
    policy.validate();
    this.cloud = Objects.requireNonNull(cloud);
    this.policy = policy;
    this.fresh = Objects.requireNonNull(fresh);
    this.source=source;this.registryIdentity=registryIdentity;
  }

  @Override public CellMembership membership(String logicalCell,String environment) {
    String logical=CellEndpoint.logicalName(logicalCell);
    var endpoints=endpoints(logical);
    var byId=new HashMap<String,org.springframework.cloud.client.ServiceInstance>();
    cloud.getInstances(logical).forEach(i->byId.put(i.getInstanceId(),i));
    var members=new ArrayList<CellMembership.Member>();
    boolean complete=!source.equals("UNKNOWN")&&!registryIdentity.equals("unknown");
    for(var endpoint:endpoints) {
      var instance=byId.get(endpoint.instanceId());if(instance==null){complete=false;continue;}
      var metadata=instance.getMetadata();
      if(!environment.equals(metadata.get("apex.environment"))){complete=false;continue;}
      try {
        String replica=metadata.get("apex.instance-id");
        if(replica==null||!replica.matches("[A-Za-z0-9._-]{1,64}"))throw new IllegalArgumentException();
        members.add(new CellMembership.Member(environment,logical,replica,UUID.fromString(metadata.get("apex.epoch")),endpoint.instanceId(),endpoint));
      }catch(RuntimeException invalid){complete=false;}
    }
    if(!fresh.getAsBoolean())throw new IllegalStateException("Registry expired during enumeration");
    return new CellMembership(source,registryIdentity,complete?"SCOPED_REGISTRY_COMPLETE":"PARTIAL_METADATA_OR_ENVIRONMENT",java.time.Instant.now(),members);
  }

  @Override
  public List<CellEndpoint> endpoints(String logicalCell) {
    String logical = CellEndpoint.logicalName(logicalCell);
    String authority = policy.trustedAuthority(logical);
    if (!fresh.getAsBoolean()) throw new IllegalStateException("Discovery snapshot is stale");
    var ids = new HashSet<String>();
    var sockets = new HashSet<String>();
    var endpoints = new ArrayList<CellEndpoint>();
    for (var instance : cloud.getInstances(logical)) {
      var metadata = instance.getMetadata();
      if ("false".equals(metadata.get("apex.grpc.enabled"))) continue;
      if (!CellEndpoint.logicalName(instance.getServiceId()).equals(logical)
          || !logical.equals(metadata.get("apex.cell-id"))
          || !authority.equals(metadata.get("apex.grpc.authority")))
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
              authority);
      if (!ids.add(endpoint.instanceId())
          || !sockets.add(endpoint.host() + ":" + endpoint.grpcPort()))
        throw new IllegalStateException("Duplicate discovered endpoint identity");
      endpoints.add(endpoint);
    }
    return endpoints.stream().sorted(Comparator.comparing(CellEndpoint::instanceId)).toList();
  }
}
