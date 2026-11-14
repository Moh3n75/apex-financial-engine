package com.apex.platform.grpc.api;
/** Technical read API pinned to an opaque registry ID, never to a caller-supplied URL. */
public interface InstanceQueryGateway {
  <R> R queryInstance(String logicalCell,String registryId,Object query,Class<R> response,QueryOptions options);
}
