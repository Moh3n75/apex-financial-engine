package com.apex.platform.grpc.internal;

import io.grpc.ManagedChannel;

/** Optional discovery adapter SPI. The runtime has no dependency on a discovery provider. */
@FunctionalInterface
public interface DiscoveredCellChannelFactory {
  ManagedChannel createChannel(String logicalCell);
}
