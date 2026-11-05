package com.apex.platform.grpc.internal;

import com.apex.platform.grpc.api.GatewayException;
import io.grpc.ManagedChannel;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.springframework.grpc.client.GrpcChannelFactory;

/** One lazily created channel per logical Cell; creation and close cannot race. */
final class CellGrpcChannelProvider implements AutoCloseable {
  private final GrpcProperties properties;
  private final GrpcChannelFactory explicit;
  private final DiscoveredCellChannelFactory discovered;
  private final Map<String, ManagedChannel> channels = new HashMap<>();
  private boolean closed;

  CellGrpcChannelProvider(GrpcProperties properties, GrpcChannelFactory explicit,
      DiscoveredCellChannelFactory discovered) {
    this.properties = properties;
    this.explicit = explicit;
    this.discovered = discovered;
  }

  boolean supportsDiscovery() { return discovered != null; }

  synchronized ManagedChannel channel(String destination) {
    if (closed) throw new GatewayException(GatewayException.Code.FAILED_PRECONDITION, false);
    var route = properties.getDestinations().get(destination);
    if (route != null && route.local())
      throw new GatewayException(GatewayException.Code.INVALID_ARGUMENT, false);
    if (discovered != null) {
      if (destination == null || !destination.matches("[a-z][a-z0-9-]{0,62}"))
        throw new GatewayException(GatewayException.Code.INVALID_ARGUMENT, false);
      return channels.computeIfAbsent("cell:" + destination, ignored -> discovered.createChannel(destination));
    }
    if (route == null) throw new GatewayException(GatewayException.Code.NOT_FOUND, false);
    return channels.computeIfAbsent("channel:" + route.channel(), ignored -> explicit.createChannel(route.channel()));
  }

  @Override
  public void close() {
    List<ManagedChannel> owned;
    synchronized (this) {
      if (closed) return;
      closed = true;
      owned = List.copyOf(channels.values());
    }
    owned.forEach(ManagedChannel::shutdown);
    for (var channel : owned) {
      try {
        if (!channel.awaitTermination(5, TimeUnit.SECONDS)) channel.shutdownNow();
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        channel.shutdownNow();
      }
    }
  }
}
