package com.apex.discovery.grpc;

import com.apex.platform.discovery.*;
import io.grpc.*;
import java.net.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/** One address group per ready replica; the existing channel chooses a subchannel per RPC. */
public final class DirectoryNameResolverFactory extends NameResolver.Factory {
  private final CellServiceDirectory directory;
  private final String logical;
  private final String authority;
  private final ScheduledExecutorService refresh;
  private final Duration interval;

  public DirectoryNameResolverFactory(
      CellServiceDirectory directory,
      String logical,
      String authority,
      ScheduledExecutorService refresh,
      Duration interval) {
    this.directory = directory;
    this.logical = CellEndpoint.logicalName(logical);
    this.authority = authority;
    this.refresh = refresh;
    this.interval = interval;
    if (authority == null
        || !authority.matches("[A-Za-z0-9.-]{1,253}")
        || interval.toMillis() < 250
        || interval.toSeconds() > 30) throw new IllegalArgumentException("Invalid resolver policy");
  }

  @Override
  public String getDefaultScheme() {
    return "dns";
  }

  @Override
  public NameResolver newNameResolver(URI uri, NameResolver.Args args) {
    if (!uri.toString().equals("dns:///" + authority + ":443")) return null;
    return new NameResolver() {
      private volatile Listener2 listener;
      private volatile boolean stopped;
      private ScheduledFuture<?> periodic;

      public String getServiceAuthority() {
        return authority;
      }

      @Override
      public synchronized void start(Listener2 value) {
        if (listener != null) throw new IllegalStateException("Resolver already started");
        listener = Objects.requireNonNull(value);
        periodic =
            refresh.scheduleWithFixedDelay(
                this::resolve, 0, interval.toMillis(), TimeUnit.MILLISECONDS);
      }

      private void resolve() {
        if (stopped) return;
        try {
          var endpoints = directory.endpoints(logical);
          var groups =
              endpoints.stream()
                  .map(
                      e ->
                          new EquivalentAddressGroup(new InetSocketAddress(e.host(), e.grpcPort())))
                  .toList();
          args.getSynchronizationContext()
              .execute(
                  () -> {
                    if (!stopped)
                      listener.onResult(ResolutionResult.newBuilder().setAddresses(groups).build());
                  });
        } catch (RuntimeException unavailable) {
          args.getSynchronizationContext()
              .execute(
                  () -> {
                    if (!stopped)
                      listener.onError(
                          Status.UNAVAILABLE.withDescription(
                              "Required Cell discovery unavailable"));
                  });
        }
      }

      @Override
      public void refresh() {
        if (!stopped) refresh.execute(this::resolve);
      }

      @Override
      public synchronized void shutdown() {
        stopped = true;
        if (periodic != null) periodic.cancel(false);
      }
    };
  }
}
