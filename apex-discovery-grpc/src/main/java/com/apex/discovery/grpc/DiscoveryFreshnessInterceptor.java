package com.apex.discovery.grpc;

import com.apex.platform.discovery.CellServiceDirectory;
import io.grpc.*;

/**
 * Resolver errors may retain ready subchannels; reject a new RPC when trusted discovery expires.
 */
public final class DiscoveryFreshnessInterceptor implements ClientInterceptor {
  private final CellServiceDirectory directory;
  private final String logical;

  public DiscoveryFreshnessInterceptor(CellServiceDirectory directory, String logical) {
    this.directory = directory;
    this.logical = logical;
  }

  @Override
  public <Q, R> ClientCall<Q, R> interceptCall(
      MethodDescriptor<Q, R> method, CallOptions options, Channel next) {
    try {
      if (directory.endpoints(logical).isEmpty())
        throw new IllegalStateException("No ready replicas");
    } catch (RuntimeException unavailable) {
      return new ClientCall<>() {
        public void start(Listener<R> listener, Metadata headers) {
          listener.onClose(
              Status.UNAVAILABLE.withDescription("Required trusted Cell discovery unavailable"),
              new Metadata());
        }

        public void request(int count) {}

        public void cancel(String message, Throwable cause) {}

        public void halfClose() {}

        public void sendMessage(Q message) {}
      };
    }
    return next.newCall(method, options);
  }
}
