package com.apex.platform.grpc.internal;

import com.apex.platform.grpc.api.GatewayCancellationException;
import java.util.concurrent.CompletableFuture;

final class RpcFuture<T> extends CompletableFuture<T> {
  private final boolean mutation;
  volatile Runnable cancellation = () -> {};

  RpcFuture(boolean mutation) {
    this.mutation = mutation;
  }

  @Override
  public boolean cancel(boolean interrupt) {
    boolean changed = completeExceptionally(new GatewayCancellationException(mutation));
    if (changed) cancellation.run();
    return changed;
  }
}
