package com.apex.platform.grpc.internal;

import com.apex.platform.grpc.api.*;
import io.grpc.Status;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.*;

final class RpcErrors {
  static GatewayException failure(Throwable error, boolean mutation) {
    while ((error instanceof ExecutionException
            || error instanceof CompletionException
            || error instanceof InvocationTargetException)
        && error.getCause() != null) error = error.getCause();
    if (error instanceof GatewayException g) return g;
    if (error instanceof CancellationException)
      return new GatewayException(GatewayException.Code.CANCELLED, mutation);
    if (error instanceof io.grpc.StatusException
        || error instanceof io.grpc.StatusRuntimeException) {
      var code = Status.fromThrowable(error).getCode();
      GatewayException.Code mapped;
      try {
        mapped = GatewayException.Code.valueOf(code.name());
      } catch (Exception unknown) {
        mapped = GatewayException.Code.INTERNAL;
      }
      return new GatewayException(
          mapped,
          mutation
              && (code == Status.Code.CANCELLED
                  || code == Status.Code.UNAVAILABLE
                  || code == Status.Code.DEADLINE_EXCEEDED
                  || code == Status.Code.INTERNAL
                  || code == Status.Code.UNKNOWN));
    }
    if (error instanceof IllegalArgumentException)
      return new GatewayException(GatewayException.Code.INVALID_ARGUMENT, false);
    return new GatewayException(GatewayException.Code.INTERNAL, mutation);
  }

  static RuntimeException wire(Throwable error) {
    var g = failure(error, true);
    return Status.fromCode(Status.Code.valueOf(g.code().name()))
        .withDescription("APEX_" + g.code().name())
        .asRuntimeException();
  }

  static <T> T await(CompletionStage<T> stage, java.time.Duration timeout, boolean mutation) {
    try {
      return stage.toCompletableFuture().get(timeout.toMillis() + 250, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      stage.toCompletableFuture().cancel(true);
      throw new GatewayException(GatewayException.Code.CANCELLED, mutation);
    } catch (TimeoutException e) {
      stage.toCompletableFuture().cancel(true);
      throw new GatewayException(GatewayException.Code.DEADLINE_EXCEEDED, mutation);
    } catch (ExecutionException | CancellationException e) {
      throw failure(e, mutation);
    }
  }
}
