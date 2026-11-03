package com.apex.platform.grpc.internal;

import com.apex.platform.core.context.*;
import com.apex.platform.grpc.api.*;
import java.time.Instant;
import java.util.concurrent.*;
import org.springframework.beans.factory.ObjectProvider;

final class DefaultQueryGateway implements QueryGateway {
  private final GrpcProperties properties;
  private final HandlerRegistry registry;
  private final RpcDispatcher dispatcher;
  private final RpcCodec codec;
  private final ExecutionContextProvider contexts;
  private final ObjectProvider<GrpcInvocationClient> remote;
  private final ExecutorService workers;
  private final ScheduledExecutorService timer;
  private RpcObservation observations=RpcObservation.NONE;
  DefaultQueryGateway withObservations(RpcObservation value){observations=value;return this;}

  DefaultQueryGateway(
      GrpcProperties p,
      HandlerRegistry r,
      RpcDispatcher d,
      RpcCodec c,
      ExecutionContextProvider ctx,
      ObjectProvider<GrpcInvocationClient> transport,
      ExecutorService workers,
      ScheduledExecutorService timer) {
    properties = p;
    registry = r;
    dispatcher = d;
    codec = c;
    contexts = ctx;
    remote = transport;
    this.workers = workers;
    this.timer = timer;
  }

  @Override
  public <R> R query(String destination, Object query, Class<R> type, QueryOptions options) {
    return RpcErrors.await(
        queryAsync(destination, query, type, options), options.deadline(), false);
  }

  @Override
  public <R> R queryLocal(Object query, Class<R> type, QueryOptions options) {
    return RpcErrors.await(local(query, type, options), options.deadline(), false);
  }

  @Override
  public <R> CompletionStage<R> queryAsync(
      String destination, Object query, Class<R> type, QueryOptions options) {
    var route = properties.getDestinations().get(destination);
    if (route == null)
      return CompletableFuture.failedFuture(
          new GatewayException(GatewayException.Code.NOT_FOUND, false));
    if (route.local()) return local(query, type, options);
    var client = remote.getIfAvailable();
    if (client == null)
      return CompletableFuture.failedFuture(
          new GatewayException(GatewayException.Code.FAILED_PRECONDITION, false));
    return client.query(destination, query, type, options);
  }

  private <R> CompletionStage<R> local(Object value, Class<R> response, QueryOptions options) {
    try {
      if (options.deadline().compareTo(properties.getMaxDeadline()) > 0)
        throw new IllegalArgumentException("Deadline exceeds configured maximum");
      var contract = Contracts.inspect(value.getClass(), true);
      if (contract.response() != response)
        throw new IllegalArgumentException("Declared response mismatch");
      var handler = registry.find(true, contract.type(), contract.version());
      Object safe =
          codec.decode(codec.encode(value, contract.payload()), handler.contract().payload());
      var origin = RpcContexts.currentOrRoot(contexts, properties.getCellId());
      var ctx =
          new RequestContext(
              options.requestId(),
              "",
              contract.type(),
              contract.version(),
              origin.correlationId(),
              origin.causationId(),
              properties.getCellId(),
              properties.getCellId(),
              "local",
              "local:" + properties.getCellId(),
              Instant.now().plus(options.deadline()));
      var future = new RpcFuture<R>(false);
      var queued=observations.start("query.queue");
      java.util.concurrent.Future<?> task;
      try {task=workers.submit(
              () -> {
                queued.finish("OBSERVED");
                var execution=observations.start("query.execution");
                try {
                  future.complete(
                      codec.decode(dispatcher.invokeHandler(handler, safe, ctx), response));
                  execution.finish("SUCCEEDED");
                } catch (Throwable failure) {
                  execution.finish(RpcObservation.outcome(failure));
                  future.completeExceptionally(RpcErrors.failure(failure, false));
                } finally {
                  execution.finish("UNKNOWN");
                }
              });}catch(RejectedExecutionException full){queued.finish("FAILED");throw full;}
      future.whenComplete((answer,failure)->queued.finish(RpcObservation.outcome(failure)));
      future.cancellation = () -> task.cancel(true);
      var timeout =
          timer.schedule(
              () -> {
                if (future.completeExceptionally(
                    new GatewayException(GatewayException.Code.DEADLINE_EXCEEDED, false)))
                  task.cancel(true);
              },
              options.deadline().toMillis(),
              TimeUnit.MILLISECONDS);
      future.whenComplete((v, e) -> timeout.cancel(false));
      return future;
    } catch (RejectedExecutionException full) {
      return CompletableFuture.failedFuture(
          new GatewayException(GatewayException.Code.RESOURCE_EXHAUSTED, false));
    } catch (Throwable failure) {
      return CompletableFuture.failedFuture(RpcErrors.failure(failure, false));
    }
  }
}
