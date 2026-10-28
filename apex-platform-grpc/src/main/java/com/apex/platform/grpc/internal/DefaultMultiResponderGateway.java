package com.apex.platform.grpc.internal;

import com.apex.platform.core.context.*;
import com.apex.platform.grpc.api.*;
import java.util.*;
import java.util.concurrent.*;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/** Per-call state only. Existing QueryGateway owns every local/remote invocation. */
final class DefaultMultiResponderGateway implements MultiResponderGateway {
  private final QueryGateway queries;
  private final ExecutionContextProvider contexts;
  private final GrpcProperties routes;
  private final CollectorProperties policy;
  private final RpcCodec codec;
  private final ExecutorService workers;
  private final ScheduledExecutorService clock;
  private final Semaphore calls, collections;

  DefaultMultiResponderGateway(
      QueryGateway queries,
      ExecutionContextProvider contexts,
      GrpcProperties routes,
      CollectorProperties policy,
      RpcCodec codec,
      ExecutorService workers,
      ScheduledExecutorService clock) {
    policy.validate(routes);
    this.queries = queries;
    this.contexts = contexts;
    this.routes = routes;
    this.policy = policy;
    this.codec = codec;
    this.workers = workers;
    this.clock = clock;
    calls = new Semaphore(policy.getMaxInFlight());
    collections = new Semaphore(policy.getMaxCollections());
  }

  @Override
  public <R> CompletionStage<CollectionResult<R>> collectQueries(
      Object query, Class<R> responseType, CollectionOptions options) {
    try {
      long start = System.nanoTime();
      Objects.requireNonNull(options);
      if (TransactionSynchronizationManager.isActualTransactionActive())
        throw new GatewayException(GatewayException.Code.FAILED_PRECONDITION, false);
      if (options.expectedResponders().size() > policy.getMaxResponders()
          || options.totalDeadline().compareTo(routes.getMaxDeadline()) > 0
          || !policy.getAllowedResponders().containsAll(options.expectedResponders()))
        throw new GatewayException(GatewayException.Code.INVALID_ARGUMENT, false);
      var contract = Contracts.inspect(query.getClass(), true);
      if (contract.response() != responseType)
        throw new IllegalArgumentException("Declared response mismatch");
      Object safe = codec.decode(codec.encode(query, contract.payload()), contract.payload());
      if (!collections.tryAcquire())
        throw new GatewayException(GatewayException.Code.RESOURCE_EXHAUSTED, false);
      int count = options.expectedResponders().size();
      if (!calls.tryAcquire(count)) {
        collections.release();
        throw new GatewayException(GatewayException.Code.RESOURCE_EXHAUSTED, false);
      }
      var job =
          new Job<R>(
              safe,
              responseType,
              options,
              RpcContexts.currentOrRoot(contexts, routes.getCellId()),
              start);
      try {
        job.expiration =
            clock.schedule(
                job::expire, Math.max(0, job.end - System.nanoTime()), TimeUnit.NANOSECONDS);
        job.launch = workers.submit(job::launch);
      } catch (RejectedExecutionException full) {
        job.reject();
      }
      return job.result;
    } catch (Throwable failure) {
      return CompletableFuture.failedFuture(RpcErrors.failure(failure, false));
    }
  }

  private final class Job<R> {
    final Object query;
    final Class<R> type;
    final CollectionOptions options;
    final ExecutionContext context;
    final long end;
    final LinkedHashMap<String, UUID> ids = new LinkedHashMap<>();
    final Map<String, ResponderResult<R>> outcomes = new HashMap<>();
    final Map<String, CompletableFuture<R>> pending = new HashMap<>();
    final CompletableFuture<CollectionResult<R>> result;
    volatile ScheduledFuture<?> expiration;
    volatile Future<?> launch;
    boolean finished;

    Job(
        Object query,
        Class<R> type,
        CollectionOptions options,
        ExecutionContext context,
        long start) {
      this.query = query;
      this.type = type;
      this.options = options;
      this.context = context;
      end = start + options.totalDeadline().toNanos();
      for (String target : options.expectedResponders()) ids.put(target, UUID.randomUUID());
      result =
          new CompletableFuture<>() {
            @Override
            public boolean cancel(boolean interrupt) {
              return cancelJob();
            }
          };
    }

    void launch() {
      try (var scope = contexts.open(context)) {
        for (String target : ids.keySet()) {
          synchronized (this) {
            if (finished) return;
          }
          long remaining = end - System.nanoTime();
          if (remaining < 1_000_000) {
            expire();
            return;
          }
          // Never call another asynchronous API, cancel it, or notify callers while holding Job's
          // monitor.
          var future =
              queries
                  .queryAsync(
                      target,
                      query,
                      type,
                      new QueryOptions(ids.get(target), java.time.Duration.ofNanos(remaining)))
                  .toCompletableFuture();
          boolean late;
          synchronized (this) {
            late = finished;
            if (!late) pending.put(target, future);
          }
          if (late) {
            future.cancel(true);
            return;
          }
          future.whenComplete((value, failure) -> completed(target, value, failure));
        }
      } catch (Throwable failure) {
        Completion completion;
        synchronized (this) {
          if (finished) return;
          fill(ResponderResult.Status.FAILED, GatewayException.Code.INTERNAL);
          completion = finishState(null);
        }
        publish(completion);
      }
    }

    void completed(String target, R value, Throwable failure) {
      Completion completion;
      synchronized (this) {
        if (finished) return;
        if (failure == null && value != null)
          outcomes.put(
              target,
              new ResponderResult<>(
                  target, ids.get(target), ResponderResult.Status.SUCCESS, value, null, false));
        else {
          var code =
              failure == null
                  ? GatewayException.Code.INTERNAL
                  : RpcErrors.failure(failure, false).code();
          var status =
              switch (code) {
                case NOT_FOUND -> ResponderResult.Status.NOT_FOUND;
                case DEADLINE_EXCEEDED -> ResponderResult.Status.TIMED_OUT;
                case UNAVAILABLE -> ResponderResult.Status.UNAVAILABLE;
                case CANCELLED -> ResponderResult.Status.CANCELLED;
                default -> ResponderResult.Status.FAILED;
              };
          outcomes.put(
              target,
              new ResponderResult<>(
                  target,
                  ids.get(target),
                  status,
                  null,
                  code,
                  status != ResponderResult.Status.NOT_FOUND));
        }
        completion = outcomes.size() == ids.size() ? finishState(null) : null;
      }
      if (completion != null) publish(completion);
    }

    void expire() {
      Completion completion;
      synchronized (this) {
        if (finished) return;
        fill(ResponderResult.Status.TIMED_OUT, GatewayException.Code.DEADLINE_EXCEEDED);
        completion = finishState(null);
      }
      publish(completion);
    }

    boolean cancelJob() {
      Completion completion;
      synchronized (this) {
        if (finished) return false;
        fill(ResponderResult.Status.CANCELLED, GatewayException.Code.CANCELLED);
        completion = finishState(new CollectionCancelledException(snapshot()));
      }
      publish(completion);
      return true;
    }

    void reject() {
      Completion completion;
      synchronized (this) {
        if (finished) return;
        fill(ResponderResult.Status.FAILED, GatewayException.Code.RESOURCE_EXHAUSTED);
        completion =
            finishState(new GatewayException(GatewayException.Code.RESOURCE_EXHAUSTED, false));
      }
      publish(completion);
    }

    void fill(ResponderResult.Status status, GatewayException.Code code) {
      ids.forEach(
          (target, id) ->
              outcomes.putIfAbsent(
                  target, new ResponderResult<>(target, id, status, null, code, true)));
    }

    CollectionResult<R> snapshot() {
      return new CollectionResult<>(ids.keySet().stream().map(outcomes::get).toList());
    }

    Completion finishState(Throwable error) {
      var completion = new Completion(snapshot(), List.copyOf(pending.values()), error);
      finished = true;
      return completion;
    }

    final class Completion {
      final CollectionResult<R> snapshot;
      final List<CompletableFuture<R>> outstanding;
      final Throwable error;

      Completion(
          CollectionResult<R> snapshot, List<CompletableFuture<R>> outstanding, Throwable error) {
        this.snapshot = snapshot;
        this.outstanding = outstanding;
        this.error = error;
      }
    }

    void publish(Completion completion) {
      try {
        // Publish the immutable terminal slots before any external cancellation/cleanup.
        // Cancelling the deadline first can strand a completed job with an unresolved result.
        if (completion.error != null) result.completeExceptionally(completion.error);
        else if (!options.allowPartial() && !completion.snapshot.completeSuccess())
          result.completeExceptionally(new CollectionFailedException(completion.snapshot));
        else result.complete(completion.snapshot);
      } finally {
        try {
          if (expiration != null) expiration.cancel(false);
          if (launch != null) launch.cancel(false);
          completion.outstanding.forEach(
              f -> {
                if (!f.isDone()) f.cancel(true);
              });
        } finally {
          calls.release(ids.size());
          collections.release();
        }
      }
    }
  }
}
