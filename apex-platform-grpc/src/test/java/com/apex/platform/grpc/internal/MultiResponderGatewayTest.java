package com.apex.platform.grpc.internal;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import com.apex.platform.core.context.*;
import com.apex.platform.grpc.api.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;

@Tag("unit")
class MultiResponderGatewayTest {
  @CellQuery(type = "VIEW", version = 1, response = View.class)
  public record Query(String value) {}

  public record View(String value) {}

  @CellRequest(type = "WRITE", version = 1, response = View.class)
  public record Write(String value) {}

  final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
  final ExecutorService workers = Executors.newFixedThreadPool(2);

  @AfterEach
  void close() throws Exception {
    workers.shutdownNow();
    timer.shutdownNow();
    assertThat(workers.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    assertThat(timer.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
  }

  static final class FakeQueries implements QueryGateway {
    final Map<String, CompletableFuture<View>> replies = new ConcurrentHashMap<>();
    final Map<String, QueryOptions> options = new ConcurrentHashMap<>();
    final Map<String, ExecutionContext> metadata = new ConcurrentHashMap<>();
    final ExecutionContextProvider ctx;

    FakeQueries(ExecutionContextProvider ctx) {
      this.ctx = ctx;
    }

    public <R> R query(String target, Object query, Class<R> type, QueryOptions o) {
      throw new AssertionError("Must be concurrent async calls");
    }

    @SuppressWarnings("unchecked")
    public <R> CompletionStage<R> queryAsync(
        String target, Object query, Class<R> type, QueryOptions o) {
      var reply = replies.computeIfAbsent(target, k -> new CompletableFuture<>());
      metadata.put(target, ctx.requireCurrent());
      options.put(target, o);
      return (CompletionStage<R>) (CompletionStage<?>) reply;
    }

    public <R> R queryLocal(Object query, Class<R> type, QueryOptions o) {
      throw new AssertionError();
    }
  }

  ExecutionContextProvider context() {
    return new com.apex.platform.core.context.ThreadLocalExecutionContextProvider();
  }

  DefaultMultiResponderGateway gateway(FakeQueries fake, int maxCollections) {
    var routes = new GrpcProperties();
    routes.setCellId("client");
    routes.setDestinations(
        Map.of(
            "a",
            new GrpcProperties.Destination(false, "a"),
            "b",
            new GrpcProperties.Destination(true, null)));
    var limits = new CollectorProperties();
    limits.setAllowedResponders(List.of("a", "b"));
    limits.setMaxResponders(2);
    limits.setMaxInFlight(2);
    limits.setMaxCollections(maxCollections);
    return new DefaultMultiResponderGateway(
        fake, fake.ctx, routes, limits, new RpcCodec(65536), workers, timer);
  }

  CollectionOptions options(boolean partial) {
    return new CollectionOptions(List.of("b", "a"), Duration.ofSeconds(2), partial);
  }

  @Test
  void validatesFiniteTargetsDuplicatesAndPolicy() {
    for (var targets : List.of(List.<String>of(), List.of("a", "a"), List.of("bad topic")))
      assertThatThrownBy(() -> new CollectionOptions(targets, Duration.ofSeconds(1), true))
          .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new CollectionOptions(List.of("a"), Duration.ZERO, true))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new CollectionOptions(List.of("a"), Duration.ofSeconds(31), true))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                new CollectionOptions(
                    java.util.stream.IntStream.range(0, 17).mapToObj(i -> "c" + i).toList(),
                    Duration.ofSeconds(1),
                    true))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(options(true).expectedResponders()).containsExactly("a", "b");
  }

  @Test
  void launchesAllTargetsConcurrentlyAndPreservesContextWithDistinctInvocationIds()
      throws Exception {
    var provider = context();
    var fake = new FakeQueries(provider);
    var gateway = gateway(fake, 1);
    var origin = new ExecutionContext(UUID.randomUUID(), UUID.randomUUID(), "source", "client");
    CompletableFuture<CollectionResult<View>> result;
    try (var scope = provider.open(origin)) {
      result =
          gateway.collectQueries(new Query("q"), View.class, options(true)).toCompletableFuture();
    }
    await().atMost(Duration.ofSeconds(1)).until(() -> fake.options.size() == 2);
    assertThat(fake.metadata.values()).containsOnly(origin);
    assertThat(fake.options.values().stream().map(QueryOptions::requestId).distinct().count())
        .isEqualTo(2);
    fake.replies.get("b").complete(new View("b"));
    fake.replies.get("a").complete(new View("a"));
    assertThat(
            result.get(2, TimeUnit.SECONDS).responders().stream().map(ResponderResult::responder))
        .containsExactly("a", "b");
    assertThatThrownBy(provider::requireCurrent).isInstanceOf(IllegalStateException.class);
    assertThat(
            workers
                .submit(
                    () -> {
                      try {
                        provider.requireCurrent();
                        return false;
                      } catch (IllegalStateException clean) {
                        return true;
                      }
                    })
                .get(1, TimeUnit.SECONDS))
        .isTrue();
  }

  @Test
  void overallDeadlineIsNotMultipliedByTargetsAndCancelsOutstandingCalls() throws Exception {
    var fake = new FakeQueries(context());
    var gateway = gateway(fake, 1);
    long start = System.nanoTime();
    var result =
        gateway
            .collectQueries(
                new Query("q"),
                View.class,
                new CollectionOptions(List.of("a", "b"), Duration.ofMillis(80), true))
            .toCompletableFuture()
            .get(1, TimeUnit.SECONDS);
    assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofMillis(700));
    assertThat(result.responders())
        .hasSize(2)
        .allMatch(r -> r.status() == ResponderResult.Status.TIMED_OUT && r.answerUnknown());
    await()
        .atMost(Duration.ofSeconds(1))
        .untilAsserted(
            () -> assertThat(fake.replies.values()).allMatch(CompletableFuture::isCancelled));
  }

  @Test
  void strictFailureRetainsEveryOutcomeAndLegitimateAbsence() throws Exception {
    var fake = new FakeQueries(context());
    var result =
        gateway(fake, 1)
            .collectQueries(new Query("q"), View.class, options(false))
            .toCompletableFuture();
    await().atMost(Duration.ofSeconds(1)).until(() -> fake.options.size() == 2);
    fake.replies.get("a").complete(new View("a"));
    fake.replies
        .get("b")
        .completeExceptionally(new GatewayException(GatewayException.Code.NOT_FOUND, false));
    assertThatThrownBy(() -> result.get(1, TimeUnit.SECONDS))
        .hasCauseInstanceOf(CollectionFailedException.class);
    var failure = (CollectionFailedException) catchThrowable(() -> result.join()).getCause();
    assertThat(failure.result().responders()).hasSize(2);
    assertThat(failure.result().responders().get(1).status())
        .isEqualTo(ResponderResult.Status.NOT_FOUND);
    assertThat(failure.result().responders().get(1).answerUnknown()).isFalse();
  }

  @Test
  void cancellationRetainsCompletedReadAndPreventsAutomaticResend() throws Exception {
    var fake = new FakeQueries(context());
    var result =
        gateway(fake, 1)
            .collectQueries(new Query("q"), View.class, options(true))
            .toCompletableFuture();
    await().atMost(Duration.ofSeconds(1)).until(() -> fake.options.size() == 2);
    fake.replies.get("a").complete(new View("done"));
    assertThat(result.cancel(true)).isTrue();
    assertThat(result.cancel(true)).isFalse();
    var failure = (CollectionCancelledException) catchThrowable(result::join);
    assertThat(failure.result().responders().getFirst().status())
        .isEqualTo(ResponderResult.Status.SUCCESS);
    assertThat(failure.result().responders().get(1).status())
        .isEqualTo(ResponderResult.Status.CANCELLED);
    assertThat(fake.replies.get("b")).isCancelled();
  }

  @Test
  void saturationRejectsWholeCollectionBeforeSendingAndCapacityRecovers() throws Exception {
    var fake = new FakeQueries(context());
    var gateway = gateway(fake, 1);
    var first =
        gateway.collectQueries(new Query("q"), View.class, options(true)).toCompletableFuture();
    await().atMost(Duration.ofSeconds(1)).until(() -> fake.options.size() == 2);
    assertThatThrownBy(
            () ->
                gateway
                    .collectQueries(new Query("q"), View.class, options(true))
                    .toCompletableFuture()
                    .join())
        .hasCauseInstanceOf(GatewayException.class);
    assertThat(fake.options).hasSize(2);
    first.cancel(true);
    fake.replies.clear();
    fake.options.clear();
    var next =
        gateway.collectQueries(new Query("again"), View.class, options(true)).toCompletableFuture();
    await().atMost(Duration.ofSeconds(1)).until(() -> fake.options.size() == 2);
    fake.replies.values().forEach(f -> f.complete(new View("again")));
    assertThat(next.get(1, TimeUnit.SECONDS).completeSuccess()).isTrue();
  }

  @Test
  void rejectsWritesUnknownTargetsAndMismatchedResponseBeforeAnyInvocation() {
    var fake = new FakeQueries(context());
    var gateway = gateway(fake, 1);
    for (var stage :
        List.of(
            gateway.collectQueries(new Write("q"), View.class, options(true)),
            gateway.collectQueries(new Query("q"), String.class, options(true)),
            gateway.collectQueries(
                new Query("q"),
                View.class,
                new CollectionOptions(List.of("unknown"), Duration.ofSeconds(1), true))))
      assertThatThrownBy(() -> stage.toCompletableFuture().join())
          .hasCauseInstanceOf(GatewayException.class);
    assertThat(fake.options).isEmpty();
  }

  @Test
  void cancellationNotificationCanReenterFromAnotherThreadWithoutHoldingJobLock() throws Exception {
    var notifications = Executors.newSingleThreadExecutor();
    try {
      var fake = new FakeQueries(context());
      var callback =
          new CompletableFuture<View>() {
            @Override
            public boolean cancel(boolean interrupt) {
              try {
                return notifications.submit(() -> super.cancel(interrupt)).get(1, TimeUnit.SECONDS);
              } catch (Exception blocked) {
                throw new AssertionError(
                    "Cancellation callback blocked on collection monitor", blocked);
              }
            }
          };
      fake.replies.put("b", callback);
      fake.replies.put("a", CompletableFuture.completedFuture(new View("a")));
      var stage =
          gateway(fake, 1)
              .collectQueries(
                  new Query("q"),
                  View.class,
                  new CollectionOptions(List.of("a", "b"), Duration.ofSeconds(2), true))
              .toCompletableFuture();
      await().atMost(Duration.ofSeconds(1)).until(() -> callback.getNumberOfDependents() > 0);
      assertThat(stage.cancel(true)).isTrue();
      var cancelled = (CollectionCancelledException) catchThrowable(stage::join);
      var result = cancelled.result();
      assertThat(result.responders().getFirst().status()).isEqualTo(ResponderResult.Status.SUCCESS);
      assertThat(result.responders().get(1).status()).isEqualTo(ResponderResult.Status.CANCELLED);
      assertThat(callback).isCancelled();
    } finally {
      notifications.shutdownNow();
      assertThat(notifications.awaitTermination(2, TimeUnit.SECONDS)).isTrue();
    }
  }
}
