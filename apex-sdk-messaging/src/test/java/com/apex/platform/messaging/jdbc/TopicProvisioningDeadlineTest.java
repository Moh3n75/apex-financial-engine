package com.apex.platform.messaging.jdbc;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.SmartLifecycle;
import org.springframework.mock.env.MockEnvironment;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

@Tag("unit")
class TopicProvisioningDeadlineTest {
    private final Admin admin = mock(Admin.class);
    private final AtomicLong now = new AtomicLong(Long.MAX_VALUE - 20_000_000); // nanoTime wrap is valid
    private final List<Integer> readBudgets = new ArrayList<>();
    private final List<Integer> createBudgets = new ArrayList<>();
    private final Deque<Object> reads = new ArrayDeque<>();
    private Object creation = KafkaFuture.completedFuture(null);
    private ApexTopicProvisioner.Sleeper sleeper = nanos -> now.addAndGet(nanos);

    private TopicDescription topic(String name, Node leader, List<Node> replicas) {
        return new TopicDescription(name, false, List.of(new TopicPartitionInfo(0, leader, replicas, replicas)));
    }
    private TopicDescription ready(String name) {
        var node = new Node(0, "localhost", 9092);
        return topic(name, node, List.of(node));
    }
    private <T> KafkaFuture<T> failed(Throwable error) {
        var future = new KafkaFutureImpl<T>(); future.completeExceptionally(error); return future;
    }
    private ApexTopicProvisioner provisioner(String names) {
        return new ApexTopicProvisioner(Map.of(), new TopicProvisioningProperties(true, 1, (short) -1,
                Duration.ofMillis(100)), new PublisherRouting(false, List.of()),
                new ConsumerReliabilityProperties(false, 3, Duration.ofSeconds(1), Duration.ofSeconds(10),
                        "-dlt", true, Duration.ofSeconds(10)),
                new MockEnvironment().withProperty("apex.messaging.outbox.publisher.enabled", "false")
                        .withProperty("apex.messaging.consumer.enabled", "true")
                        .withProperty("apex.messaging.consumer.topics", names), now::get, nanos -> sleeper.sleep(nanos));
    }
    private void run(Runnable action) {
        when(admin.describeTopics(anyCollection(), any(DescribeTopicsOptions.class))).thenAnswer(call -> {
            readBudgets.add(((DescribeTopicsOptions) call.getArgument(1)).timeoutMs());
            Object next = reads.size() > 1 ? reads.removeFirst() : reads.getFirst();
            if (next instanceof Runnable task) { task.run(); next = reads.removeFirst(); }
            var result = mock(DescribeTopicsResult.class);
            KafkaFuture<Map<String, TopicDescription>> future;
            if (next instanceof Throwable error) future = failed(error);
            else if (next instanceof KafkaFuture<?>) future = (KafkaFuture<Map<String, TopicDescription>>) next;
            else { var description = (TopicDescription) next; future = KafkaFuture.completedFuture(Map.of(description.name(), description)); }
            when(result.allTopicNames()).thenReturn(future); return result;
        });
        when(admin.createTopics(anyCollection(), any(CreateTopicsOptions.class))).thenAnswer(call -> {
            createBudgets.add(((CreateTopicsOptions) call.getArgument(1)).timeoutMs());
            var result = mock(CreateTopicsResult.class);
            when(result.all()).thenReturn(creation instanceof Throwable error ? failed(error) : (KafkaFuture<Void>) creation);
            return result;
        });
        try (var factory = mockStatic(Admin.class)) {
            factory.when(() -> Admin.create(Map.<String, Object>of())).thenReturn(admin);
            action.run();
        }
    }

    @Test void acknowledgedCreationWaitsForVisibilityWithoutRecreating() {
        reads.add(new UnknownTopicOrPartitionException()); reads.add(new UnknownTopicOrPartitionException()); reads.add(ready("source"));
        run(() -> provisioner("source").afterSingletonsInstantiated());
        assertThat(readBudgets).containsExactly(100, 100, 50);
        assertThat(createBudgets).containsExactly(100);
    }

    @Test void concurrentCreatorWinnerIsReadAndValidated() {
        reads.add(new UnknownTopicOrPartitionException()); reads.add(ready("source"));
        creation = new TopicExistsException("another Cell won");
        run(() -> provisioner("source").afterSingletonsInstantiated());
        assertThat(createBudgets).hasSize(1);
    }

    static List<Throwable> transientErrors() {
        return List.of(new LeaderNotAvailableException("election"), new NotLeaderOrFollowerException("stale"));
    }
    @ParameterizedTest @MethodSource("transientErrors")
    void documentedMetadataErrorsCanConverge(Throwable error) {
        reads.add(error); reads.add(ready("source"));
        run(() -> provisioner("source").afterSingletonsInstantiated());
        assertThat(readBudgets).containsExactly(100, 50);
        assertThat(createBudgets).isEmpty();
    }

    static List<Throwable> permanentErrors() {
        return List.of(new TopicAuthorizationException("secret"), new ClusterAuthorizationException("secret"),
                new InvalidReplicationFactorException("secret"), new InvalidConfigurationException("secret"),
                new InvalidTopicException("secret"), new org.apache.kafka.common.errors.TimeoutException("secret"));
    }
    @ParameterizedTest @MethodSource("permanentErrors")
    void permanentDescribeFailuresAreImmediateAndSanitized(Throwable error) {
        reads.add(error);
        run(() -> assertThatThrownBy(() -> provisioner("source").afterSingletonsInstantiated())
                .hasMessageContaining(error.getClass().getSimpleName()).hasMessageNotContaining("secret").hasNoCause());
        assertThat(readBudgets).containsExactly(100);
        assertThat(createBudgets).isEmpty();
    }
    @ParameterizedTest @MethodSource("permanentErrors")
    void permanentCreateFailuresAreImmediateAndSanitized(Throwable error) {
        reads.add(new UnknownTopicOrPartitionException()); creation = error;
        run(() -> assertThatThrownBy(() -> provisioner("source").afterSingletonsInstantiated())
                .hasMessageContaining(error.getClass().getSimpleName()).hasMessageNotContaining("secret").hasNoCause());
        assertThat(readBudgets).hasSize(1); assertThat(createBudgets).hasSize(1);
    }

    @Test void allTopicsShareTheRemainingBudget() {
        reads.add((Runnable) () -> now.addAndGet(60_000_000)); reads.add(ready("a"));
        reads.add(topic("b", Node.noNode(), List.of(new Node(0, "localhost", 9092))));
        run(() -> assertThatThrownBy(() -> provisioner("a,b").afterSingletonsInstantiated())
                .hasMessageContaining("topic b").hasMessageContaining("TimeoutException"));
        assertThat(readBudgets).containsExactly(100, 40);
        verify(admin).close(Duration.ZERO);
    }

    @Test void creationAndFutureWaitReceiveOnlyTheRemainingBudget() throws Exception {
        reads.add((Runnable) () -> now.addAndGet(60_000_000)); reads.add(new UnknownTopicOrPartitionException());
        KafkaFuture<Void> pending = mock(KafkaFuture.class); creation = pending;
        when(pending.get(anyLong(), eq(TimeUnit.NANOSECONDS))).thenAnswer(call -> {
            long remaining = call.getArgument(0); assertThat(remaining).isEqualTo(40_000_000);
            now.addAndGet(remaining); throw new java.util.concurrent.TimeoutException();
        });
        run(() -> assertThatThrownBy(() -> provisioner("source").afterSingletonsInstantiated()).hasMessageContaining("TimeoutException"));
        assertThat(createBudgets).containsExactly(40);
        verify(admin).close(Duration.ZERO);
    }

    @Test void unavailableMetadataExhaustsDeadlineWithoutBusySpin() {
        reads.add(new TopicDescription("source", false, List.of()));
        run(() -> assertThatThrownBy(() -> provisioner("source").afterSingletonsInstantiated()).hasMessageContaining("TimeoutException"));
        assertThat(readBudgets).containsExactly(100, 50);
    }

    @Test void internalTopicFailsImmediatelyEvenWithoutLeader() {
        reads.add(new TopicDescription("source", true, List.of()));
        run(() -> assertThatThrownBy(() -> provisioner("source").afterSingletonsInstantiated()).hasMessageContaining("incompatible"));
        assertThat(readBudgets).hasSize(1);
    }

    @Test void interruptDuringPollingIsRestoredAndSanitized() {
        reads.add(topic("source", null, List.of()));
        sleeper = nanos -> { throw new InterruptedException("secret"); };
        try {
            run(() -> assertThatThrownBy(() -> provisioner("source").afterSingletonsInstantiated())
                    .hasMessageContaining("InterruptedException").hasMessageNotContaining("secret").hasNoCause());
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally { Thread.interrupted(); }
    }

    @Test void lifecycleCannotStartWhileMetadataIsPendingOrAfterFailure() {
        var started = new AtomicBoolean();
        SmartLifecycle listener = new SmartLifecycle() {
            public void start() { started.set(true); }
            public void stop() { started.set(false); }
            public boolean isRunning() { return started.get(); }
            public int getPhase() { return Integer.MIN_VALUE; }
        };
        reads.add(topic("source", Node.noNode(), List.of())); reads.add(ready("source"));
        sleeper = nanos -> { assertThat(started).isFalse(); now.addAndGet(nanos); };
        run(() -> new ApplicationContextRunner().withBean(ApexTopicProvisioner.class, () -> provisioner("source"))
                .withBean("dependentListener", SmartLifecycle.class, () -> listener).run(context -> {
                    assertThat(context).hasNotFailed(); assertThat(started).isTrue();
                }));
        reads.clear(); reads.add(topic("source", Node.noNode(), List.of())); started.set(false);
        run(() -> new ApplicationContextRunner().withBean(ApexTopicProvisioner.class, () -> provisioner("source"))
                .withBean("dependentListener", SmartLifecycle.class, () -> listener).run(context -> {
                    assertThat(context).hasFailed(); assertThat(started).isFalse();
                }));
    }
}
