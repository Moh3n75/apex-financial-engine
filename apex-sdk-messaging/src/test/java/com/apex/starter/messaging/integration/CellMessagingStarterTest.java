package com.apex.starter.messaging.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.aop.support.AopUtils;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.scheduling.TaskScheduler;

import com.apex.platform.core.context.ExecutionContext;
import com.apex.platform.core.context.ExecutionContextProvider;
import com.apex.platform.events.EventEnvelope;
import com.apex.platform.events.EventEnvelopeFactory;
import com.apex.platform.events.EventMetadata;
import com.apex.platform.events.api.CellEvents;
import com.apex.platform.messaging.consumer.ConsumerContext;
import com.apex.platform.messaging.consumer.DispatchResult;
import com.apex.platform.messaging.consumer.EventHandler;
import com.apex.platform.messaging.consumer.EventHandlerExecutor;
import com.apex.platform.messaging.consumer.EventHandlerRegistry;
import com.apex.platform.messaging.consumer.EventMessageDispatcher;
import com.apex.platform.messaging.consumer.ProcessedEventStore;
import com.apex.platform.messaging.jdbc.ConfigurableKafkaEventConsumer;
import com.apex.platform.messaging.jdbc.OutboxPublisherScheduler;
import com.apex.platform.messaging.kafka.KafkaMessagePublisher;
import com.apex.platform.messaging.outbox.OutboxAppender;
import com.apex.platform.messaging.outbox.OutboxPublisher;

import tools.jackson.databind.json.JsonMapper;

/** Actual Boot imports discovery; mocked external resources never open sockets. */
@Tag("integration")
class CellMessagingStarterTest {
    private static final List<Class<?>> PUBLISHER_TYPES = List.of(
            KafkaMessagePublisher.class, OutboxPublisher.class, OutboxPublisherScheduler.class);
    private static final List<Class<?>> CONSUMER_TYPES = List.of(
            ProcessedEventStore.class, EventHandlerRegistry.class, EventHandlerExecutor.class,
            EventMessageDispatcher.class, ConfigurableKafkaEventConsumer.class);

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(MinimalCell.class)
            .withInitializer(context -> {
                context.getEnvironment().getPropertySources().remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
                context.getEnvironment().getPropertySources().remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
            })
            .withPropertyValues("apex.messaging.cell.enabled=true",
                    "spring.application.name=starter-test", "apex.cell.id=starter-test",
                    "spring.kafka.bootstrap-servers=127.0.0.1:1",
                    "spring.kafka.listener.auto-startup=false", "spring.kafka.admin.auto-create=false",
                    "spring.kafka.consumer.group-id=starter-test", "apex.messaging.consumer.topics=test.events.v1");

    @ParameterizedTest(name = "publisher={0}, consumer={1}")
    @CsvSource({"true, false", "false, true", "true, true", "false, false"})
    void discoversOnlyEnabledApexSubsystems(boolean publisher, boolean consumer) {
        var mode = withTestResources(runner).withPropertyValues(
                "apex.messaging.outbox.publisher.enabled=" + publisher,
                "apex.messaging.consumer.enabled=" + consumer);
        if (publisher) {
            mode = mode.withPropertyValues("apex.messaging.publisher.topic=test.events.v1");
        }
        mode.run(context -> {
            assertThat(context).hasNotFailed();
            assertShared(context);
            assertCounts(context, PUBLISHER_TYPES, publisher ? 1 : 0);
            assertCounts(context, CONSUMER_TYPES, consumer ? 1 : 0);
            if (!publisher) {
                assertThat(context.getEnvironment().containsProperty("apex.messaging.publisher.topic")).isFalse();
            }
        });
    }

    @Test
    void preservesPublisherEnabledConsumerDisabledDefaults() {
        withTestResources(runner).withPropertyValues("apex.messaging.publisher.topic=test.events.v1")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertShared(context);
                    assertCounts(context, PUBLISHER_TYPES, 1);
                    assertCounts(context, CONSUMER_TYPES, 0);
                });
    }

    @Test
    void missingDataSourceDoesNotInstallAFallbackDatabase() {
        runner.withPropertyValues("apex.messaging.outbox.publisher.enabled=false",
                "apex.messaging.consumer.enabled=false").run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasStackTraceContaining("DataSource");
                });
    }

    @Test
    void missingPublisherTopicIsNotReplacedByAFallback() {
        withTestResources(runner).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasStackTraceContaining("apex.messaging.publisher.topic");
        });
    }

    @Test
    void consumerDispatchPreservesContextTransactionsAndDuplicateSuppression() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getAutoCommit()).thenReturn(true);
        ProcessedEventStore inbox = mock(ProcessedEventStore.class);
        when(inbox.registerIfAbsent(any(), any())).thenReturn(true, false);
        AtomicReference<ExecutionContext> observed = new AtomicReference<>();
        AtomicReference<ExecutionContextProvider> provider = new AtomicReference<>();
        EventHandler<String> handler = new EventHandler<>() {
            @Override
            public String eventType() { return "TEST_CREATED"; }
            @Override
            public int eventVersion() { return 1; }
            @Override
            public Class<String> payloadType() { return String.class; }
            @Override
            public void handle(EventEnvelope<String> event, ConsumerContext context) {
                assertThat(observed.compareAndSet(null, provider.get().requireCurrent())).isTrue();
            }
        };
        runner.withPropertyValues("apex.messaging.outbox.publisher.enabled=false", "apex.messaging.consumer.enabled=true")
                .withBean(DataSource.class, () -> dataSource)
                .withBean(ProcessedEventStore.class, () -> inbox)
                .withBean("testHandler", EventHandler.class, () -> handler)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    provider.set(context.getBean(ExecutionContextProvider.class));
                    assertThat(AopUtils.isAopProxy(context.getBean(EventHandlerExecutor.class))).isTrue();
                    EventMetadata metadata = new EventMetadata(UUID.randomUUID(), "TEST_CREATED", 1,
                            UUID.randomUUID(), null, "test-1", "TEST", "source", "source-cell", Instant.EPOCH);
                    String json = context.getBean(JsonMapper.class).writeValueAsString(new EventEnvelope<>(metadata, "payload"));
                    ConsumerContext delivery = new ConsumerContext("starter-test", "test.events.v1", 0, 1,
                            "test-1", Instant.EPOCH);
                    EventMessageDispatcher dispatcher = context.getBean(EventMessageDispatcher.class);
                    assertThat(dispatcher.dispatch(json, delivery)).isEqualTo(DispatchResult.HANDLED);
                    assertThat(dispatcher.dispatch(json, delivery)).isEqualTo(DispatchResult.DUPLICATE);
                    assertThat(observed.get().correlationId()).isEqualTo(metadata.correlationId());
                    assertThat(observed.get().causationId()).isEqualTo(metadata.eventId());
                    assertThat(observed.get().sourceService()).isEqualTo("starter-test");
                    assertThat(observed.get().cellId()).isEqualTo("starter-test");
                    assertThatIllegalStateException().isThrownBy(provider.get()::requireCurrent);
                });
        verify(connection, times(2)).commit();
    }

    private static ApplicationContextRunner withTestResources(ApplicationContextRunner context) {
        return context.withBean(DataSource.class, () -> mock(DataSource.class))
                .withBean(TaskScheduler.class, () -> mock(TaskScheduler.class));
    }

    private static void assertShared(AssertableApplicationContext context) {
        for (Class<?> type : List.of(ExecutionContextProvider.class, CellEvents.class, OutboxAppender.class, EventEnvelopeFactory.class)) {
            assertThat(context.getBeansOfType(type)).as(type.getSimpleName()).hasSize(1);
        }
    }

    private static void assertCounts(AssertableApplicationContext context, List<Class<?>> types, int count) {
        for (Class<?> type : types) {
            assertThat(context.getBeansOfType(type)).as(type.getSimpleName()).hasSize(count);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class MinimalCell {}
}
