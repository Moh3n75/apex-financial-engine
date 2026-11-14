package com.apex.platform.messaging.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.TaskScheduler;

import com.apex.platform.context.autoconfigure.ApexExecutionContextAutoConfiguration;
import com.apex.platform.core.context.ExecutionContextProvider;
import com.apex.platform.events.EventEnvelopeFactory;
import com.apex.platform.events.api.CellEvents;
import com.apex.platform.messaging.consumer.EventHandlerExecutor;
import com.apex.platform.messaging.consumer.EventHandlerRegistry;
import com.apex.platform.messaging.consumer.EventMessageDispatcher;
import com.apex.platform.messaging.consumer.ProcessedEventStore;
import com.apex.platform.messaging.jdbc.ConfigurableKafkaEventConsumer;
import com.apex.platform.messaging.jdbc.OutboxPublisherScheduler;
import com.apex.platform.messaging.jdbc.PlatformCellMessagingAutoConfiguration;
import com.apex.platform.messaging.kafka.KafkaMessagePublisher;
import com.apex.platform.messaging.outbox.OutboxAppender;
import com.apex.platform.messaging.outbox.OutboxPublisher;

/** Exercises real bean registration without opening Kafka or database connections. */
@Tag("integration")
class PlatformCellMessagingAutoConfigurationTest {

    private static final List<Class<?>> PUBLISHER_TYPES = List.of(
            KafkaMessagePublisher.class, OutboxPublisher.class, OutboxPublisherScheduler.class);
    private static final List<Class<?>> CONSUMER_TYPES = List.of(
            ProcessedEventStore.class, EventHandlerRegistry.class, EventHandlerExecutor.class,
            EventMessageDispatcher.class, ConfigurableKafkaEventConsumer.class);

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    PropertyPlaceholderAutoConfiguration.class,
                    ApexExecutionContextAutoConfiguration.class,
                    PlatformCellMessagingAutoConfiguration.class))
            .withPropertyValues("apex.messaging.cell.enabled=true")
            .withBean(JdbcTemplate.class, () -> mock(JdbcTemplate.class))
            // Capture scheduled tasks without starting background polling in context tests.
            .withBean(TaskScheduler.class, () -> mock(TaskScheduler.class));

    @ParameterizedTest(name = "publisher={0}, consumer={1}")
    @CsvSource({"true, false", "false, true", "true, true", "false, false"})
    void registersOnlyEnabledSubsystems(boolean publisherEnabled, boolean consumerEnabled) {
        var mode = runner.withPropertyValues(
                "apex.messaging.outbox.publisher.enabled=" + publisherEnabled,
                "apex.messaging.consumer.enabled=" + consumerEnabled);
        if (publisherEnabled) {
            mode = withPublisherPrerequisites(mode);
        }
        if (consumerEnabled) {
            mode = withConsumerPrerequisites(mode);
        }

        mode.run(context -> {
            assertThat(context).hasNotFailed();
            assertSharedBeans(context);
            assertBeanCounts(context, PUBLISHER_TYPES, publisherEnabled ? 1 : 0);
            assertBeanCounts(context, CONSUMER_TYPES, consumerEnabled ? 1 : 0);
            if (!publisherEnabled) {
                assertThat(context.getEnvironment().containsProperty("apex.messaging.publisher.topic")).isFalse();
                assertThat(context).doesNotHaveBean(KafkaTemplate.class);
            }
        });
    }

    @Test
    void defaultsToPublisherEnabledAndConsumerDisabled() {
        withPublisherPrerequisites(runner).run(context -> {
            assertThat(context).hasNotFailed();
            assertSharedBeans(context);
            assertBeanCounts(context, PUBLISHER_TYPES, 1);
            assertBeanCounts(context, CONSUMER_TYPES, 0);
        });
    }

    @Test
    void preservesCustomAdaptersWithoutDuplicateRegistrations() {
        var publisher = mock(KafkaMessagePublisher.class);
        var inbox = mock(ProcessedEventStore.class);
        withConsumerPrerequisites(runner)
                .withPropertyValues("apex.messaging.publisher.topic=test.events.v1",
                        "apex.messaging.consumer.enabled=true")
                .withBean("customPublisher", KafkaMessagePublisher.class, () -> publisher)
                .withBean("customInbox", ProcessedEventStore.class, () -> inbox)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertBeanCounts(context, PUBLISHER_TYPES, 1);
                    assertBeanCounts(context, CONSUMER_TYPES, 1);
                    assertThat(context.getBean(KafkaMessagePublisher.class)).isSameAs(publisher);
                    assertThat(context.getBean(ProcessedEventStore.class)).isSameAs(inbox);
                    assertThat(context).doesNotHaveBean(KafkaTemplate.class);
                });
    }

    @Test
    void leavesExecutionContextIndependentOfCellMessaging() {
        runner.withPropertyValues("apex.messaging.cell.enabled=false").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(ExecutionContextProvider.class);
            assertThat(context).doesNotHaveBean(CellEvents.class);
            assertThat(context).doesNotHaveBean(OutboxAppender.class);
            assertThat(context).doesNotHaveBean(EventEnvelopeFactory.class);
            assertBeanCounts(context, PUBLISHER_TYPES, 0);
            assertBeanCounts(context, CONSUMER_TYPES, 0);
        });
    }

    private static ApplicationContextRunner withPublisherPrerequisites(ApplicationContextRunner context) {
        return context.withPropertyValues("apex.messaging.publisher.topic=test.events.v1")
                .withBean(KafkaTemplate.class, () -> mock(KafkaTemplate.class));
    }

    private static ApplicationContextRunner withConsumerPrerequisites(ApplicationContextRunner context) {
        return context.withPropertyValues(
                "spring.application.name=test-cell",
                "apex.cell.id=test-cell",
                "spring.kafka.consumer.group-id=test-cell",
                "apex.messaging.consumer.topics=test.events.v1");
    }

    private static void assertSharedBeans(AssertableApplicationContext context) {
        assertThat(context).hasSingleBean(CellEvents.class);
        assertThat(context).hasSingleBean(OutboxAppender.class);
        assertThat(context).hasSingleBean(EventEnvelopeFactory.class);
        assertThat(context).hasSingleBean(ExecutionContextProvider.class);
    }

    private static void assertBeanCounts(AssertableApplicationContext context, List<Class<?>> types, int count) {
        for (Class<?> type : types) {
            assertThat(context.getBeansOfType(type)).as("beans of type %s", type.getSimpleName()).hasSize(count);
        }
    }
}
