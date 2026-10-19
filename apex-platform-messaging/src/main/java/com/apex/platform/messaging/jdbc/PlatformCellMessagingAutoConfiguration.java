package com.apex.platform.messaging.jdbc;

import com.apex.platform.context.autoconfigure.ApexExecutionContextAutoConfiguration;
import com.apex.platform.core.context.ExecutionContextProvider;
import com.apex.platform.events.EventEnvelopeFactory;
import com.apex.platform.events.api.CellEvents;
import com.apex.platform.messaging.consumer.*;
import com.apex.platform.messaging.internal.DefaultCellEvents;
import com.apex.platform.messaging.kafka.KafkaMessagePublisher;
import com.apex.platform.messaging.kafka.SpringKafkaMessagePublisher;
import com.apex.platform.messaging.outbox.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.util.List;
import java.util.UUID;

@AutoConfiguration(
        after = ApexExecutionContextAutoConfiguration.class
)
@EnableScheduling
@ConditionalOnProperty(
        prefix = "apex.messaging.cell",
        name = "enabled",
        havingValue = "true"
)
public class PlatformCellMessagingAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    @ConditionalOnMissingBean
    JsonMapper jsonMapper() {
        return JsonMapper.builder().build();
    }

    @Bean
    @ConditionalOnMissingBean
    EventEnvelopeFactory eventEnvelopeFactory(Clock clock) {
        return new EventEnvelopeFactory(clock);
    }


    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(
            prefix = "apex.messaging.outbox.publisher",
            name = "enabled",
            havingValue = "true",
            matchIfMissing = true
    )
    KafkaMessagePublisher kafkaMessagePublisher(
            KafkaTemplate<String, String> kafkaTemplate
    ) {
        return new SpringKafkaMessagePublisher(kafkaTemplate);
    }


    @Bean
    @ConditionalOnMissingBean
    OutboxAppender outboxAppender(JdbcTemplate jdbcTemplate, JsonMapper jsonMapper) {
        return new JdbcOutboxAppender(jdbcTemplate, jsonMapper);
    }

    @Bean
    @ConditionalOnMissingBean
    OutboxStore outboxStore(JdbcTemplate jdbcTemplate, Clock clock) {
        return new JdbcOutboxStore(jdbcTemplate, clock);
    }

    @Bean
    @ConditionalOnMissingBean
    OutboxEventMapper outboxEventMapper(JsonMapper jsonMapper) {
        return new JacksonOutboxEventMapper(jsonMapper);
    }

    @Bean
    @ConditionalOnMissingBean
    RetryPolicy retryPolicy() {
        return new RetryPolicy();
    }

    @Bean
    @ConditionalOnMissingBean(name = "outboxPublisherInstanceId")
    String outboxPublisherInstanceId() {
        return "cell-" + UUID.randomUUID();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(
            prefix = "apex.messaging.outbox.publisher",
            name = "enabled",
            havingValue = "true",
            matchIfMissing = true
    )
    OutboxPublisher outboxPublisher(
            OutboxStore outboxStore,
            KafkaMessagePublisher kafkaMessagePublisher,
            JsonMapper jsonMapper,
            OutboxEventMapper outboxEventMapper,
            RetryPolicy retryPolicy,
            String outboxPublisherInstanceId,
            @Value("${apex.messaging.publisher.topic}") String topic
    ) {
        return new ConfigurableOutboxPublisher(
                outboxStore,
                kafkaMessagePublisher,
                jsonMapper,
                outboxEventMapper,
                retryPolicy,
                outboxPublisherInstanceId,
                topic
        );
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(
            prefix = "apex.messaging.outbox.publisher",
            name = "enabled",
            havingValue = "true",
            matchIfMissing = true
    )
    OutboxPublisherScheduler outboxPublisherScheduler(OutboxPublisher outboxPublisher) {
        return new OutboxPublisherScheduler(outboxPublisher);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "apex.messaging.consumer", name = "enabled", havingValue = "true")
    ProcessedEventStore processedEventStore(JdbcTemplate jdbcTemplate, Clock clock) {
        return new JdbcProcessedEventStore(jdbcTemplate, clock);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "apex.messaging.consumer", name = "enabled", havingValue = "true")
    EventHandlerRegistry eventHandlerRegistry(List<EventHandler<?>> handlers) {
        return new DefaultEventHandlerRegistry(handlers);
    }

    @Bean
    @ConditionalOnMissingBean(EventHandlerExecutor.class)
    @ConditionalOnProperty(prefix = "apex.messaging.consumer", name = "enabled", havingValue = "true")
    EventHandlerExecutor eventHandlerExecutor(
            ProcessedEventStore processedEventStore,
            ExecutionContextProvider executionContextProvider,
            @Value("${spring.application.name}") String sourceService,
            @Value("${apex.cell.id}") String cellId
    ) {
        return new SimpleTransactionalEventHandlerExecutor(
                processedEventStore,
                executionContextProvider,
                sourceService,
                cellId
        );
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "apex.messaging.consumer", name = "enabled", havingValue = "true")
    EventMessageDispatcher eventMessageDispatcher(
            JsonMapper jsonMapper,
            EventHandlerRegistry eventHandlerRegistry,
            EventHandlerExecutor eventHandlerExecutor
    ) {
        return new JacksonEventMessageDispatcher(jsonMapper, eventHandlerRegistry, eventHandlerExecutor);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(
            prefix = "apex.messaging.consumer",
            name = "enabled",
            havingValue = "true",
            matchIfMissing = false
    )
    ConfigurableKafkaEventConsumer configurableKafkaEventConsumer(
            EventMessageDispatcher dispatcher,
            Clock clock,
            @Value("${spring.kafka.consumer.group-id}") String consumerGroup
    ) {
        return new ConfigurableKafkaEventConsumer(dispatcher, clock, consumerGroup);
    }
    

    @Bean
    @ConditionalOnMissingBean(CellEvents.class)
    CellEvents cellEvents(
            EventEnvelopeFactory envelopeFactory,
            OutboxAppender outboxAppender,
            ExecutionContextProvider contextProvider
    ) {
        return new DefaultCellEvents(
                envelopeFactory,
                outboxAppender,
                contextProvider::requireCurrent
        );
    }

}
