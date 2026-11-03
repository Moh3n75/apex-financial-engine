package com.apex.platform.messaging.jdbc;

import com.apex.platform.context.autoconfigure.ApexExecutionContextAutoConfiguration;
import com.apex.platform.core.context.ExecutionContextProvider;
import com.apex.platform.events.EventEnvelopeFactory;
import com.apex.platform.events.api.CellEvents;
import com.apex.platform.messaging.consumer.*;
import com.apex.platform.messaging.consumer.internal.DiscoveringEventHandlerRegistry;
import com.apex.platform.messaging.internal.DefaultCellEvents;
import com.apex.platform.messaging.kafka.KafkaMessagePublisher;
import com.apex.platform.messaging.kafka.SpringKafkaMessagePublisher;
import com.apex.platform.messaging.outbox.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.util.UUID;

@AutoConfiguration(
        after = ApexExecutionContextAutoConfiguration.class
)
@EnableScheduling
@org.springframework.boot.context.properties.EnableConfigurationProperties(PublisherRouting.class)
@ConditionalOnProperty(
        prefix = "apex.messaging.cell",
        name = "enabled",
        havingValue = "true"
)
public class PlatformCellMessagingAutoConfiguration {
    @Bean
    com.apex.platform.messaging.observation.MessagingObservation apexMessagingObservation(
            org.springframework.beans.factory.ObjectProvider<io.micrometer.observation.ObservationRegistry> registries,
            org.springframework.core.env.Environment environment) {
        return new com.apex.platform.messaging.observation.MessagingObservation(
            environment.getProperty("apex.observability.enabled",Boolean.class,false)
                ?registries.getIfAvailable(()->io.micrometer.observation.ObservationRegistry.NOOP)
                :io.micrometer.observation.ObservationRegistry.NOOP);
    }

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
            KafkaTemplate<String, String> kafkaTemplate,
            com.apex.platform.messaging.observation.MessagingObservation observations
    ) {
        return new SpringKafkaMessagePublisher(kafkaTemplate,observations);
    }


    @Bean
    @ConditionalOnMissingBean
    OutboxAppender outboxAppender(JdbcTemplate jdbcTemplate, JsonMapper jsonMapper,
            com.apex.platform.messaging.observation.MessagingObservation observations) {
        return new JdbcOutboxAppender(jdbcTemplate, jsonMapper,observations);
    }

    @Bean
    @ConditionalOnMissingBean
    OutboxStore outboxStore(JdbcTemplate jdbcTemplate, Clock clock,
            com.apex.platform.messaging.observation.MessagingObservation observations) {
        return new JdbcOutboxStore(jdbcTemplate, clock,observations);
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
            @Value("${apex.messaging.publisher.topic}") String topic,
            PublisherRouting routing,
            com.apex.platform.messaging.observation.MessagingObservation observations
    ) {
        return new ConfigurableOutboxPublisher(
                outboxStore,
                kafkaMessagePublisher,
                jsonMapper,
                outboxEventMapper,
                retryPolicy,
                outboxPublisherInstanceId,
                topic, routing, observations
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
    @ConditionalOnMissingBean(EventHandlerRegistry.class)
    @ConditionalOnProperty(prefix = "apex.messaging.consumer", name = "enabled", havingValue = "true")
    EventHandlerRegistry eventHandlerRegistry(
            ConfigurableListableBeanFactory beanFactory
    ) {
        return new DiscoveringEventHandlerRegistry(beanFactory);
    }


    @Bean
    @ConditionalOnMissingBean(EventHandlerExecutor.class)
    @ConditionalOnProperty(prefix = "apex.messaging.consumer", name = "enabled", havingValue = "true")
    EventHandlerExecutor eventHandlerExecutor(
            ProcessedEventStore processedEventStore,
            ExecutionContextProvider executionContextProvider,
            @Value("${spring.application.name}") String sourceService,
            @Value("${apex.cell.id}") String cellId,
            com.apex.platform.messaging.observation.MessagingObservation observations
    ) {
        return new SimpleTransactionalEventHandlerExecutor(
                processedEventStore,
                executionContextProvider,
                sourceService,
                cellId
        ).withObservations(observations);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "apex.messaging.consumer", name = "enabled", havingValue = "true")
    EventMessageDispatcher eventMessageDispatcher(
            JsonMapper jsonMapper,
            EventHandlerRegistry eventHandlerRegistry,
            EventHandlerExecutor eventHandlerExecutor,
            @Value("${apex.messaging.consumer.reliability.enabled:false}") boolean reliable
    ) {
        return new JacksonEventMessageDispatcher(jsonMapper, eventHandlerRegistry, eventHandlerExecutor, reliable);
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
            @Value("${spring.kafka.consumer.group-id}") String consumerGroup,
            @Value("${apex.messaging.consumer.reliability.enabled:false}") boolean reliable,
            JsonMapper jsonMapper,
            com.apex.platform.messaging.observation.MessagingObservation observations
    ) {
        return new ConfigurableKafkaEventConsumer(dispatcher, clock, consumerGroup, reliable, jsonMapper).withObservations(observations);
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
