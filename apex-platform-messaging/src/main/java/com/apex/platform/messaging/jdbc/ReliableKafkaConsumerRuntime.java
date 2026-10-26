package com.apex.platform.messaging.jdbc;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.apex.platform.messaging.consumer.NonRetryableEventException;
import com.apex.platform.messaging.consumer.UnsupportedEventContractException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConsumerAwareRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultBackOffHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Resources owned only by the APEX reliable factory. Deliberately not a global
 * KafkaTemplate/CommonErrorHandler bean, so unrelated listeners keep their policy.
 */
public final class ReliableKafkaConsumerRuntime implements SmartLifecycle, AutoCloseable {
    private final DefaultKafkaProducerFactory<String, String> producerFactory;
    private final KafkaTemplate<String, String> template;
    private final DefaultErrorHandler errorHandler;
    private final ConsumerReliabilityProperties properties;
    private final Map<String, Object> adminProperties;
    private final List<String> topics;
    private final ObjectProvider<ConfigurableKafkaEventConsumer> consumer;
    private boolean running;

    @SuppressWarnings("unchecked")
    public ReliableKafkaConsumerRuntime(ProducerFactory<?, ?> bootProducerFactory, Map<String, Object> adminProperties,
            Object bootstrapServers, ConsumerReliabilityProperties properties, String topics,
            ObjectProvider<ConfigurableKafkaEventConsumer> consumer, boolean observationEnabled) {
        this.properties = properties;
        this.adminProperties = new HashMap<>(adminProperties);
        this.adminProperties.put("bootstrap.servers", bootstrapServers);
        this.topics = Arrays.stream(topics.split(",")).map(String::trim).filter(value -> !value.isEmpty()).distinct().toList();
        if (this.topics.isEmpty()) {
            throw new IllegalArgumentException("Reliable APEX consumer requires explicit source topics");
        }
        this.consumer = consumer;
        if (bootProducerFactory.transactionCapable()
                || bootProducerFactory.getConfigurationProperties().get(ProducerConfig.TRANSACTIONAL_ID_CONFIG) != null) {
            throw new IllegalStateException("Reliable APEX DB-Inbox recovery requires a non-transactional Kafka producer factory");
        }
        int sendMillis = Math.toIntExact(properties.sendTimeout().toMillis());
        Map<String, Object> overrides = new HashMap<>();
        overrides.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        overrides.put(ProducerConfig.ACKS_CONFIG, "all");
        overrides.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        overrides.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE); // Bounded in time by delivery.timeout.ms.
        overrides.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        overrides.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        overrides.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        overrides.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, (long) sendMillis);
        overrides.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, sendMillis);
        overrides.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, sendMillis);
        overrides.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        var candidate = (ProducerFactory<String, String>) bootProducerFactory.copyWithConfigurationOverride(overrides);
        if (!(candidate instanceof DefaultKafkaProducerFactory<String, String> copy)) {
            throw new IllegalStateException("Reliable DLT requires a copyable DefaultKafkaProducerFactory");
        }
        producerFactory = copy;
        // Also replace serializer instances/suppliers inherited by copyWithConfigurationOverride.
        copy.setKeySerializer(new StringSerializer());
        copy.setValueSerializer(new StringSerializer());
        template = new KafkaTemplate<>(producerFactory);
        template.setObservationEnabled(observationEnabled);
        var recoverer = new DeadLetterPublishingRecoverer(template,
                (record, failure) -> new TopicPartition(destination(record.topic()), record.partition()));
        recoverer.setVerifyPartition(false); // Never silently fall back to another partition.
        recoverer.setThrowIfNoDestinationReturned(true);
        recoverer.setFailIfSendResultIsError(true);
        recoverer.setWaitForSendResultTimeout(properties.sendTimeout());
        recoverer.setTimeoutBuffer(1_000L);
        recoverer.setLogRecoveryRecord(false);
        recoverer.setExceptionHeadersCreator((headers, exception, key, names) -> {
            Throwable cause = exception;
            while (!(cause instanceof NonRetryableEventException)
                    && cause.getCause() != null && cause.getCause() != cause) {
                cause = cause.getCause();
            }
            headers.add(KafkaHeaders.DLT_EXCEPTION_FQCN, bytes(cause.getClass().getName()));
            String message = cause instanceof UnsupportedEventContractException ? cause.getMessage()
                    : "APEX consumer failure; business exception details redacted";
            headers.add(KafkaHeaders.DLT_EXCEPTION_MESSAGE, bytes(message));
        });
        ConsumerAwareRecordRecoverer confirmedRecovery = (record, kafkaConsumer, failure) -> {
            try {
                recoverer.accept(record, kafkaConsumer, failure);
            } catch (RuntimeException sendFailure) {
                // Includes permanent-contract recovery failures; prevent a tight authorization-error loop.
                new DefaultBackOffHandler().onNextBackOff(null, sendFailure, properties.retryDelay().toMillis());
                throw sendFailure;
            }
        };
        errorHandler = new DefaultErrorHandler(confirmedRecovery,
                new FixedBackOff(properties.retryDelay().toMillis(), properties.maxAttempts() - 1L));
        errorHandler.addNotRetryableExceptions(NonRetryableEventException.class);
        errorHandler.setCommitRecovered(true);
        errorHandler.setAckAfterHandle(true);
        errorHandler.setResetStateOnRecoveryFailure(true);
        errorHandler.setResetStateOnExceptionChange(false); // Changing failure classes must not restart the bounded budget.
    }

    public DefaultErrorHandler errorHandler() {
        return errorHandler;
    }

    public String destination(String source) {
        String target = source + properties.dltSuffix();
        if (target.length() > 249) {
            throw new IllegalArgumentException("Source topic plus configured DLT suffix exceeds Kafka's topic name limit");
        }
        return target;
    }

    @Override
    public void start() {
        if (!consumer.getObject().isReliable()) {
            throw new IllegalStateException("Reliable mode requires the strict APEX consumer; legacy override rejected");
        }
        if (properties.preflightEnabled()) {
            try (Admin admin = Admin.create(adminProperties)) {
                var names = topics.stream().flatMap(source -> java.util.stream.Stream.of(source, destination(source))).toList();
                var descriptions = admin.describeTopics(names).allTopicNames()
                        .get(properties.preflightTimeout().toMillis(), TimeUnit.MILLISECONDS);
                for (String source : topics) {
                    if (descriptions.get(destination(source)).partitions().size() < descriptions.get(source).partitions().size()) {
                        throw new IllegalStateException("DLT must have at least as many partitions as source topic " + source);
                    }
                }
            } catch (Exception failure) {
                throw new IllegalStateException("Reliable APEX source/DLT provisioning preflight failed", failure);
            }
        }
        running = true;
        org.slf4j.LoggerFactory.getLogger(ReliableKafkaConsumerRuntime.class)
                .info("APEX reliable consumer active topics={} dltSuffix={} maxAttempts={} preflight={}",
                        topics, properties.dltSuffix(), properties.maxAttempts(), properties.preflightEnabled());
    }

    @Override
    public int getPhase() {
        return Integer.MIN_VALUE;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public void stop() {
        running = false;
    }

    @Override
    public void close() {
        template.destroy();
        producerFactory.destroy();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
