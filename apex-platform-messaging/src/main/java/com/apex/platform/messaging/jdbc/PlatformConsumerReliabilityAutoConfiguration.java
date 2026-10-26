package com.apex.platform.messaging.jdbc;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.kafka.autoconfigure.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

@AutoConfiguration(after = {KafkaAutoConfiguration.class, PlatformCellMessagingAutoConfiguration.class})
@ConditionalOnProperty(prefix = "apex.messaging.cell", name = "enabled", havingValue = "true")
@ConditionalOnProperty(prefix = "apex.messaging.consumer", name = "enabled", havingValue = "true")
@ConditionalOnProperty(prefix = "apex.messaging.consumer.reliability", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(ConsumerReliabilityProperties.class)
public class PlatformConsumerReliabilityAutoConfiguration {
    @Bean(destroyMethod = "close")
    ReliableKafkaConsumerRuntime apexReliableKafkaConsumerRuntime(
            ProducerFactory<?, ?> producerFactory, ConsumerFactory<?, ?> consumerFactory, KafkaAdmin kafkaAdmin,
            KafkaProperties kafkaProperties, ConsumerReliabilityProperties properties,
            @Value("${apex.messaging.consumer.topics}") String topics,
            ObjectProvider<ConfigurableKafkaEventConsumer> consumer) {
        return new ReliableKafkaConsumerRuntime(producerFactory, kafkaAdmin.getConfigurationProperties(),
                consumerFactory.getConfigurationProperties().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
                properties, topics, consumer, kafkaProperties.getTemplate().isObservationEnabled());
    }

    @Bean
    @SuppressWarnings("unchecked")
    ConcurrentKafkaListenerContainerFactory<Object, Object> apexReliableKafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer, ConsumerFactory<?, ?> consumerFactory,
            ReliableKafkaConsumerRuntime runtime,
            ObjectProvider<ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>>> customizers) {
        var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
        configurer.configure(factory, (ConsumerFactory<Object, Object>) consumerFactory);
        // A global application discard filter must not silently bypass APEX contract/DLT handling.
        // Other factories retain their own filters; this factory routes every record through APEX.
        factory.setRecordFilterStrategy(null);
        validate(factory.getContainerProperties(), consumerFactory, Boolean.TRUE.equals(factory.isBatchListener()));
        // Match Boot's default-factory customizer support, but keep the mandatory APEX recovery policy.
        factory.setContainerCustomizer(container -> {
            customizers.ifAvailable(customizer -> customizer.configure(container));
            validate(container.getContainerProperties(), consumerFactory, Boolean.TRUE.equals(factory.isBatchListener()));
            container.setCommonErrorHandler(runtime.errorHandler());
        });
        factory.setCommonErrorHandler(runtime.errorHandler());
        return factory;
    }

    @SuppressWarnings("deprecation")
    private static void validate(ContainerProperties container, ConsumerFactory<?, ?> consumerFactory, boolean batch) {
        if (container.getAckMode() != ContainerProperties.AckMode.MANUAL_IMMEDIATE
                || container.isAsyncAcks() || !container.isSyncCommits()
                || batch
                || container.getTransactionManager() != null
                || container.getKafkaAwareTransactionManager() != null
                || !"false".equalsIgnoreCase(String.valueOf(consumerFactory.getConfigurationProperties()
                        .get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)))) {
            throw new IllegalStateException("APEX reliable consumer requires record listeners, MANUAL_IMMEDIATE, "
                    + "enable-auto-commit=false, sync commits, no async ACKs and no container transaction manager");
        }
    }
}
