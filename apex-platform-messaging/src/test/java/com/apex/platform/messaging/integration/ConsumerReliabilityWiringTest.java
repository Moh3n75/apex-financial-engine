package com.apex.platform.messaging.integration;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicBoolean;

import com.apex.platform.context.autoconfigure.ApexExecutionContextAutoConfiguration;
import com.apex.platform.messaging.consumer.*;
import com.apex.platform.messaging.jdbc.*;
import com.apex.platform.messaging.kafka.KafkaMessagePublisher;
import com.apex.platform.messaging.outbox.OutboxPublisher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;

@Tag("unit")
class ConsumerReliabilityWiringTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(PropertyPlaceholderAutoConfiguration.class,
                    ApexExecutionContextAutoConfiguration.class, KafkaAutoConfiguration.class,
                    PlatformCellMessagingAutoConfiguration.class, PlatformConsumerReliabilityAutoConfiguration.class))
            .withBean(JdbcTemplate.class, () -> mock(JdbcTemplate.class))
            .withPropertyValues("spring.application.name=reliability-wiring", "apex.cell.id=test",
                    "apex.messaging.cell.enabled=true", "spring.kafka.bootstrap-servers=127.0.0.1:1",
                    "spring.kafka.listener.auto-startup=false", "spring.kafka.listener.ack-mode=manual_immediate",
                    "spring.kafka.listener.poll-timeout=250ms", "spring.kafka.listener.observation-enabled=true",
                    "spring.kafka.consumer.enable-auto-commit=false", "spring.kafka.consumer.group-id=wiring",
                    "apex.messaging.consumer.topics=wiring.source", "apex.messaging.publisher.topic=wiring.out",
                    "apex.messaging.consumer.reliability.preflight-enabled=false");

    @ParameterizedTest
    @CsvSource({"true,false,true", "false,false,true", "false,true,false", "false,true,true", "true,true,true"})
    void roleAndOptInMatrixDoesNotLeakGlobalRecoveryOrOutboxPublisher(boolean publisher, boolean consumer, boolean reliable) {
        runner.withPropertyValues("apex.messaging.outbox.publisher.enabled=" + publisher,
                "apex.messaging.consumer.enabled=" + consumer,
                "apex.messaging.consumer.reliability.enabled=" + reliable).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBeansOfType(KafkaMessagePublisher.class)).hasSize(publisher ? 1 : 0);
            assertThat(context.getBeansOfType(OutboxPublisher.class)).hasSize(publisher ? 1 : 0);
            assertThat(context.getBeansOfType(ReliableKafkaConsumerRuntime.class)).hasSize(consumer && reliable ? 1 : 0);
            assertThat(context.getBeansOfType(CommonErrorHandler.class)).isEmpty();
            assertThat(context.getBeansOfType(KafkaTemplate.class)).hasSize(1); // Boot-owned, not a second global DLT template.
            assertThat(context.containsBean("apexReliableKafkaListenerContainerFactory")).isEqualTo(consumer && reliable);
            if (consumer && reliable) {
                var factory = context.getBean("apexReliableKafkaListenerContainerFactory",
                        ConcurrentKafkaListenerContainerFactory.class);
                assertThat(factory.getContainerProperties().getPollTimeout()).isEqualTo(250);
                assertThat(factory.getContainerProperties().isObservationEnabled()).isTrue();
                assertThat(factory.getConsumerFactory()).isSameAs(context.getBean("kafkaConsumerFactory"));
            }
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"spring.kafka.listener.ack-mode=batch", "spring.kafka.consumer.enable-auto-commit=true",
            "spring.kafka.listener.async-acks=true", "apex.messaging.consumer.reliability.max-attempts=0",
            "spring.kafka.listener.type=batch"})
    void incompatibleReliableConfigurationFailsInsteadOfFallingBack(String property) {
        runner.withPropertyValues("apex.messaging.consumer.enabled=true", "apex.messaging.outbox.publisher.enabled=false",
                "apex.messaging.consumer.reliability.enabled=true", property).run(context ->
                assertThat(context).hasFailed());
    }

    @Test
    void bootContainerCustomizerIsPreservedButCannotReplaceReliableRecovery() {
        runner.withUserConfiguration(CustomizerConfiguration.class)
                .withPropertyValues("apex.messaging.consumer.enabled=true", "apex.messaging.outbox.publisher.enabled=false",
                        "apex.messaging.consumer.reliability.enabled=true").run(context -> {
                    assertThat(context).hasNotFailed();
                    var runtime = context.getBean(ReliableKafkaConsumerRuntime.class);
                    var containers = context.getBean(org.springframework.kafka.config.KafkaListenerEndpointRegistry.class)
                            .getListenerContainers();
                    assertThat(containers).hasSize(1).allSatisfy(container -> {
                        assertThat(container.getContainerProperties().getPollTimeout()).isEqualTo(333);
                        assertThat(((org.springframework.kafka.listener.AbstractMessageListenerContainer<?, ?>) container)
                                .getCommonErrorHandler()).isSameAs(runtime.errorHandler());
                    });
                });
    }

    @org.springframework.context.annotation.Configuration(proxyBeanMethods = false)
    static class CustomizerConfiguration {
        @org.springframework.context.annotation.Bean
        org.springframework.kafka.config.ContainerCustomizer<Object, Object,
                org.springframework.kafka.listener.ConcurrentMessageListenerContainer<Object, Object>> customizer() {
            return container -> {
                container.getContainerProperties().setPollTimeout(333);
                container.setCommonErrorHandler(new org.springframework.kafka.listener.DefaultErrorHandler());
            };
        }
    }

    @Test
    void customDispatcherIgnoredResultStillCannotAckInReliableModeAndLegacyConstructorRetainsBehavior() {
        EventMessageDispatcher ignores = (raw, context) -> DispatchResult.IGNORED;
        var record = new ConsumerRecord<>("test.source", 1, 7, "safe-key",
                "{\"metadata\":{\"eventType\":\"UNKNOWN\",\"eventVersion\":9},\"payload\":\"private-value\"}");
        var ack = new AtomicBoolean();
        var strict = new ConfigurableKafkaEventConsumer(ignores, Clock.systemUTC(), "test", true,
                JsonMapper.builder().build());
        assertThatThrownBy(() -> strict.consume(record, () -> ack.set(true)))
                .isInstanceOf(UnsupportedEventContractException.class).hasMessageContaining("UNKNOWN")
                .hasMessageContaining("version=9").hasMessageContaining("offset=7")
                .hasMessageNotContaining("private-value");
        assertThat(ack).isFalse();
        new ConfigurableKafkaEventConsumer(ignores, Clock.systemUTC(), "legacy")
                .consume(record, () -> ack.set(true));
        assertThat(ack).isTrue();
    }
}
