package com.apex.messaging.core;

import com.apex.platform.messaging.consumer.*;
import com.apex.platform.messaging.jdbc.*;
import com.apex.platform.messaging.kafka.KafkaMessagePublisher;
import com.apex.platform.messaging.outbox.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class NeutralCompositionTest {
    @Configuration(proxyBeanMethods=false) @EnableAutoConfiguration static class App {}
    final ApplicationContextRunner runner = new ApplicationContextRunner().withUserConfiguration(App.class);

    @Test void disabledDefaultBootHasNoDatasourceOrStorage() {
        runner.run(c -> {
            assertThat(c).hasNotFailed().doesNotHaveBean(javax.sql.DataSource.class).doesNotHaveBean(OutboxAppender.class);
            for (String type : new String[]{"org.postgresql.Driver", "oracle.jdbc.OracleDriver", "io.r2dbc.spi.ConnectionFactory"})
                assertThatThrownBy(() -> Class.forName(type)).isInstanceOf(ClassNotFoundException.class);
        });
    }

    @ParameterizedTest @CsvSource({"true,false", "false,true", "true,true", "false,false"})
    void ownerStorageSupportsAllModesWithoutDatabase(boolean publisher, boolean consumer) {
        var appender = mock(OutboxAppender.class); var store = mock(OutboxStore.class); var inbox = mock(ProcessedEventStore.class);
        var selected=runner.withPropertyValues("apex.messaging.cell.enabled=true", "spring.application.name=neutral",
                "apex.cell.id=neutral", "spring.kafka.listener.auto-startup=false", "spring.kafka.admin.auto-create=false",
                "apex.messaging.outbox.publisher.enabled="+publisher, "apex.messaging.consumer.enabled="+consumer)
                .withBean(OutboxAppender.class, () -> appender).withBean(TaskScheduler.class, () -> mock(TaskScheduler.class));
        if (publisher) selected=selected.withPropertyValues("apex.messaging.publisher.topic=neutral.events")
                .withBean(OutboxStore.class, () -> store).withBean(KafkaMessagePublisher.class, () -> mock(KafkaMessagePublisher.class));
        if (consumer) selected=selected.withPropertyValues("apex.messaging.consumer.topics=neutral.events", "spring.kafka.consumer.group-id=neutral")
                .withBean(ProcessedEventStore.class, () -> inbox);
        selected.run(c -> {
            assertThat(c).hasNotFailed().doesNotHaveBean(javax.sql.DataSource.class).doesNotHaveBean(JdbcOutboxAppender.class)
                    .doesNotHaveBean(JdbcOutboxStore.class).doesNotHaveBean(JdbcProcessedEventStore.class);
            assertThat(c.getBean(OutboxAppender.class)).isSameAs(appender);
            assertThat(c.getBeansOfType(OutboxPublisher.class)).hasSize(publisher?1:0);
            assertThat(c.getBeansOfType(EventHandlerExecutor.class)).hasSize(consumer?1:0);
        });
    }

    @Test void neutralSelectionRequiresOwnerStorageAndDoesNotChoosePostgresql() {
        runner.withPropertyValues("apex.messaging.cell.enabled=true", "apex.messaging.outbox.publisher.enabled=false")
                .run(c -> assertThat(c).hasFailed().getFailure().hasMessageContaining("provider=custom requires owner bean"));
    }
}
