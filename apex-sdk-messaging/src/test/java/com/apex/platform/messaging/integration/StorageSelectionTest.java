package com.apex.platform.messaging.integration;

import com.apex.platform.context.autoconfigure.ApexExecutionContextAutoConfiguration;
import com.apex.platform.messaging.consumer.ProcessedEventStore;
import com.apex.platform.messaging.jdbc.*;
import com.apex.platform.messaging.outbox.*;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class StorageSelectionTest {
    @org.springframework.context.annotation.Configuration(proxyBeanMethods=false)
    static class PrimaryOwner {
        @org.springframework.context.annotation.Bean
        @org.springframework.context.annotation.Primary
        OutboxAppender primaryAppender(){return mock(OutboxAppender.class);}
        @org.springframework.context.annotation.Bean
        OutboxAppender secondaryAppender(){return mock(OutboxAppender.class);}
    }
    final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ApexExecutionContextAutoConfiguration.class,
                    PlatformCellMessagingAutoConfiguration.class))
            .withPropertyValues("apex.messaging.cell.enabled=true", "apex.messaging.outbox.publisher.enabled=false");

    @Test void customAppenderAndStoreWinIndividually() {
        var appender = mock(OutboxAppender.class);
        var store = mock(OutboxStore.class);
        runner.withBean(JdbcTemplate.class, () -> mock(JdbcTemplate.class))
                .withBean(OutboxAppender.class, () -> appender).run(c -> {
                    assertThat(c).hasNotFailed().hasSingleBean(OutboxAppender.class);
                    assertThat(c.getBean(OutboxAppender.class)).isSameAs(appender);
                    assertThat(c.getBean(OutboxStore.class)).isInstanceOf(JdbcOutboxStore.class);
                });
        runner.withBean(JdbcTemplate.class, () -> mock(JdbcTemplate.class))
                .withBean(OutboxStore.class, () -> store).run(c -> {
                    assertThat(c).hasNotFailed().hasSingleBean(OutboxStore.class);
                    assertThat(c.getBean(OutboxStore.class)).isSameAs(store);
                });
    }

    @Test void customStorageNeedsNoJdbcTemplateOrDriverAndDoesNotOpenConnections() {
        runner.withClassLoader(new FilteredClassLoader("org.postgresql"))
                .withPropertyValues("apex.messaging.storage.provider=custom")
                .withBean(OutboxAppender.class, () -> mock(OutboxAppender.class))
                .run(c -> assertThat(c).hasNotFailed().doesNotHaveBean(JdbcTemplate.class)
                        .doesNotHaveBean(JdbcOutboxStore.class).doesNotHaveBean(ProcessedEventStore.class));
    }

    @Test void unsupportedSelectionAndMissingCustomStoreFailClearly() {
        for (String value : new String[]{"oracle", "typo", "", "POSTGRESQL"})
            runner.withPropertyValues("apex.messaging.storage.provider=" + value)
                    .run(c -> assertThat(c).hasFailed().getFailure().hasMessageContaining("Unsupported apex.messaging.storage.provider"));
        runner.withPropertyValues("apex.messaging.storage.provider=custom")
                .run(c -> assertThat(c).hasFailed().getFailure().hasMessageContaining("requires owner bean OutboxAppender"));
    }

    @Test void duplicateOwnerBeansFailBeforeRuntime() {
        runner.withPropertyValues("apex.messaging.storage.provider=custom")
                .withBean("first", OutboxAppender.class, () -> mock(OutboxAppender.class))
                .withBean("second", OutboxAppender.class, () -> mock(OutboxAppender.class))
                .run(c -> assertThat(c).hasFailed().getFailure().hasMessageContaining("Multiple messaging storage beans"));
    }

    @Test void nativePrimaryOwnerSelectionRemainsSupported() {
        runner.withPropertyValues("apex.messaging.storage.provider=custom")
                .withUserConfiguration(PrimaryOwner.class).run(c -> {
                    assertThat(c).hasNotFailed();
                    assertThat(c.getBean(OutboxAppender.class)).isSameAs(c.getBean("primaryAppender"));
                });
    }

    @Test void missingPostgresqlDriverFailsWithoutFallback() {
        runner.withClassLoader(new FilteredClassLoader("org.postgresql"))
                .withBean(JdbcTemplate.class, () -> mock(JdbcTemplate.class))
                .run(c -> assertThat(c).hasFailed().getFailure().hasStackTraceContaining("requires the PostgreSQL JDBC driver"));
    }

    @Test void disabledMessagingIgnoresProviderAndNeverTouchesDataSource() {
        var ds = mock(DataSource.class);
        runner.withPropertyValues("apex.messaging.cell.enabled=false", "apex.messaging.storage.provider=oracle")
                .withBean(DataSource.class, () -> ds).run(c -> {
                    assertThat(c).hasNotFailed().doesNotHaveBean(OutboxAppender.class);
                    verifyNoInteractions(ds);
                });
    }

    @Test void productDetectionIsDeferredUntilStorageUseAndRejectsNonPostgresql() throws Exception {
        var ds = mock(DataSource.class); var connection = mock(Connection.class); var metadata = mock(DatabaseMetaData.class);
        when(ds.getConnection()).thenReturn(connection); when(connection.getMetaData()).thenReturn(metadata);
        when(metadata.getDatabaseProductName()).thenReturn("Oracle");
        var store = new JdbcOutboxStore(new JdbcTemplate(ds), java.time.Clock.systemUTC());
        verifyNoInteractions(ds);
        assertThatThrownBy(() -> store.claimBatch(1, "test", java.time.Duration.ofSeconds(5)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("requires database product PostgreSQL");
        verify(connection, never()).prepareStatement(anyString());
    }
}
