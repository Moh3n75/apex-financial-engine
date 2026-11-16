package com.apex.platform.messaging.jdbc;

import com.apex.platform.messaging.consumer.ProcessedEventStore;
import com.apex.platform.messaging.observation.MessagingObservation;
import com.apex.platform.messaging.outbox.OutboxAppender;
import com.apex.platform.messaging.outbox.OutboxStore;
import java.time.Clock;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.ClassUtils;
import tools.jackson.databind.json.JsonMapper;

/** PostgreSQL defaults only; owner replacements and transaction proxies remain native Spring beans. */
@Configuration(proxyBeanMethods = false)
@Conditional(MessagingStorageSelection.class)
class PostgresqlStorageConfiguration {
    @Bean
    @ConditionalOnMissingBean
    OutboxAppender outboxAppender(JdbcTemplate jdbc, JsonMapper mapper, MessagingObservation observations) {
        requireDriver();
        return new JdbcOutboxAppender(jdbc, mapper, observations);
    }

    @Bean
    @ConditionalOnMissingBean
    OutboxStore outboxStore(JdbcTemplate jdbc, Clock clock, MessagingObservation observations) {
        requireDriver();
        return new JdbcOutboxStore(jdbc, clock, observations);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "apex.messaging.consumer", name = "enabled", havingValue = "true")
    ProcessedEventStore processedEventStore(JdbcTemplate jdbc, Clock clock) {
        requireDriver();
        return new JdbcProcessedEventStore(jdbc, clock);
    }

    private static void requireDriver() {
        if (!ClassUtils.isPresent("org.postgresql.Driver", ClassUtils.getDefaultClassLoader()))
            throw new IllegalStateException("PostgreSQL storage requires the PostgreSQL JDBC driver; "
                    + "select apex.messaging.storage.provider=custom with owner storage beans instead");
    }
}
