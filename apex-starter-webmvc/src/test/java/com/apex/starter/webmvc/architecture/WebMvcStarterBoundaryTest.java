package com.apex.starter.webmvc.architecture;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.util.ClassUtils;

@Tag("architecture")
class WebMvcStarterBoundaryTest {
    @Test
    void httpStarterHasNoMessagingKafkaOrJdbcClasspath() {
        ClassLoader loader = getClass().getClassLoader();
        for (String type : new String[]{
                "com.apex.platform.messaging.outbox.OutboxAppender",
                "com.apex.platform.messaging.consumer.ProcessedEventStore",
                "org.apache.kafka.clients.producer.KafkaProducer",
                "org.springframework.kafka.core.KafkaTemplate",
                "org.springframework.jdbc.core.JdbcTemplate"}) {
            assertThat(ClassUtils.isPresent(type, loader)).as("class must be absent: %s", type).isFalse();
        }
    }
}
