package com.apex.platform.messaging.jdbc;

import java.time.Clock;
import java.time.OffsetDateTime;

import org.springframework.jdbc.core.JdbcTemplate;

import com.apex.platform.events.EventMetadata;
import com.apex.platform.messaging.consumer.ConsumerContext;
import com.apex.platform.messaging.consumer.ProcessedEventStore;

public class JdbcProcessedEventStore implements ProcessedEventStore {

    private final JdbcTemplate jdbcTemplate;
    private final Clock clock;

    public JdbcProcessedEventStore(JdbcTemplate jdbcTemplate, Clock clock) {
        this.jdbcTemplate = jdbcTemplate;
        this.clock = clock;
    }

    @Override
    public boolean registerIfAbsent(EventMetadata metadata, ConsumerContext context) {
        int inserted = jdbcTemplate.update(
                """
                INSERT INTO processed_event (
                    consumer_group, event_id, event_type, event_version,
                    topic, partition_id, offset_value, processed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (consumer_group, event_id) DO NOTHING
                """,
                context.consumerGroup(),
                metadata.eventId(),
                metadata.eventType(),
                metadata.eventVersion(),
                context.topic(),
                context.partition(),
                context.offset(),
                OffsetDateTime.now(clock)
        );
        return inserted == 1;
    }
}
