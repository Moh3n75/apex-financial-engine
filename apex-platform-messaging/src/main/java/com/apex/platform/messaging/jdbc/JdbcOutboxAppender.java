package com.apex.platform.messaging.jdbc;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;

import com.apex.platform.events.EventEnvelope;
import com.apex.platform.events.IntegrationEvent;
import com.apex.platform.messaging.outbox.OutboxAppender;

import tools.jackson.databind.json.JsonMapper;

public class JdbcOutboxAppender implements OutboxAppender {

    private final JdbcTemplate jdbcTemplate;
    private final JsonMapper jsonMapper;

    public JdbcOutboxAppender(JdbcTemplate jdbcTemplate, JsonMapper jsonMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.jsonMapper = jsonMapper;
    }

    @Override
    public void append(EventEnvelope<? extends IntegrationEvent> event) {
        var metadata = event.metadata();
        UUID aggregateId = UUID.fromString(metadata.aggregateId());

        String payloadJson;
        try {
            payloadJson = jsonMapper.writeValueAsString(event.payload());
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to serialize outbox payload", ex);
        }

        jdbcTemplate.update(
                """
                INSERT INTO outbox_event (
                    event_id, event_type, event_version,
                    correlation_id, causation_id,
                    aggregate_id, aggregate_type,
                    source_service, cell_id, occurred_at,
                    payload, published
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS jsonb), false)
                """,
                metadata.eventId(),
                metadata.eventType(),
                metadata.eventVersion(),
                metadata.correlationId(),
                metadata.causationId(),
                aggregateId,
                metadata.aggregateType(),
                metadata.sourceService(),
                metadata.cellId(),
                OffsetDateTime.ofInstant(metadata.occurredAt(), ZoneOffset.UTC),
                payloadJson
        );
    }
}
