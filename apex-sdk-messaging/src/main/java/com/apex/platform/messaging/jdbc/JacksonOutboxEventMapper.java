package com.apex.platform.messaging.jdbc;

import com.apex.platform.events.EventEnvelope;
import com.apex.platform.events.EventMetadata;
import com.apex.platform.messaging.outbox.OutboxEventMapper;
import com.apex.platform.messaging.outbox.OutboxRecord;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

public class JacksonOutboxEventMapper implements OutboxEventMapper {

    private final JsonMapper jsonMapper;

    public JacksonOutboxEventMapper(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    @Override
    public EventEnvelope<?> map(OutboxRecord record) {
        EventMetadata metadata = new EventMetadata(
                record.eventId(),
                record.eventType(),
                record.eventVersion(),
                record.correlationId(),
                record.causationId(),
                record.aggregateId(),
                record.aggregateType(),
                record.sourceService(),
                record.cellId(),
                record.occurredAt()
        );

        try {
            JsonNode payload = jsonMapper.readTree(record.payload());
            return new EventEnvelope<>(metadata, payload);
        } catch (Exception ex) {
            throw new IllegalStateException("Cannot deserialize outbox payload", ex);
        }
    }
}
