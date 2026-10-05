package com.apex.infrastructure.outbox;


import com.apex.platform.events.EventEnvelope;
import com.apex.platform.events.EventMetadata;
import com.apex.platform.messaging.outbox.OutboxEventMapper;
import com.apex.platform.messaging.outbox.OutboxRecord;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.stereotype.Component;


@Component
public class DefaultOutboxEventMapper
        implements OutboxEventMapper {


    private final JsonMapper jsonMapper;


    public DefaultOutboxEventMapper(
            JsonMapper jsonMapper
    ) {

        this.jsonMapper = jsonMapper;
    }


    @Override
    public EventEnvelope<?> map(
            OutboxRecord record
    ) {


        EventMetadata metadata =
                new EventMetadata(

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


        JsonNode payload;

        try {

            payload =
                    jsonMapper.readTree(
                            record.payload()
                    );

        } catch (Exception exception) {

            throw new IllegalStateException(
                    "Cannot deserialize outbox payload",
                    exception
            );
        }


        return new EventEnvelope<>(
                metadata,
                payload
        );
    }
}