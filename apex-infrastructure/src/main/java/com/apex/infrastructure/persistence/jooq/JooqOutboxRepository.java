package com.apex.infrastructure.persistence.jooq;

import com.apex.credit.application.port.out.OutboxRepository;
import com.apex.platform.events.EventEnvelope;
import com.apex.platform.events.IntegrationEvent;

import org.jooq.DSLContext;
import org.jooq.JSONB;

import org.springframework.stereotype.Repository;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.time.ZoneOffset;
import java.util.UUID;

import static com.apex.infrastructure.jooq.generated.Tables.FINANCIAL_TRANSACTION;
import static com.apex.infrastructure.jooq.generated.Tables.OUTBOX_EVENT;

@Repository
public class JooqOutboxRepository
        implements OutboxRepository {

    private final DSLContext dsl;
    private final JsonMapper jsonMapper;

    public JooqOutboxRepository(
            DSLContext dsl,
            JsonMapper jsonMapper
    ) {
        this.dsl = dsl;
        this.jsonMapper = jsonMapper;
    }

    @Override
    public void append(
            EventEnvelope<? extends IntegrationEvent> event
    ) {

        var metadata = event.metadata();

        UUID aggregatePublicId;

        try {
            aggregatePublicId =
                    UUID.fromString(
                            metadata.aggregateId()
                    );
        }
        catch (IllegalArgumentException exception) {
            throw new IllegalStateException(
                    "Outbox aggregateId must be a valid UUID",
                    exception
            );
        }

        Long aggregateDbId =
                dsl.select(
                                FINANCIAL_TRANSACTION.ID
                        )
                        .from(
                                FINANCIAL_TRANSACTION
                        )
                        .where(
                                FINANCIAL_TRANSACTION.PUBLIC_ID.eq(
                                        aggregatePublicId
                                )
                        )
                        .fetchOne(
                                FINANCIAL_TRANSACTION.ID
                        );

        if (aggregateDbId == null) {
            throw new IllegalStateException(
                    "Financial transaction not found for outbox"
            );
        }

        String payloadJson;

        try {
            payloadJson =
                    jsonMapper.writeValueAsString(
                            event.payload()
                    );
        }
        catch (JacksonException exception) {
            throw new IllegalStateException(
                    "Failed to serialize outbox event payload",
                    exception
            );
        }

        dsl.insertInto(
                        OUTBOX_EVENT
                )

                .set(
                        OUTBOX_EVENT.EVENT_ID,
                        metadata.eventId()
                )

                .set(
                        OUTBOX_EVENT.EVENT_TYPE,
                        metadata.eventType()
                )

                .set(
                        OUTBOX_EVENT.EVENT_VERSION,
                        metadata.eventVersion()
                )

                .set(
                        OUTBOX_EVENT.CORRELATION_ID,
                        metadata.correlationId()
                )

                .set(
                        OUTBOX_EVENT.CAUSATION_ID,
                        metadata.causationId()
                )

                .set(
                        OUTBOX_EVENT.AGGREGATE_ID,
                        aggregateDbId
                )

                .set(
                        OUTBOX_EVENT.AGGREGATE_TYPE,
                        metadata.aggregateType()
                )

                .set(
                        OUTBOX_EVENT.SOURCE_SERVICE,
                        metadata.sourceService()
                )

                .set(
                        OUTBOX_EVENT.CELL_ID,
                        metadata.cellId()
                )

                .set(
                        OUTBOX_EVENT.OCCURRED_AT,
                        metadata.occurredAt()
                                .atOffset(ZoneOffset.UTC)
                )

                .set(
                        OUTBOX_EVENT.PAYLOAD,
                        JSONB.jsonb(
                                payloadJson
                        )
                )

                .set(
                        OUTBOX_EVENT.PUBLISHED,
                        false
                )

                .execute();
    }
}
