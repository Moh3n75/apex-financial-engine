package com.apex.infrastructure.persistence.jooq;

import com.apex.credit.application.port.out.OutboxRepository;
import com.apex.platform.events.EventEnvelope;
import com.apex.platform.events.IntegrationEvent;

import org.jooq.DSLContext;
import org.jooq.JSONB;

import org.springframework.stereotype.Repository;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

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

        UUID aggregatePublicId;

        try {
            aggregatePublicId =
                    UUID.fromString(
                            event.metadata()
                                    .aggregateId()
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
                        OUTBOX_EVENT.EVENT_TYPE,
                        event.metadata()
                                .eventType()
                )

                .set(
                        OUTBOX_EVENT.AGGREGATE_ID,
                        aggregateDbId
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
