package com.apex.infrastructure.persistence.jooq;

import com.apex.credit.application.outbox.OutboxMessage;
import com.apex.credit.application.port.out.OutboxRepository;

import org.jooq.DSLContext;
import org.jooq.JSONB;

import org.springframework.stereotype.Repository;

import static com.apex.infrastructure.jooq.generated.Tables.FINANCIAL_TRANSACTION;
import static com.apex.infrastructure.jooq.generated.Tables.OUTBOX_EVENT;


@Repository
public class JooqOutboxRepository
        implements OutboxRepository {


    private final DSLContext dsl;


    public JooqOutboxRepository(
            DSLContext dsl
    ) {
        this.dsl = dsl;
    }


    @Override
    public void append(
            OutboxMessage message
    ) {

        Long aggregateDbId =
                dsl.select(
                                FINANCIAL_TRANSACTION.ID
                        )

                        .from(
                                FINANCIAL_TRANSACTION
                        )

                        .where(
                                FINANCIAL_TRANSACTION.PUBLIC_ID.eq(
                                        message
                                                .aggregateId()
                                                .value()
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


        dsl.insertInto(
                        OUTBOX_EVENT
                )

                .set(
                        OUTBOX_EVENT.EVENT_TYPE,
                        message.eventType()
                )

                .set(
                        OUTBOX_EVENT.AGGREGATE_ID,
                        aggregateDbId
                )

                .set(
                        OUTBOX_EVENT.PAYLOAD,
                        JSONB.jsonb(
                                message.payload()
                        )
                )

                .set(
                        OUTBOX_EVENT.PUBLISHED,
                        false
                )

                .execute();
    }
}