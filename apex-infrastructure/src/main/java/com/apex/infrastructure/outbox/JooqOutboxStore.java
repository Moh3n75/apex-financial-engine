package com.apex.infrastructure.outbox;


import com.apex.platform.messaging.outbox.OutboxRecord;
import com.apex.platform.messaging.outbox.OutboxStore;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.springframework.stereotype.Repository;

import java.time.Clock;
import java.time.Instant;

import java.time.OffsetDateTime;
import java.util.List;

import static com.apex.infrastructure.jooq.generated.tables.OutboxEvent.OUTBOX_EVENT;


@Repository
public class JooqOutboxStore implements OutboxStore {


    private final DSLContext dsl;

    private final Clock clock;


    public JooqOutboxStore(
            DSLContext dsl,
            Clock clock
    ) {
        this.dsl = dsl;
        this.clock = clock;
    }


    @Override
    public List<OutboxRecord> findBatch(
            int size
    ) {


        return dsl
                .selectFrom(OUTBOX_EVENT)

                .where(
                        OUTBOX_EVENT.PUBLISHED.eq(false)
                )

                .and(
                        OUTBOX_EVENT.NEXT_RETRY_AT.isNull()
                                .or(
                                        OUTBOX_EVENT.NEXT_RETRY_AT.lessOrEqual(
                                                OffsetDateTime.now(clock)
                                        )
                                )
                )

                .orderBy(
                        OUTBOX_EVENT.OCCURRED_AT.asc()
                )

                .limit(size)

                .forUpdate()
                .skipLocked()

                .fetch()

                .map(record -> {


                    JSONB payload =
                            record.getPayload();


                    return new OutboxRecord(

                            record.getId(),

                            record.getEventId(),

                            record.getEventType(),

                            record.getEventVersion(),

                            record.getCorrelationId(),

                            record.getCausationId(),

                            String.valueOf(
                                    record.getAggregateId()
                            ),

                            record.getAggregateType(),

                            record.getSourceService(),

                            record.getCellId(),

                            record.getOccurredAt() != null
                                    ? record.getOccurredAt().toInstant()
                                    : null,

                            payload.data(),

                            record.getRetryCount(),

                            record.getNextRetryAt() != null
                                    ? record.getNextRetryAt().toInstant()
                                    : null
                    );

                });

    }


    @Override
    public void markPublished(
            Long id
    ) {

        dsl.update(
                        OUTBOX_EVENT
                )

                .set(
                        OUTBOX_EVENT.PUBLISHED,
                        true
                )

                .set(
                        OUTBOX_EVENT.PUBLISHED_AT,
                        OffsetDateTime.now(clock)
                )
                .where(
                        OUTBOX_EVENT.ID.eq(id)
                )

                .execute();

    }


    @Override
    public void markFailed(
            Long id,
            String error,
            Instant nextRetryAt

    ) {


        dsl.update(
                        OUTBOX_EVENT
                )

                .set(
                        OUTBOX_EVENT.RETRY_COUNT,
                        OUTBOX_EVENT.RETRY_COUNT.plus(1)
                )
                .set(
                        OUTBOX_EVENT.LAST_ERROR,
                        error
                )
                .set(
                        OUTBOX_EVENT.NEXT_RETRY_AT,
                        nextRetryAt != null
                                ? OffsetDateTime.ofInstant(
                                nextRetryAt,
                                clock.getZone()
                        )
                                : null
                )
                .where(
                        OUTBOX_EVENT.ID.eq(id)
                )

                .execute();

    }

}