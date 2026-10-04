package com.apex.infrastructure.outbox;



import com.apex.platform.messaging.outbox.OutboxRecord;
import com.apex.platform.messaging.outbox.OutboxStore;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.springframework.stereotype.Repository;

import java.util.List;

import static com.apex.infrastructure.jooq.generated.tables.OutboxEvent.OUTBOX_EVENT;


@Repository
public class JooqOutboxStore implements OutboxStore {


    private final DSLContext dsl;


    public JooqOutboxStore(
            DSLContext dsl
    ) {
        this.dsl = dsl;
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

                .orderBy(
                        OUTBOX_EVENT.ID.asc()
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

                            String.valueOf(
                                    record.getAggregateId()
                            ),

                            payload.data(),

                            record.getRetryCount()

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

                .where(
                        OUTBOX_EVENT.ID.eq(id)
                )

                .execute();

    }


    @Override
    public void markFailed(
            Long id,
            String error
    ) {


        dsl.update(
                        OUTBOX_EVENT
                )

                .set(
                        OUTBOX_EVENT.RETRY_COUNT,
                        OUTBOX_EVENT.RETRY_COUNT.plus(1)
                )

                .where(
                        OUTBOX_EVENT.ID.eq(id)
                )

                .execute();

    }

}