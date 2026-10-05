package com.apex.infrastructure.outbox;


import com.apex.platform.messaging.outbox.OutboxStatistics;

import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import java.time.Instant;


import static com.apex.infrastructure.jooq.generated.tables.OutboxEvent.OUTBOX_EVENT;


@Repository
public class JooqOutboxStatistics
        implements OutboxStatistics {


    private final DSLContext dsl;


    public JooqOutboxStatistics(
            DSLContext dsl
    ) {

        this.dsl = dsl;

    }


    @Override
    public long pendingCount() {


        return dsl
                .selectCount()
                .from(OUTBOX_EVENT)
                .where(
                        OUTBOX_EVENT.PUBLISHED.eq(false)
                )
                .and(
                        OUTBOX_EVENT.DEAD_LETTER.eq(false)
                )
                .fetchOne(0, Long.class);

    }



    @Override
    public Instant oldestUnpublishedEventTime() {


        var result =
                dsl
                        .select(
                                OUTBOX_EVENT.OCCURRED_AT
                        )
                        .from(OUTBOX_EVENT)
                        .where(
                                OUTBOX_EVENT.PUBLISHED.eq(false)
                        )
                        .and(
                                OUTBOX_EVENT.DEAD_LETTER.eq(false)
                        )
                        .orderBy(
                                OUTBOX_EVENT.OCCURRED_AT.asc()
                        )
                        .limit(1)
                        .fetchOne();


        if(result == null) {

            return null;

        }


        return result.value1()
                .toInstant();

    }

}