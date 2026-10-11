package com.apex.infrastructure.outbox;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.OffsetDateTime;

import static com.apex.infrastructure.jooq.generated.tables.OutboxEvent.OUTBOX_EVENT;

@Service
public class OutboxOperationalService {


    private final DSLContext dsl;

    public OutboxOperationalService(DSLContext dsl) {
        this.dsl = dsl;
    }


    public long countPending(){

        Long count = dsl
                .selectCount()
                .from(OUTBOX_EVENT)
                .where(
                        OUTBOX_EVENT.PUBLISHED.eq(false)
                )
                .fetchOne(0, Long.class);


        return count != null ? count : 0;
    }


    public Instant oldestPendingEventTime(){

        return dsl
                .select(
                        DSL.min(OUTBOX_EVENT.CREATED_AT)
                )
                .from(OUTBOX_EVENT)
                .where(
                        OUTBOX_EVENT.PUBLISHED.eq(false)
                )
                .fetchOneInto(Instant.class);

    }

    public long countExpiredClaims(){

        Long count = dsl
                .selectCount()
                .from(OUTBOX_EVENT)
                .where(
                        OUTBOX_EVENT.PUBLISHED.eq(false)
                )
                .and(
                        OUTBOX_EVENT.CLAIM_TOKEN.isNotNull()
                )
                .and(
                        OUTBOX_EVENT.CLAIMED_UNTIL
                                .lessThan(
                                        OffsetDateTime.now()
                                )
                )
                .fetchOne(0, Long.class);

        return count != null ? count : 0;

    }

    public long countRetrying(){

        Long count = dsl
                .selectCount()
                .from(OUTBOX_EVENT)
                .where(
                        OUTBOX_EVENT.RETRY_COUNT.gt(0)
                )
                .and(
                        OUTBOX_EVENT.PUBLISHED.eq(false)
                )
                .fetchOne(0, Long.class);

        return count != null ? count : 0;
    }

}