package com.apex.infrastructure.outbox;

import com.apex.platform.messaging.outbox.OutboxRecord;
import com.apex.platform.messaging.outbox.OutboxStore;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

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
        OffsetDateTime now =
                OffsetDateTime.now(clock);

        return dsl.selectFrom(OUTBOX_EVENT)

                .where(
                        OUTBOX_EVENT.PUBLISHED.eq(false)
                )

                .and(
                        OUTBOX_EVENT.DEAD_LETTER.eq(false)
                )

                .and(
                        OUTBOX_EVENT.NEXT_RETRY_AT.isNull()
                                .or(
                                        OUTBOX_EVENT.NEXT_RETRY_AT.lessOrEqual(now)
                                )
                )

                .and(
                        OUTBOX_EVENT.CLAIM_TOKEN.isNull()
                                .or(
                                        OUTBOX_EVENT.CLAIMED_UNTIL.lessOrEqual(now)
                                )
                )

                .orderBy(
                        OUTBOX_EVENT.OCCURRED_AT.asc()
                )

                .limit(size)

                .forUpdate()
                .skipLocked()

                .fetch()
                .map(this::mapRecord);
    }


    @Override
    @Transactional
    public List<OutboxRecord> claimBatch(
            int size,
            String publisherInstanceId,
            Duration lease
    ) {

        OffsetDateTime now =
                OffsetDateTime.now(clock);

        OffsetDateTime claimedUntil =
                now.plus(lease);

        UUID claimToken =
                UUID.randomUUID();


        List<Long> ids =
                dsl.select(
                                OUTBOX_EVENT.ID
                        )
                        .from(OUTBOX_EVENT)
                        .where(
                                OUTBOX_EVENT.PUBLISHED.eq(false)
                        )
                        .and(
                                OUTBOX_EVENT.DEAD_LETTER.eq(false)
                        )
                        .and(
                                OUTBOX_EVENT.NEXT_RETRY_AT.isNull()
                                        .or(
                                                OUTBOX_EVENT.NEXT_RETRY_AT.lessOrEqual(now)
                                        )
                        )
                        .and(
                                OUTBOX_EVENT.CLAIM_TOKEN.isNull()
                                        .or(
                                                OUTBOX_EVENT.CLAIMED_UNTIL.lessOrEqual(now)
                                        )
                        )
                        .orderBy(
                                OUTBOX_EVENT.OCCURRED_AT.asc()
                        )
                        .limit(size)
                        .forUpdate()
                        .skipLocked()
                        .fetch(
                                OUTBOX_EVENT.ID
                        );


        if (ids.isEmpty()) {
            return List.of();
        }


        dsl.update(OUTBOX_EVENT)
                .set(
                        OUTBOX_EVENT.CLAIM_TOKEN,
                        claimToken
                )
                .set(
                        OUTBOX_EVENT.CLAIMED_AT,
                        now
                )
                .set(
                        OUTBOX_EVENT.CLAIMED_UNTIL,
                        claimedUntil
                )
                .set(
                        OUTBOX_EVENT.PUBLISHER_INSTANCE_ID,
                        publisherInstanceId
                )
                .where(
                        OUTBOX_EVENT.ID.in(ids)
                )
                .execute();


        return dsl.selectFrom(OUTBOX_EVENT)
                .where(
                        OUTBOX_EVENT.CLAIM_TOKEN.eq(claimToken)
                )
                .fetch()
                .map(this::mapRecord);
    }

    @Override
    @Transactional
    public boolean markPublished(
            Long id,
            UUID claimToken
    ) {

        return dsl.update(OUTBOX_EVENT)

                .set(
                        OUTBOX_EVENT.PUBLISHED,
                        true
                )

                .set(
                        OUTBOX_EVENT.PUBLISHED_AT,
                        OffsetDateTime.now(clock)
                )

                .set(
                        OUTBOX_EVENT.CLAIM_TOKEN,
                        (UUID) null
                )

                .set(
                        OUTBOX_EVENT.CLAIMED_UNTIL,
                        (OffsetDateTime) null
                )

                .set(
                        OUTBOX_EVENT.PUBLISHER_INSTANCE_ID,
                        (String) null
                )

                .where(
                        OUTBOX_EVENT.ID.eq(id)
                )
                .and(OUTBOX_EVENT.CLAIM_TOKEN.eq(claimToken))

                .execute() == 1;
    }

    @Override
    @Transactional
    public boolean markFailed(
            Long id,
            UUID claimToken,
            String error,
            Instant nextRetryAt
    ) {

        return dsl.update(OUTBOX_EVENT)

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

                .set(
                        OUTBOX_EVENT.CLAIM_TOKEN,
                        (UUID) null
                )

                .set(
                        OUTBOX_EVENT.CLAIMED_UNTIL,
                        (OffsetDateTime) null
                )

                .set(
                        OUTBOX_EVENT.PUBLISHER_INSTANCE_ID,
                        (String) null
                )

                .where(
                        OUTBOX_EVENT.ID.eq(id)
                )
                .and(
                        OUTBOX_EVENT.CLAIM_TOKEN.eq(claimToken)
                )

                .execute() == 1;
    }

    @Override
    public boolean moveToDeadLetter(
            Long id,
            UUID claimToken,
            String error
    ) {

        return dsl.update(OUTBOX_EVENT)

                .set(
                        OUTBOX_EVENT.DEAD_LETTER,
                        true
                )

                .set(
                        OUTBOX_EVENT.DEAD_LETTER_AT,
                        OffsetDateTime.now(clock)
                )

                .set(
                        OUTBOX_EVENT.LAST_ERROR,
                        error
                )

                .set(
                        OUTBOX_EVENT.CLAIM_TOKEN,
                        (UUID) null
                )

                .set(
                        OUTBOX_EVENT.CLAIMED_UNTIL,
                        (OffsetDateTime) null
                )

                .set(
                        OUTBOX_EVENT.PUBLISHER_INSTANCE_ID,
                        (String) null
                )

                .where(
                        OUTBOX_EVENT.ID.eq(id)
                )
                .and(
                        OUTBOX_EVENT.CLAIM_TOKEN.eq(claimToken)
                )

                .execute() == 1;
    }


    private OutboxRecord mapRecord(
            com.apex.infrastructure.jooq.generated.tables.records.OutboxEventRecord record
    ) {

        JSONB payload =
                record.getPayload();


        return new OutboxRecord(

                record.getId(),

                record.getEventId(),

                record.getEventType(),

                record.getEventVersion(),

                record.getCorrelationId(),

                record.getCausationId(),

                String.valueOf(record.getAggregateId()),

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
                        : null,

                record.getClaimToken(),

                record.getClaimedUntil() != null
                        ? record.getClaimedUntil().toInstant()
                        : null,

                record.getPublisherInstanceId()
        );
    }
}
