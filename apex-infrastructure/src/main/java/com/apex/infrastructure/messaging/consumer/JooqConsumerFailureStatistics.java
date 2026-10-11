package com.apex.infrastructure.messaging.consumer;

import com.apex.platform.messaging.consumer.ConsumerFailureStatistics;
import com.apex.platform.messaging.consumer.ConsumerFailureStatus;

import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import java.time.Instant;

import static com.apex.infrastructure.jooq.generated.tables.ConsumerEventFailure.CONSUMER_EVENT_FAILURE;
import static org.jooq.impl.DSL.min;

@Repository
public class JooqConsumerFailureStatistics
        implements ConsumerFailureStatistics {

    private final DSLContext dsl;

    public JooqConsumerFailureStatistics(
            DSLContext dsl
    ) {
        this.dsl = dsl;
    }

    @Override
    public long countByStatus(
            ConsumerFailureStatus status
    ) {

        Long count =
                dsl.selectCount()
                        .from(CONSUMER_EVENT_FAILURE)
                        .where(
                                CONSUMER_EVENT_FAILURE.STATUS
                                        .eq(status.name())
                        )
                        .fetchOne(0, Long.class);

        return count != null
                ? count
                : 0L;
    }

    @Override
    public long unresolvedCount() {

        Long count =
                dsl.selectCount()
                        .from(CONSUMER_EVENT_FAILURE)
                        .where(
                                CONSUMER_EVENT_FAILURE.STATUS.in(
                                        ConsumerFailureStatus.RETRYING.name(),
                                        ConsumerFailureStatus.DEAD_LETTERED.name(),
                                        ConsumerFailureStatus.RECOVERY_FAILED.name(),
                                        ConsumerFailureStatus.REPLAYING.name(),
                                        ConsumerFailureStatus.REPLAY_FAILED.name()
                                )
                        )
                        .fetchOne(0, Long.class);

        return count != null
                ? count
                : 0L;
    }

    @Override
    public Instant oldestUnresolvedFailureTime() {

        var record =
                dsl.select(
                                min(
                                        CONSUMER_EVENT_FAILURE.FIRST_FAILED_AT
                                )
                        )
                        .from(CONSUMER_EVENT_FAILURE)
                        .where(
                                CONSUMER_EVENT_FAILURE.STATUS.in(
                                        ConsumerFailureStatus.RETRYING.name(),
                                        ConsumerFailureStatus.DEAD_LETTERED.name(),
                                        ConsumerFailureStatus.RECOVERY_FAILED.name(),
                                        ConsumerFailureStatus.REPLAYING.name(),
                                        ConsumerFailureStatus.REPLAY_FAILED.name()
                                )
                        )
                        .fetchOne();

        if (record == null) {
            return null;
        }

        var value =
                record.value1();

        return value != null
                ? value.toInstant()
                : null;
    }
}