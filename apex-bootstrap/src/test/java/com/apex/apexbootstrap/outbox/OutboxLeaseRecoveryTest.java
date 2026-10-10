package com.apex.apexbootstrap.outbox;


import com.apex.apexbootstrap.testsupport.AbstractIntegrationTest;
import com.apex.infrastructure.outbox.JooqOutboxStore;
import com.apex.platform.messaging.outbox.OutboxRecord;

import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static com.apex.infrastructure.jooq.generated.Tables.OUTBOX_EVENT;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
class OutboxLeaseRecoveryTest {


    @Autowired
    private JooqOutboxStore outboxStore;


    @Autowired
    private DSLContext dsl;



    @Test
    void should_reclaim_event_after_lease_expired()
            throws Exception {

        insertEvents(1);

        List<OutboxRecord> firstClaim =
                outboxStore.claimBatch(
                        1,
                        "publisher-A",
                        Duration.ofMinutes(5)
                );

        assertThat(firstClaim)
                .hasSize(1);

        OutboxRecord event =
                firstClaim.get(0);

        assertThat(event.claimToken())
                .isNotNull();


        dsl.update(OUTBOX_EVENT)

                .set(
                        OUTBOX_EVENT.CLAIMED_UNTIL,
                        OffsetDateTime.now()
                                .minusMinutes(10)
                )

                .where(
                        OUTBOX_EVENT.ID.eq(event.id())
                )

                .execute();

        List<OutboxRecord> secondClaim =
                outboxStore.claimBatch(
                        1,
                        "publisher-B",
                        Duration.ofMinutes(5)
                );

        assertThat(secondClaim)
                .hasSize(1);


        assertThat(
                secondClaim.get(0).claimToken()
        )
                .isNotEqualTo(
                        event.claimToken()
                );
    }


    private void insertEvents(int count) {

        for (int i = 0; i < count; i++) {

            dsl.insertInto(OUTBOX_EVENT)

                    .set(
                            OUTBOX_EVENT.EVENT_ID,
                            UUID.randomUUID()
                    )

                    .set(
                            OUTBOX_EVENT.EVENT_TYPE,
                            "TEST_EVENT"
                    )

                    .set(
                            OUTBOX_EVENT.EVENT_VERSION,
                            1
                    )

                    .set(
                            OUTBOX_EVENT.AGGREGATE_ID,
                            100L + i
                    )

                    .set(
                            OUTBOX_EVENT.PAYLOAD,
                            JSONB.valueOf("{}")
                    )

                    .set(
                            OUTBOX_EVENT.PUBLISHED,
                            false
                    )

                    .set(
                            OUTBOX_EVENT.RETRY_COUNT,
                            0
                    )

                    .execute();
        }
    }

}