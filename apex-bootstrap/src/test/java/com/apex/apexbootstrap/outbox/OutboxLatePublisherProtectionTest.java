package com.apex.apexbootstrap.outbox;


import com.apex.apexbootstrap.testsupport.AbstractIntegrationTest;
import com.apex.infrastructure.outbox.JooqOutboxStore;
import com.apex.platform.messaging.outbox.OutboxRecord;

import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;


import static com.apex.infrastructure.jooq.generated.Tables.OUTBOX_EVENT;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
class OutboxLatePublisherProtectionTest{


    @Autowired
    private JooqOutboxStore outboxStore;


    @Autowired
    private DSLContext dsl;


    @Test
    void old_publisher_should_not_mark_event_as_published()
            throws Exception {


        // 1- Create event
        insertEvents();


        // 2- Publisher A claim
        List<OutboxRecord> firstClaim =
                outboxStore.claimBatch(
                        1,
                        "publisher-A",
                        Duration.ofMinutes(5)
                );


        OutboxRecord event =
                firstClaim.get(0);


        var oldToken =
                event.claimToken();



        // 3- Expire lease
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



        // 4- Publisher B claims
        List<OutboxRecord> secondClaim =
                outboxStore.claimBatch(
                        1,
                        "publisher-B",
                        Duration.ofMinutes(5)
                );


        assertThat(secondClaim)
                .hasSize(1);



        // 5- Old publisher tries to publish
        boolean result =
                outboxStore.markPublished(
                        event.id(),
                        oldToken
                );


        // 6- Should fail because ownership changed
        assertThat(result)
                .isFalse();

    }



    private void insertEvents(){

        dsl.insertInto(OUTBOX_EVENT)

                .set(
                        OUTBOX_EVENT.EVENT_ID,
                        java.util.UUID.randomUUID()
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
                        100L
                )

                .set(
                        OUTBOX_EVENT.PAYLOAD,
                        org.jooq.JSONB.valueOf("{}")
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