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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.apex.infrastructure.jooq.generated.Tables.OUTBOX_EVENT;
import static org.assertj.core.api.Assertions.assertThat;


@SpringBootTest
@ActiveProfiles("test")
class OutboxClaimConcurrencyTest{


    @Autowired
    private JooqOutboxStore outboxStore;

    @Autowired
    private DSLContext dsl;


    @Test
    void should_not_claim_same_event_twice()
            throws Exception {

        insertEvents(20);


        try (ExecutorService executor =
                Executors.newFixedThreadPool(2)) {


            Callable<List<OutboxRecord>> publisherA =
                    () ->
                            outboxStore.claimBatch(
                                    10,
                                    "publisher-A",
                                    Duration.ofMinutes(5)
                            );


            Callable<List<OutboxRecord>> publisherB =
                    () ->
                            outboxStore.claimBatch(
                                    10,
                                    "publisher-B",
                                    Duration.ofMinutes(5)
                            );


            Future<List<OutboxRecord>> futureA =
                    executor.submit(publisherA);


            Future<List<OutboxRecord>> futureB =
                    executor.submit(publisherB);


            List<OutboxRecord> eventsA =
                    futureA.get();


            List<OutboxRecord> eventsB =
                    futureB.get();

            assertThat(eventsA)
                    .doesNotContainAnyElementsOf(eventsB);


            assertThat(
                    eventsA.size() + eventsB.size()
            )
                    .isEqualTo(20);
        }
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