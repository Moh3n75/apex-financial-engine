package com.apex.infrastructure.outbox;


import org.jooq.DSLContext;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;


import static com.apex.infrastructure.jooq.generated.Tables.OUTBOX_EVENT;


@Service
public class OutboxHealthService {


    private final DSLContext dsl;

    private final Clock clock;


    private static final long MAX_PENDING_EVENTS = 10000;

    private static final Duration MAX_EVENT_AGE =
            Duration.ofMinutes(30);



    public OutboxHealthService(
            DSLContext dsl,
            Clock clock
    ){

        this.dsl = dsl;
        this.clock = clock;

    }



    public OutboxHealthStatus check(){


        Long pendingEvents =
                dsl.selectCount()

                        .from(OUTBOX_EVENT)

                        .where(
                                OUTBOX_EVENT.PUBLISHED.eq(false)
                        )

                        .fetchOne(0, Long.class);



        Instant oldestEvent =
                dsl.select(
                                OUTBOX_EVENT.CREATED_AT.min()
                        )

                        .from(OUTBOX_EVENT)

                        .where(
                                OUTBOX_EVENT.PUBLISHED.eq(false)
                        )

                        .fetchOneInto(Instant.class);



        long ageSeconds = 0;


        if(oldestEvent != null){

            ageSeconds =
                    Duration.between(
                                    oldestEvent,
                                    clock.instant()
                            )
                            .getSeconds();

        }



        boolean healthy =
                pendingEvents <= MAX_PENDING_EVENTS
                        &&
                        ageSeconds <= MAX_EVENT_AGE.toSeconds();



        return new OutboxHealthStatus(
                healthy,
                pendingEvents,
                ageSeconds,
                healthy
                        ?
                        null
                        :
                        "Outbox backlog is too high"
        );

    }

}