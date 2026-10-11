package com.apex.infrastructure.outbox;


import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;


@Component
@Endpoint(id = "outbox")
public class OutboxEndpoint {


    private final OutboxOperationalService operationalService;


    public OutboxEndpoint(
            OutboxOperationalService operationalService
    ){

        this.operationalService = operationalService;

    }



    @ReadOperation
    public OutboxStatus outboxStatus(){


        long pendingEvents =
                operationalService.countPending();



        Instant oldestEvent =
                operationalService.oldestPendingEventTime();



        long oldestAgeSeconds = 0;


        if(oldestEvent != null){

            oldestAgeSeconds =
                    Duration.between(
                                    oldestEvent,
                                    Instant.now()
                            )
                            .getSeconds();

        }



        return new OutboxStatus(

                pendingEvents,

                oldestAgeSeconds,

                operationalService.countExpiredClaims(),

                operationalService.countRetrying()

        );

    }



    public record OutboxStatus(

            long pendingEvents,

            long oldestEventAgeSeconds,

            long expiredClaims,

            long retryingEvents

    ){}


}