package com.apex.infrastructure.outbox;



import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

import java.time.Clock;


@Component("outbox")
public class OutboxHealthIndicator
        implements HealthIndicator {


    private final OutboxHealthService healthService;


    public OutboxHealthIndicator(
            OutboxHealthService healthService
    ){
        this.healthService = healthService;
    }



    @Override
    public Health health() {


        var status =
                healthService.check();


        if(status.healthy()){

            return Health.up()

                    .withDetail(
                            "pendingEvents",
                            status.pendingEvents()
                    )

                    .withDetail(
                            "oldestEventAgeSeconds",
                            status.oldestEventAgeSeconds()
                    )

                    .build();

        }


        return Health.down()

                .withDetail(
                        "reason",
                        status.reason()
                )

                .withDetail(
                        "pendingEvents",
                        status.pendingEvents()
                )

                .build();

    }

}