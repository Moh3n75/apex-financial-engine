package com.apex.infrastructure.outbox;


import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import org.springframework.stereotype.Component;


@Component
public class OutboxMetrics {


    private final Counter claimSuccess;

    private final Counter claimEmpty;

    private final Counter publishSuccess;

    private final Counter publishFailed;

    private final Counter retryCount;

    private final Counter deadLetterCount;


    public OutboxMetrics(
            MeterRegistry registry
    ){

        claimSuccess =
                Counter.builder(
                                "outbox.claim.success"
                        )
                        .description(
                                "Number of successfully claimed events"
                        )
                        .register(registry);



        claimEmpty =
                Counter.builder(
                                "outbox.claim.empty"
                        )
                        .description(
                                "Number of empty claim attempts"
                        )
                        .register(registry);



        publishSuccess =
                Counter.builder(
                                "outbox.publish.success"
                        )
                        .register(registry);



        publishFailed =
                Counter.builder(
                                "outbox.publish.failed"
                        )
                        .register(registry);



        retryCount =
                Counter.builder(
                                "outbox.retry.count"
                        )
                        .register(registry);



        deadLetterCount =
                Counter.builder(
                                "outbox.deadletter.count"
                        )
                        .register(registry);

    }



    public void claimSuccess(){
        claimSuccess.increment();
    }


    public void claimEmpty(){
        claimEmpty.increment();
    }


    public void publishSuccess(){
        publishSuccess.increment();
    }


    public void publishFailed(){
        publishFailed.increment();
    }


    public void retry(){
        retryCount.increment();
    }


    public void deadLetter(){
        deadLetterCount.increment();
    }

}