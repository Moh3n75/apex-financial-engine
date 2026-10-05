package com.apex.infrastructure.outbox;


import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import org.springframework.stereotype.Component;


@Component
public class OutboxMetrics {


    private final Counter publishedCounter;

    private final Counter failedCounter;

    private final Counter deadLetterCounter;



    public OutboxMetrics(
            MeterRegistry registry
    ) {


        this.publishedCounter =
                Counter.builder("apex.outbox.published")
                        .description(
                                "Number of successfully published outbox events"
                        )
                        .register(registry);



        this.failedCounter =
                Counter.builder("apex.outbox.failed")
                        .description(
                                "Number of failed outbox publish attempts"
                        )
                        .register(registry);



        this.deadLetterCounter =
                Counter.builder("apex.outbox.deadletter")
                        .description(
                                "Number of events moved to dead letter"
                        )
                        .register(registry);

    }



    public void incrementPublished(){

        publishedCounter.increment();

    }



    public void incrementFailed(){

        failedCounter.increment();

    }



    public void incrementDeadLetter(){

        deadLetterCounter.increment();

    }

}