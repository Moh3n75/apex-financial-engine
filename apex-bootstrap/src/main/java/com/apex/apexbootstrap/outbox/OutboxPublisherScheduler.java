package com.apex.apexbootstrap.outbox;

import com.apex.platform.messaging.outbox.OutboxPublisher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class OutboxPublisherScheduler {


    private final OutboxPublisher publisher;

    public OutboxPublisherScheduler(OutboxPublisher publisher) {
        this.publisher = publisher;
    }


    @Scheduled(
            fixedDelayString = "${outbox.publisher.interval:1000}"
    )
    public void publish(){

        publisher.publishBatch();

    }

}