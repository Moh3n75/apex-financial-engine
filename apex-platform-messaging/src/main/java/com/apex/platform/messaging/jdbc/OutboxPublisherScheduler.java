package com.apex.platform.messaging.jdbc;

import org.springframework.scheduling.annotation.Scheduled;

import com.apex.platform.messaging.outbox.OutboxPublisher;

public class OutboxPublisherScheduler {

    private final OutboxPublisher publisher;

    public OutboxPublisherScheduler(OutboxPublisher publisher) {
        this.publisher = publisher;
    }

    @Scheduled(fixedDelayString = "${apex.messaging.outbox.publisher.interval-ms:1000}")
    public void publish() {
        publisher.publishBatch();
    }
}
