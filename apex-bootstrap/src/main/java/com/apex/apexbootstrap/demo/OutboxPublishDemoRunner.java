package com.apex.apexbootstrap.demo;



import com.apex.platform.messaging.outbox.OutboxPublisher;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;


@Component
public class OutboxPublishDemoRunner
        implements CommandLineRunner {


    private final OutboxPublisher publisher;


    public OutboxPublishDemoRunner(
            OutboxPublisher publisher
    ) {
        this.publisher = publisher;
    }


    @Override
    public void run(
            String... args
    ) {

        publisher.publishBatch();

    }
}