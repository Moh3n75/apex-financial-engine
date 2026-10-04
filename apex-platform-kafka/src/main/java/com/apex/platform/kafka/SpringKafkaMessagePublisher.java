package com.apex.platform.kafka;

import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletionStage;

public final class SpringKafkaMessagePublisher
        implements KafkaMessagePublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public SpringKafkaMessagePublisher(
            KafkaTemplate<String, String> kafkaTemplate
    ) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public CompletionStage<Void> publish(
            String topic,
            String key,
            String message
    ) {
        return kafkaTemplate
                .send(topic, key, message)
                .thenApply(result -> null);
    }
}
