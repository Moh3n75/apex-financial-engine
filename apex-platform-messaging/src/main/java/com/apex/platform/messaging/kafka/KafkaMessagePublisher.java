package com.apex.platform.messaging.kafka;

import java.util.concurrent.CompletionStage;

public interface KafkaMessagePublisher {

    CompletionStage<Void> publish(
            String topic,
            String key,
            String message
    );
}
