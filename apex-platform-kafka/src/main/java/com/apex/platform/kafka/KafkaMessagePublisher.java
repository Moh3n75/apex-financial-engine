package com.apex.platform.kafka;

import java.util.concurrent.CompletionStage;

public interface KafkaMessagePublisher {

    CompletionStage<Void> publish(
            String topic,
            String key,
            String message
    );
}
