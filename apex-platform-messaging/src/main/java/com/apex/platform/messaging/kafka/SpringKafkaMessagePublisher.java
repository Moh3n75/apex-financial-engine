package com.apex.platform.messaging.kafka;

import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletionStage;

public final class SpringKafkaMessagePublisher
        implements KafkaMessagePublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final com.apex.platform.messaging.observation.MessagingObservation observations;

    public SpringKafkaMessagePublisher(
            KafkaTemplate<String, String> kafkaTemplate
    ) {
        this(kafkaTemplate,com.apex.platform.messaging.observation.MessagingObservation.NONE);
    }
    public SpringKafkaMessagePublisher(KafkaTemplate<String,String> kafkaTemplate,
            com.apex.platform.messaging.observation.MessagingObservation observations) {
        this.kafkaTemplate = kafkaTemplate;
        this.observations = observations;
    }

    @Override
    public CompletionStage<Void> publish(
            String topic,
            String key,
            String message
    ) {
        var attempt=observations.start("outbox.ack");
        try {
            return kafkaTemplate.send(topic,key,message).whenComplete((receipt,failure)->
                attempt.finish(failure==null?"OBSERVED":"UNKNOWN")).thenApply(result->null);
        } catch(RuntimeException failure){attempt.finish("UNKNOWN");throw failure;}
    }
}
