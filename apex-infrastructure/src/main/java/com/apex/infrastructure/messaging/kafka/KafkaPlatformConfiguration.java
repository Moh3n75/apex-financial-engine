package com.apex.infrastructure.messaging.kafka;


import com.apex.platform.messaging.kafka.KafkaMessagePublisher;
import com.apex.platform.messaging.kafka.SpringKafkaMessagePublisher;
import org.apache.kafka.clients.admin.NewTopic;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration
public class KafkaPlatformConfiguration {

    public static final String FINANCIAL_EVENTS_TOPIC =
            "apex.financial.events.v1";

    public static final String FINANCIAL_EVENTS_DLT_TOPIC =
            FINANCIAL_EVENTS_TOPIC + "-dlt";

    @Bean
    public KafkaMessagePublisher kafkaMessagePublisher(
            KafkaTemplate<String, String> kafkaTemplate
    ) {
        return new SpringKafkaMessagePublisher(
                kafkaTemplate
        );
    }

    @Bean
    public NewTopic financialEventsTopic() {
        return TopicBuilder
                .name(FINANCIAL_EVENTS_TOPIC)
                .partitions(6)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic financialEventsDltTopic() {

        return TopicBuilder
                .name(
                        FINANCIAL_EVENTS_DLT_TOPIC
                )
                .partitions(6)
                .replicas(1)
                .build();
    }
}
