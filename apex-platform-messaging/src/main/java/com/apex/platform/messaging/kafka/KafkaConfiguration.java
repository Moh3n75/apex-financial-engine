package com.apex.platform.messaging.kafka;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;


@Configuration
public class KafkaConfiguration {


    @Bean
    public KafkaMessagePublisher kafkaMessagePublisher(
            KafkaTemplate<String, String> kafkaTemplate
    ) {

        return new SpringKafkaMessagePublisher(
                kafkaTemplate
        );
    }

}
