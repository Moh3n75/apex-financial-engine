package com.apex.infrastructure.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;

@Configuration
public class OutboxPublisherConfiguration {


    @Bean("outboxPublisherInstanceId")
    public String publisherInstanceId() {

        return UUID.randomUUID()
                .toString();

    }

}