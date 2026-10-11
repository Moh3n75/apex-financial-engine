package com.apex.infrastructure.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.apex.platform.messaging.outbox.RetryPolicy;

@Configuration
public class OutboxConfiguration {


    @Bean
    public RetryPolicy retryPolicy(){

        return new RetryPolicy();

    }

}