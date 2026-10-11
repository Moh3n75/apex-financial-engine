package com.apex.infrastructure.config;

import com.apex.platform.messaging.consumer.DefaultEventHandlerRegistry;
import com.apex.platform.messaging.consumer.EventHandler;
import com.apex.platform.messaging.consumer.EventHandlerRegistry;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class ConsumerPlatformConfiguration {

    @Bean
    public EventHandlerRegistry eventHandlerRegistry(
            List<EventHandler<?>> handlers
    ) {

        return new DefaultEventHandlerRegistry(
                handlers
        );
    }
}