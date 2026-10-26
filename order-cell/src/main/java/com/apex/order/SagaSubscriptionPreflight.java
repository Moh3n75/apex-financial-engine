package com.apex.order;

import java.util.List;
import com.apex.platform.messaging.consumer.EventHandlerRegistry;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/** Validate this Cell's required business contracts before Kafka containers start. */
@Component
@Profile("saga")
public class SagaSubscriptionPreflight implements SmartLifecycle {
    private final EventHandlerRegistry registry;
    private boolean running;

    public SagaSubscriptionPreflight(EventHandlerRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void start() {
        for (String type : List.of("INVENTORY_RESERVED", "INVENTORY_RESERVATION_REJECTED")) {
            if (registry.find(type, 1).isEmpty()) {
                throw new IllegalStateException("Missing required Saga handler: " + type + " V1");
            }
            org.slf4j.LoggerFactory.getLogger(SagaSubscriptionPreflight.class)
                    .info("Saga preflight validated {} V1", type);
        }
        running = true;
    }

    @Override
    public int getPhase() {
        return Integer.MIN_VALUE;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public void stop() {
        running = false;
    }
}
