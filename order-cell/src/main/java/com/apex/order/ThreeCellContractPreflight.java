package com.apex.order;
import java.util.List;
import org.springframework.stereotype.Component;
import org.springframework.context.annotation.Profile;
import org.springframework.context.SmartLifecycle;
import com.apex.platform.messaging.consumer.EventHandlerRegistry;
@Component
@Profile("three-cell-saga")
public class ThreeCellContractPreflight implements SmartLifecycle {
    private final EventHandlerRegistry registry;
    private boolean running;
    public ThreeCellContractPreflight(EventHandlerRegistry registry) { this.registry=registry; }
    public void start() {
        for(String type:List.of("INVENTORY_RESERVED","INVENTORY_RESERVATION_REJECTED","INVENTORY_RELEASED","PAYMENT_AUTHORIZED","PAYMENT_DECLINED")) {
            if(registry.find(type,1).isEmpty()) { throw new IllegalStateException("Missing three-cell contract: "+type); }
        }
        running=true;
    }
    public void stop() { running=false; }
    public boolean isRunning() { return running; }
    public int getPhase() { return Integer.MIN_VALUE; }
}
