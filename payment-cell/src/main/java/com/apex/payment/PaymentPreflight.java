package com.apex.payment;
import org.springframework.stereotype.Component;
import org.springframework.context.SmartLifecycle;
import com.apex.platform.messaging.consumer.EventHandlerRegistry;
@Component
public class PaymentPreflight implements SmartLifecycle {
    private final EventHandlerRegistry handlers;
    private boolean running;
    public PaymentPreflight(EventHandlerRegistry handlers) { this.handlers=handlers; }
    public void start() {
        if(handlers.find("ORDER_READY_FOR_PAYMENT",1).isEmpty()) { throw new IllegalStateException("Missing payment handler"); }
        running=true;
    }
    public void stop() { running=false; }
    public boolean isRunning() { return running; }
    public int getPhase() { return Integer.MIN_VALUE; }
}
