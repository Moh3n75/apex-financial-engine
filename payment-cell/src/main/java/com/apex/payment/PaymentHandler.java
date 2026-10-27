package com.apex.payment;
import java.util.UUID;
import com.apex.platform.events.IntegrationEvent;
import com.apex.platform.events.EventMetadata;
import com.apex.platform.events.api.*;
import com.apex.platform.messaging.consumer.OnCellEvent;
import org.springframework.stereotype.Component;
@Component
public class PaymentHandler {
    public record Request(UUID orderId) { public Request { java.util.Objects.requireNonNull(orderId); } }
    public record Outcome(UUID orderId) implements IntegrationEvent { }
    private static final EventContract<Outcome> AUTHORIZED=EventContracts.define("PAYMENT_AUTHORIZED",1,"PAYMENT_AUTHORIZATION",Outcome.class,"payment.authorized.v1");
    private static final EventContract<Outcome> DECLINED=EventContracts.define("PAYMENT_DECLINED",1,"PAYMENT_AUTHORIZATION",Outcome.class,"payment.declined.v1");
    private final AuthorizePaymentUseCase authorize;
    private final CellEvents events;
    public PaymentHandler(AuthorizePaymentUseCase authorize,CellEvents events) { this.authorize=authorize; this.events=events; }
    @OnCellEvent(type="ORDER_READY_FOR_PAYMENT",version=1)
    public void handle(Request request,EventMetadata metadata) {
        if(!"ORDER".equals(metadata.aggregateType()) || !request.orderId().toString().equals(metadata.aggregateId())) {
            throw new IllegalArgumentException("Payment request aggregate mismatch");
        }
        var decision=authorize.execute(request.orderId());
        if(decision.first()) { events.append("AUTHORIZED".equals(decision.status())?AUTHORIZED:DECLINED,
                request.orderId(),new Outcome(request.orderId())); }
    }
}
