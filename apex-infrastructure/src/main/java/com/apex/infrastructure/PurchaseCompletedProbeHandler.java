package com.apex.infrastructure;

import com.apex.credit.application.purchase.event.PurchaseCompletedIntegrationEvent;
import com.apex.credit.application.purchase.event.PurchaseEvents;
import com.apex.platform.events.EventEnvelope;
import com.apex.platform.messaging.consumer.ConsumerContext;
import com.apex.platform.messaging.consumer.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class PurchaseCompletedProbeHandler
        implements EventHandler<PurchaseCompletedIntegrationEvent> {

    private static final Logger log =
            LoggerFactory.getLogger(
                    PurchaseCompletedProbeHandler.class
            );

    @Override
    public String eventType() {
        return PurchaseEvents.PURCHASE_COMPLETED;
    }

    @Override
    public int eventVersion() {
        return PurchaseEvents.PURCHASE_COMPLETED_VERSION;
    }

    @Override
    public Class<PurchaseCompletedIntegrationEvent> payloadType() {
        return PurchaseCompletedIntegrationEvent.class;
    }

    @Override
    public void handle(
            EventEnvelope<PurchaseCompletedIntegrationEvent> event,
            ConsumerContext context
    ) {



        log.info(
                "PurchaseCompleted consumed eventId={} transactionId={}",
                event.metadata().eventId(),
                event.payload().transactionId()
        );
    }
}