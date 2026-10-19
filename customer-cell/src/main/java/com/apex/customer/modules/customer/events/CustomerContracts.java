
package com.apex.customer.modules.customer.events;

import com.apex.platform.events.api.EventContract;
import com.apex.platform.events.api.EventContracts;

public final class CustomerContracts {

    private CustomerContracts() {
    }

    public static final EventContract<CustomerCreatedIntegrationEvent>
            CREATED_V1 = EventContracts.define(
            CustomerCreatedIntegrationEvent.EVENT_TYPE,
            CustomerCreatedIntegrationEvent.EVENT_VERSION,
            CustomerCreatedIntegrationEvent.AGGREGATE_TYPE,
            CustomerCreatedIntegrationEvent.class,
            "customer.created.v1"
    );
}
