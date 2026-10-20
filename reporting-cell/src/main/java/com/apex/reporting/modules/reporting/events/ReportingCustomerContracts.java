
package com.apex.reporting.modules.reporting.events;

import com.apex.platform.events.api.EventContract;
import com.apex.platform.events.api.EventContracts;

public final class ReportingCustomerContracts {

    private ReportingCustomerContracts() {
    }

    public static final EventContract<CustomerCreatedV1Payload>
            CREATED_V1 = EventContracts.define(
            CustomerCreatedV1Payload.EVENT_TYPE,
            CustomerCreatedV1Payload.EVENT_VERSION,
            "CUSTOMER",
            CustomerCreatedV1Payload.class,
            "customer.created.v1"
    );
}
