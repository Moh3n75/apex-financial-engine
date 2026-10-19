
package com.apex.customer.unit;

import com.apex.customer.modules.customer.events.CustomerContracts;
import com.apex.customer.modules.customer.events.CustomerCreatedIntegrationEvent;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@Tag("unit")
class CustomerContractsTest {

    @Test
    void shouldPreserveExistingCustomerCreatedContract() {

        var contract = CustomerContracts.CREATED_V1;

        assertEquals(
                CustomerCreatedIntegrationEvent.EVENT_TYPE,
                contract.eventType()
        );

        assertEquals(
                CustomerCreatedIntegrationEvent.EVENT_VERSION,
                contract.eventVersion()
        );

        assertEquals(
                CustomerCreatedIntegrationEvent.AGGREGATE_TYPE,
                contract.aggregateType()
        );

        assertEquals(
                CustomerCreatedIntegrationEvent.class,
                contract.payloadType()
        );

        assertEquals(
                "customer.created.v1",
                contract.schemaId()
        );
    }
}
