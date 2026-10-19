
package com.apex.platform.events.api;

import com.apex.platform.events.IntegrationEvent;

import java.util.UUID;

/**
 * Business-facing API for recording integration events.
 *
 * Events must be appended within the active business
 * transaction. Delivery is handled asynchronously
 * by the APEX messaging runtime.
 */
public interface CellEvents {

    <T extends IntegrationEvent> void append(
            EventContract<T> contract,
            UUID aggregateId,
            T payload
    );
}
