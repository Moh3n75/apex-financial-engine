
package com.apex.platform.messaging.consumer;

import com.apex.platform.events.api.EventContract;

public interface ContractEventHandler<T>
        extends EventHandler<T> {

    EventContract<T> contract();

    @Override
    default String eventType() {
        return contract().eventType();
    }

    @Override
    default int eventVersion() {
        return contract().eventVersion();
    }

    @Override
    default Class<T> payloadType() {
        return contract().payloadType();
    }
}
