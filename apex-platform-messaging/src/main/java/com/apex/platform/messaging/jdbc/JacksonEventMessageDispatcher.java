package com.apex.platform.messaging.jdbc;

import com.apex.platform.events.EventEnvelope;
import com.apex.platform.events.EventMetadata;
import com.apex.platform.messaging.consumer.ConsumerContext;
import com.apex.platform.messaging.consumer.DispatchResult;
import com.apex.platform.messaging.consumer.EventHandler;
import com.apex.platform.messaging.consumer.EventHandlerExecutor;
import com.apex.platform.messaging.consumer.EventHandlerRegistry;
import com.apex.platform.messaging.consumer.EventMessageDispatcher;
import com.apex.platform.messaging.consumer.MalformedEventException;
import com.apex.platform.messaging.consumer.UnsupportedEventContractException;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

public final class JacksonEventMessageDispatcher implements EventMessageDispatcher {

    private final JsonMapper jsonMapper;
    private final EventHandlerRegistry handlerRegistry;
    private final EventHandlerExecutor handlerExecutor;
    private final boolean reliable;

    public JacksonEventMessageDispatcher(
            JsonMapper jsonMapper,
            EventHandlerRegistry handlerRegistry,
            EventHandlerExecutor handlerExecutor
    ) {
        this(jsonMapper, handlerRegistry, handlerExecutor, false);
    }

    public JacksonEventMessageDispatcher(JsonMapper jsonMapper, EventHandlerRegistry handlerRegistry,
            EventHandlerExecutor handlerExecutor, boolean reliable) {
        this.jsonMapper = jsonMapper;
        this.handlerRegistry = handlerRegistry;
        this.handlerExecutor = handlerExecutor;
        this.reliable = reliable;
    }

    @Override
    public DispatchResult dispatch(String serializedEnvelope, ConsumerContext context) {
        JsonNode root;
        try {
            root = jsonMapper.readTree(serializedEnvelope);
        } catch (Exception exception) {
            throw new MalformedEventException("Invalid integration event JSON", exception);
        }

        if (root == null || !root.isObject()) {
            throw new MalformedEventException("Integration event must be a JSON object");
        }
        JsonNode metadataNode = root.get("metadata");
        JsonNode payloadNode = root.get("payload");
        if (metadataNode == null || metadataNode.isNull()) {
            throw new MalformedEventException("Event metadata is missing");
        }
        if (payloadNode == null || payloadNode.isNull()) {
            throw new MalformedEventException("Event payload is missing");
        }

        EventMetadata metadata;
        try {
            metadata = jsonMapper.treeToValue(metadataNode, EventMetadata.class);
        } catch (Exception exception) {
            throw new MalformedEventException("Invalid event metadata", exception);
        }

        EventHandler<?> handler = handlerRegistry
                .find(metadata.eventType(), metadata.eventVersion())
                .orElse(null);

        if (handler == null) {
            if (reliable) {
                throw new UnsupportedEventContractException(metadata.eventType(), metadata.eventVersion(), context);
            }
            return DispatchResult.IGNORED;
        }

        return invoke(handler, metadata, payloadNode, context);
    }

    @SuppressWarnings("unchecked")
    private <T> DispatchResult invoke(
            EventHandler<?> rawHandler,
            EventMetadata metadata,
            JsonNode payloadNode,
            ConsumerContext context
    ) {
        EventHandler<T> handler = (EventHandler<T>) rawHandler;
        T payload;
        try {
            payload = jsonMapper.treeToValue(payloadNode, handler.payloadType());
        } catch (Exception exception) {
            throw new MalformedEventException(
                    "Invalid payload for handler type " + handler.payloadType().getName(),
                    exception
            );
        }

        EventEnvelope<T> envelope = new EventEnvelope<>(metadata, payload);
        return handlerExecutor.execute(handler, envelope, context);
    }
}
