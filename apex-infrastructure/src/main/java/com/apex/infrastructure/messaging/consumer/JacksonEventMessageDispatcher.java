package com.apex.infrastructure.messaging.consumer;

import com.apex.platform.events.EventEnvelope;
import com.apex.platform.events.EventMetadata;
import com.apex.platform.messaging.consumer.*;

import org.springframework.stereotype.Component;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@Component
public final class JacksonEventMessageDispatcher
        implements EventMessageDispatcher {

    private final JsonMapper jsonMapper;

    private final EventHandlerRegistry handlerRegistry;

    private final EventHandlerExecutor handlerExecutor;

    public JacksonEventMessageDispatcher(
            JsonMapper jsonMapper,
            EventHandlerRegistry handlerRegistry,
            EventHandlerExecutor handlerExecutor
    ) {

        this.jsonMapper = jsonMapper;
        this.handlerRegistry = handlerRegistry;
        this.handlerExecutor = handlerExecutor;
    }

    @Override
    public DispatchResult dispatch(
            String serializedEnvelope,
            ConsumerContext context
    ) {

        try {

            JsonNode root;

            try {

                root =
                        jsonMapper.readTree(
                                serializedEnvelope
                        );

            } catch (Exception exception) {

                throw new MalformedEventException(
                        "Invalid integration event JSON",
                        exception
                );
            }

            JsonNode metadataNode =
                    root.get("metadata");

            JsonNode payloadNode =
                    root.get("payload");

            if (metadataNode == null) {

                throw new MalformedEventException(
                        "Event metadata is missing"
                );
            }

            if (payloadNode == null) {

                throw new MalformedEventException(
                        "Event payload is missing"
                );
            }

            EventMetadata metadata;

            try {

                metadata =
                        jsonMapper.treeToValue(
                                metadataNode,
                                EventMetadata.class
                        );

            } catch (Exception exception) {

                throw new MalformedEventException(
                        "Invalid event metadata",
                        exception
                );
            }

            EventHandler<?> handler =
                    handlerRegistry
                            .find(
                                    metadata.eventType(),
                                    metadata.eventVersion()
                            )
                            .orElse(null);

            if (handler == null) {

                return DispatchResult.IGNORED;
            }

            return dispatchToHandler(
                    handler,
                    metadata,
                    payloadNode,
                    context
            );

        } catch (Exception exception) {

            throw new IllegalStateException(
                    "Failed to dispatch integration event",
                    exception
            );
        }
    }

    @SuppressWarnings("unchecked")
    private <T> DispatchResult dispatchToHandler(
            EventHandler<?> rawHandler,
            EventMetadata metadata,
            JsonNode payloadNode,
            ConsumerContext context
    ) throws Exception {

        EventHandler<T> handler =
                (EventHandler<T>) rawHandler;

        T payload;

        try {

            payload =
                    jsonMapper.treeToValue(
                            payloadNode,
                            handler.payloadType()
                    );

        } catch (Exception exception) {

            throw new MalformedEventException(
                    "Invalid event payload for "
                            + metadata.eventType()
                            + " version "
                            + metadata.eventVersion(),
                    exception
            );
        }

        EventEnvelope<T> envelope =
                new EventEnvelope<>(
                        metadata,
                        payload
                );

        return handlerExecutor.execute(
                handler,
                envelope,
                context
        );
    }
}