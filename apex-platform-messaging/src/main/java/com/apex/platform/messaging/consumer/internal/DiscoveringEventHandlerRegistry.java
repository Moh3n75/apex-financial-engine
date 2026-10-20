package com.apex.platform.messaging.consumer.internal;

import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;

import com.apex.platform.messaging.consumer.DefaultEventHandlerRegistry;
import com.apex.platform.messaging.consumer.EventHandler;
import com.apex.platform.messaging.consumer.EventHandlerRegistry;

/** Builds the existing immutable registry after handler construction and AOP post-processing. */
public final class DiscoveringEventHandlerRegistry implements EventHandlerRegistry, SmartInitializingSingleton {
    private final ConfigurableListableBeanFactory beanFactory;
    private volatile EventHandlerRegistry delegate;

    public DiscoveringEventHandlerRegistry(ConfigurableListableBeanFactory beanFactory) {
        this.beanFactory = Objects.requireNonNull(beanFactory);
    }

    @Override
    public void afterSingletonsInstantiated() {
        var handlers = new ArrayList<EventHandler<?>>();
        beanFactory.getBeansOfType(EventHandler.class).values().forEach(handlers::add);
        handlers.addAll(new AnnotatedEventHandlerDiscovery(beanFactory).discover());
        try {
            delegate = new DefaultEventHandlerRegistry(handlers);
        } catch (IllegalStateException failure) {
            String descriptions = handlers.stream().map(handler ->
                    handler instanceof ReflectiveEventHandlerAdapter<?> adapter
                            ? adapter.description() : handler.getClass().getName()).collect(Collectors.joining(", "));
            throw new IllegalStateException(failure.getMessage() + "; registrations: " + descriptions, failure);
        }
    }

    @Override
    public Optional<EventHandler<?>> find(String eventType, int eventVersion) {
        EventHandlerRegistry initialized = delegate;
        if (initialized == null) {
            throw new IllegalStateException("Event handler registry is not initialized; dispatch events after singleton initialization");
        }
        return initialized.find(eventType, eventVersion);
    }
}
