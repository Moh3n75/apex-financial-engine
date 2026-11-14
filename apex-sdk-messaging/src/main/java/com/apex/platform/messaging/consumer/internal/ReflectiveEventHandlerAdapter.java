
package com.apex.platform.messaging.consumer.internal;

import com.apex.platform.events.EventEnvelope;
import com.apex.platform.events.EventMetadata;
import com.apex.platform.messaging.consumer.ConsumerContext;
import com.apex.platform.messaging.consumer.EventHandler;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Objects;

import org.springframework.aop.support.AopUtils;

public final class ReflectiveEventHandlerAdapter<T>
        implements EventHandler<T> {

    private final Object bean;
    private final Method method;

    private final String eventType;
    private final int eventVersion;
    private final Class<T> payloadType;
    private final boolean metadataParameter;
    private final String description;

    public ReflectiveEventHandlerAdapter(
            Object bean,
            Method method,
            String eventType,
            int eventVersion,
            Class<T> payloadType
    ) {
        this(bean, method, eventType, eventVersion, payloadType, bean.getClass().getName());
    }

    ReflectiveEventHandlerAdapter(Object bean, Method method, String eventType, int eventVersion,
            Class<T> payloadType, String beanName) {
        this.bean = Objects.requireNonNull(bean);
        this.method = Objects.requireNonNull(method);
        this.eventType = Objects.requireNonNull(eventType);
        this.payloadType = Objects.requireNonNull(payloadType);
        this.description = "bean '" + beanName + "' method " + method.toGenericString();

        if (!Modifier.isPublic(method.getModifiers()) || Modifier.isStatic(method.getModifiers())) {
            throw new IllegalArgumentException("Event handler must be a public instance method: " + description);
        }
        if (AopUtils.isCglibProxy(bean) && Modifier.isFinal(method.getModifiers())) {
            throw new IllegalArgumentException("A final handler method cannot be advised through a CGLIB proxy: " + description);
        }

        if (eventType.isBlank() || eventVersion <= 0) {
            throw new IllegalArgumentException(
                    "Invalid event type or version: " + description
            );
        }

        if (!void.class.equals(method.getReturnType())) {
            throw new IllegalArgumentException(
                    "Event handler must return void: " + method
            );
        }

        int count = method.getParameterCount();

        if (count != 1 && count != 2) {
            throw new IllegalArgumentException(
                    "Event handler must accept payload " +
                            "and optional EventMetadata: " + method
            );
        }

        if (!method.getParameterTypes()[0].equals(payloadType)) {
            throw new IllegalArgumentException(
                    "Handler payload type mismatch: " + method
            );
        }

        if (count == 2
                && method.getParameterTypes()[1] != EventMetadata.class) {
            throw new IllegalArgumentException(
                    "Second handler parameter must be EventMetadata: " + description
            );
        }

        if (!(method.getGenericParameterTypes()[0] instanceof Class<?>) || !supportedPayload(payloadType)) {
            throw new IllegalArgumentException("Handler payload must have a concrete, non-generic deserialization type: " + description);
        }
        if (!method.canAccess(bean) && !method.trySetAccessible()) {
            throw new IllegalArgumentException("Cannot access event handler: " + description);
        }

        this.metadataParameter = count == 2;
        this.eventVersion = eventVersion;
    }

    private static boolean supportedPayload(Class<?> type) {
        if (type.isArray()) {
            return type.getComponentType().isPrimitive() || supportedPayload(type.getComponentType());
        }
        return type != Object.class && !type.isPrimitive() && !type.isInterface()
                && !Modifier.isAbstract(type.getModifiers()) && type.getTypeParameters().length == 0
                && (!type.isMemberClass() || Modifier.isStatic(type.getModifiers()));
    }

    String description() {
        return description;
    }

    @Override
    public String eventType() {
        return eventType;
    }

    @Override
    public int eventVersion() {
        return eventVersion;
    }

    @Override
    public Class<T> payloadType() {
        return payloadType;
    }

    @Override
    public void handle(
            EventEnvelope<T> event,
            ConsumerContext context
    ) {
        try {
            if (metadataParameter) {
                method.invoke(
                        bean,
                        event.payload(),
                        event.metadata()
                );
            } else {
                method.invoke(bean, event.payload());
            }

        } catch (InvocationTargetException exception) {
            Throwable cause = exception.getCause();

            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }

            if (cause instanceof Error error) {
                throw error;
            }

            throw new IllegalStateException(
                    "Event handler invocation failed: " + description,
                    cause
            );

        } catch (IllegalAccessException exception) {
            throw new IllegalStateException(
                    "Cannot invoke event handler: " + description,
                    exception
            );
        }
    }
}
