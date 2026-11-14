package com.apex.platform.messaging.consumer.internal;

import com.apex.platform.events.EventEnvelope;
import com.apex.platform.events.EventMetadata;
import com.apex.platform.messaging.consumer.ConsumerContext;
import com.apex.platform.messaging.consumer.OnCellEvent;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Tag("unit")
class ReflectiveEventHandlerAdapterTest {

    static class SampleHandler {

        String received;

        @OnCellEvent(type = "TEST_EVENT", version = 1)
        public void handle(String payload) {
            received = payload;
        }
    }

    @Test
    void shouldInvokeAnnotatedBusinessMethod() throws Exception {

        var bean = new SampleHandler();

        var method = SampleHandler.class
                .getMethod("handle", String.class);

        var adapter = new ReflectiveEventHandlerAdapter<>(
                bean,
                method,
                "TEST_EVENT",
                1,
                String.class
        );

        @SuppressWarnings("unchecked")
        EventEnvelope<String> envelope =
                mock(EventEnvelope.class);

        when(envelope.payload()).thenReturn("hello");

        adapter.handle(envelope, mock(ConsumerContext.class));

        assertThat(bean.received).isEqualTo("hello");
        assertThat(adapter.eventType()).isEqualTo("TEST_EVENT");
        assertThat(adapter.eventVersion()).isEqualTo(1);
        assertThat(adapter.payloadType()).isEqualTo(String.class);
    }

    @Test
    void shouldRejectInvalidHandlerSignature() throws Exception {

        var bean = new InvalidHandler();

        var method = InvalidHandler.class
                .getMethod("handle", String.class);

        assertThatThrownBy(() ->
                new ReflectiveEventHandlerAdapter<>(
                        bean,
                        method,
                        "TEST_EVENT",
                        1,
                        String.class
                )
        ).isInstanceOf(IllegalArgumentException.class);
    }

    public static class InvalidHandler {

        public String handle(String payload) {
            return payload;
        }
    }

    @Test
    void passesTheOriginalIncomingMetadataInstance() throws Exception {
        MetadataHandler target = new MetadataHandler();
        var adapter = new ReflectiveEventHandlerAdapter<>(target,
                MetadataHandler.class.getMethod("handle", String.class, EventMetadata.class), "META", 1, String.class);
        EventMetadata metadata = new EventMetadata(UUID.randomUUID(), "META", 1, UUID.randomUUID(), null,
                "one", "TEST", "producer", "producer-cell", Instant.EPOCH);
        adapter.handle(new EventEnvelope<>(metadata, "payload"), mock(ConsumerContext.class));
        assertThat(target.metadata).isSameAs(metadata);
        assertThat(target.payload).isEqualTo("payload");
    }

    @ParameterizedTest
    @ValueSource(strings = {"runtime", "error", "checked"})
    void propagatesBusinessFailuresWithoutLosingTheirCause(String kind) throws Exception {
        Throwable failure = switch (kind) {
            case "runtime" -> new IllegalArgumentException("business failure");
            case "error" -> new AssertionError("business failure");
            default -> new IOException("business failure");
        };
        ThrowingHandler target = new ThrowingHandler(failure);
        var adapter = new ReflectiveEventHandlerAdapter<>(target,
                ThrowingHandler.class.getMethod("handle", String.class), "FAIL", 1, String.class);
        @SuppressWarnings("unchecked")
        EventEnvelope<String> event = mock(EventEnvelope.class);
        when(event.payload()).thenReturn("payload");
        Throwable observed = org.assertj.core.api.Assertions.catchThrowable(() -> adapter.handle(event, mock(ConsumerContext.class)));
        if (failure instanceof RuntimeException || failure instanceof Error) {
            assertThat(observed).isSameAs(failure);
        } else {
            assertThat(observed).isInstanceOf(IllegalStateException.class).hasCauseReference(failure)
                    .hasMessageContaining("handle");
        }
    }

    public static class MetadataHandler {
        String payload;
        EventMetadata metadata;
        public void handle(String payload, EventMetadata metadata) {
            this.payload = payload;
            this.metadata = metadata;
        }
    }

    public static class ThrowingHandler {
        private final Throwable failure;
        ThrowingHandler(Throwable failure) { this.failure = failure; }
        public void handle(String payload) throws Exception {
            if (failure instanceof Error error) { throw error; }
            throw (Exception) failure;
        }
    }
}
