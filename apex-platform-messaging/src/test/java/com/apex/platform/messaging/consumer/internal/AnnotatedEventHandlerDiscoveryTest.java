
package com.apex.platform.messaging.consumer.internal;

import com.apex.platform.messaging.consumer.EventHandler;
import com.apex.platform.messaging.consumer.EventHandlerRegistry;
import com.apex.platform.messaging.consumer.DefaultEventHandlerRegistry;
import com.apex.platform.messaging.consumer.OnCellEvent;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
class AnnotatedEventHandlerDiscoveryTest {

    @Test
    void shouldDiscoverAnnotatedSpringHandler() {

        try (var context = new AnnotationConfigApplicationContext()) {

            context.registerBean(
                    "sampleHandler",
                    SampleHandler.class
            );

            context.refresh();

            var discovery = new AnnotatedEventHandlerDiscovery(
                    context.getBeanFactory()
            );

            List<EventHandler<?>> handlers = discovery.discover();

            assertThat(handlers).hasSize(1);

            EventHandlerRegistry registry =
                    new DefaultEventHandlerRegistry(handlers);

            assertThat(registry.find("SAMPLE_CREATED", 1))
                    .isPresent();

            assertThat(registry.find("OTHER_EVENT", 1))
                    .isEmpty();
        }
    }

    @Component
    public static class SampleHandler {

        @OnCellEvent(type = "SAMPLE_CREATED", version = 1)
        public void handle(SamplePayload payload) {
        }
    }

    public record SamplePayload(String id) {
    }
}
