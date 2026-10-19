
package com.apex.apexbootstrap.context;

import com.apex.platform.core.context.ExecutionContext;
import com.apex.platform.core.context.ExecutionContextProvider;
import com.apex.platform.core.context.ThreadLocalExecutionContextProvider;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ThreadLocalExecutionContextProviderTest {

    private final ExecutionContextProvider provider =
            new ThreadLocalExecutionContextProvider();

    @Test
    void shouldRejectMissingContext() {
        assertThrows(
                IllegalStateException.class,
                provider::requireCurrent
        );
    }

    @Test
    void shouldActivateAndCleanUpContext() {
        ExecutionContext root =
                ExecutionContext.root("customer", "customer");

        try (var scope = provider.open(root)) {
            assertEquals(root, provider.requireCurrent());
        }

        assertThrows(
                IllegalStateException.class,
                provider::requireCurrent
        );
    }

    @Test
    void shouldRestoreParentContextAfterNestedScope() {
        ExecutionContext root =
                ExecutionContext.root("customer", "customer");

        ExecutionContext child = new ExecutionContext(
                root.correlationId(),
                java.util.UUID.randomUUID(),
                "reporting",
                "reporting"
        );

        try (var outer = provider.open(root)) {
            try (var inner = provider.open(child)) {
                assertEquals(child, provider.requireCurrent());
            }

            assertEquals(root, provider.requireCurrent());
        }

        assertThrows(
                IllegalStateException.class,
                provider::requireCurrent
        );
    }
}
