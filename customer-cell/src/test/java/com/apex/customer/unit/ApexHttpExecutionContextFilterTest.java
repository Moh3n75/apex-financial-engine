
package com.apex.customer.unit;

import com.apex.platform.webmvc.ApexHttpExecutionContextFilter;
import com.apex.platform.core.context.ExecutionContext;
import com.apex.platform.core.context.ThreadLocalExecutionContextProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Tag("unit")
class ApexHttpExecutionContextFilterTest {

    private final ThreadLocalExecutionContextProvider provider =
            new ThreadLocalExecutionContextProvider();

    private final ApexHttpExecutionContextFilter filter =
            new ApexHttpExecutionContextFilter(
                    provider,
                    "customer-cell",
                    "customer"
            );

    @Test
    void shouldProvideContextDuringRequestAndCleanAfterwards()
            throws Exception {

        AtomicReference<ExecutionContext> captured =
                new AtomicReference<>();

        filter.doFilter(
                new MockHttpServletRequest(),
                new MockHttpServletResponse(),
                (request, response) -> {
                    captured.set(provider.requireCurrent());
                }
        );

        assertNotNull(captured.get());
        assertNotNull(captured.get().correlationId());
        assertNull(captured.get().causationId());
        assertEquals("customer-cell", captured.get().sourceService());
        assertEquals("customer", captured.get().cellId());

        assertThrows(
                IllegalStateException.class,
                provider::requireCurrent
        );
    }

    @Test
    void shouldCleanContextWhenRequestFails() {
        assertThrows(
                IllegalStateException.class,
                () -> filter.doFilter(
                        new MockHttpServletRequest(),
                        new MockHttpServletResponse(),
                        (request, response) -> {
                            assertNotNull(provider.requireCurrent());
                            throw new IllegalStateException("failed");
                        }
                )
        );

        assertThrows(
                IllegalStateException.class,
                provider::requireCurrent
        );
    }
}
