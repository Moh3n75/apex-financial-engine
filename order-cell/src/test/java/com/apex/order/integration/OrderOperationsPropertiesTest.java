package com.apex.order.integration;

import static org.assertj.core.api.Assertions.*;

import java.time.Duration;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.apex.order.modules.order.operations.OrderOperationsProperties;

@Tag("unit")
class OrderOperationsPropertiesTest {
    @ParameterizedTest
    @ValueSource(strings = {"", "short", "${APEX_ORDER_OPERATIONS_READ_TOKEN}", "                        ", "long-token-containing spaces-invalid"})
    void refusesUnsafeOrUnresolvedToken(String token) {
        assertThatThrownBy(() -> new OrderOperationsProperties(Duration.ofMinutes(5), token))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void requiresPositiveBoundedThresholdAndExactBearerCredential() {
        String token = "isolated-only-read-token-for-unit-test";
        for (Duration invalid : new Duration[]{Duration.ZERO, Duration.ofMillis(-1), Duration.ofDays(366)}) {
            assertThatThrownBy(() -> new OrderOperationsProperties(invalid, token)).isInstanceOf(IllegalArgumentException.class);
        }
        var properties = new OrderOperationsProperties(Duration.ofMinutes(5), token);
        assertThat(properties.authorizes("Bearer " + token)).isTrue();
        assertThat(properties.authorizes(null)).isFalse();
        assertThat(properties.authorizes("Bearer wrong")).isFalse();
        assertThat(properties.authorizes(token)).isFalse();
    }
}
