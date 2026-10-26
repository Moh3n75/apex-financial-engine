package com.apex.platform.messaging.jdbc;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("apex.messaging.consumer.reliability")
public record ConsumerReliabilityProperties(
        @DefaultValue("false") boolean enabled,
        @DefaultValue("3") int maxAttempts,
        @DefaultValue("1s") Duration retryDelay,
        @DefaultValue("10s") Duration sendTimeout,
        @DefaultValue("-dlt") String dltSuffix,
        @DefaultValue("true") boolean preflightEnabled,
        @DefaultValue("10s") Duration preflightTimeout) {

    public ConsumerReliabilityProperties {
        if (maxAttempts < 1 || maxAttempts > 100) {
            throw new IllegalArgumentException("Consumer reliability max-attempts must be between 1 and 100 (including initial delivery)");
        }
        bounded(retryDelay, "retry-delay", Duration.ofMinutes(1));
        bounded(sendTimeout, "send-timeout", Duration.ofMinutes(1));
        bounded(preflightTimeout, "preflight-timeout", Duration.ofMinutes(1));
        if (dltSuffix == null || !dltSuffix.matches("[a-zA-Z0-9._-]{1,64}")) {
            throw new IllegalArgumentException("Consumer reliability dlt-suffix must be a nonempty Kafka topic suffix");
        }
    }

    private static void bounded(Duration value, String name, Duration maximum) {
        if (value == null || value.toMillis() < 1 || value.compareTo(maximum) > 0) {
            throw new IllegalArgumentException("Consumer reliability " + name + " must be between 1ms and " + maximum);
        }
    }
}
