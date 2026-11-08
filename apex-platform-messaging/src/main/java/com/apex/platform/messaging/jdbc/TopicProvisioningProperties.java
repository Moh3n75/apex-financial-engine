package com.apex.platform.messaging.jdbc;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("apex.messaging.provisioning")
public record TopicProvisioningProperties(@DefaultValue("false") boolean enabled,
        @DefaultValue("1") int partitions, @DefaultValue("-1") short replicationFactor,
        @DefaultValue("10s") Duration timeout) {
    public TopicProvisioningProperties {
        if (partitions < 1 || (replicationFactor != -1 && replicationFactor < 1)
                || timeout == null || timeout.toMillis() < 1 || timeout.compareTo(Duration.ofMinutes(1)) > 0) {
            throw new IllegalArgumentException("APEX provisioning requires partitions >= 1, replication-factor = -1 "
                    + "(broker default) or >= 1, and timeout between 1ms and 1m");
        }
    }
}
