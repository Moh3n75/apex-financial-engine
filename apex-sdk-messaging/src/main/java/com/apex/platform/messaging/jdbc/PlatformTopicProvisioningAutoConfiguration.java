package com.apex.platform.messaging.jdbc;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaAdmin;

@AutoConfiguration(after = {KafkaAutoConfiguration.class, PlatformCellMessagingAutoConfiguration.class})
@ConditionalOnProperty(prefix = "apex.messaging.cell", name = "enabled", havingValue = "true")
@ConditionalOnProperty(prefix = "apex.messaging.provisioning", name = "enabled", havingValue = "true")
@EnableConfigurationProperties({TopicProvisioningProperties.class, PublisherRouting.class,
        ConsumerReliabilityProperties.class})
public class PlatformTopicProvisioningAutoConfiguration {
    @Bean
    ApexTopicProvisioner apexTopicProvisioner(KafkaAdmin admin, TopicProvisioningProperties properties,
            PublisherRouting routing, ConsumerReliabilityProperties reliability, Environment environment) {
        return new ApexTopicProvisioner(admin.getConfigurationProperties(), properties, routing, reliability, environment);
    }
}
