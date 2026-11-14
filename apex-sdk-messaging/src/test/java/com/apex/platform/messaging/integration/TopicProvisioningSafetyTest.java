package com.apex.platform.messaging.integration;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import com.apex.platform.messaging.jdbc.*;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

@Tag("unit")
class TopicProvisioningSafetyTest {
    @Test void authorizationFailureRedactsKafkaExceptionAndNamesAffectedTopic() {
        var environment = new MockEnvironment().withProperty("apex.messaging.outbox.publisher.enabled", "false")
                .withProperty("apex.messaging.consumer.enabled", "true")
                .withProperty("apex.messaging.consumer.topics", "authorized.source");
        var provisioner = new ApexTopicProvisioner(Map.of(),
                new TopicProvisioningProperties(true, 1, (short) -1, Duration.ofSeconds(1)),
                new PublisherRouting(false, List.of()),
                new ConsumerReliabilityProperties(true, 3, Duration.ofSeconds(1), Duration.ofSeconds(10),
                        "-dlt", true, Duration.ofSeconds(10)), environment);
        Admin admin = mock(Admin.class);
        DescribeTopicsResult result = mock(DescribeTopicsResult.class);
        when(admin.describeTopics(eq(List.of("authorized.source")), any(org.apache.kafka.clients.admin.DescribeTopicsOptions.class))).thenReturn(result);
        var denied = new KafkaFutureImpl<Map<String, org.apache.kafka.clients.admin.TopicDescription>>();
        denied.completeExceptionally(new TopicAuthorizationException("private-credential-and-connection-details"));
        when(result.allTopicNames()).thenReturn(denied);
        try (var factory = mockStatic(Admin.class)) {
            factory.when(() -> Admin.create(Map.<String, Object>of())).thenReturn(admin);
            new org.springframework.boot.test.context.runner.ApplicationContextRunner()
                    .withBean(ApexTopicProvisioner.class, () -> provisioner).run(context -> {
                        assertThat(context).hasFailed();
                        assertThat(context.getStartupFailure()).hasMessageContaining("authorized.source")
                                .hasMessageContaining("TopicAuthorizationException")
                                .hasMessageNotContaining("private-credential").hasNoCause();
                    });
            verify(admin, never()).createTopics(anyCollection(), any(org.apache.kafka.clients.admin.CreateTopicsOptions.class));
        }
    }

    @Test void invalidProvisioningPoliciesFailBindingValidation() {
        assertThatThrownBy(() -> new TopicProvisioningProperties(true, 0, (short) -1, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TopicProvisioningProperties(true, 1, (short) 0, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TopicProvisioningProperties(true, 1, (short) -1, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
