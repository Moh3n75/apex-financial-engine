package com.apex.platform.messaging.integration;

import com.apex.platform.messaging.jdbc.*;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/** The two M38 plans, with creation acknowledged before metadata converges. */
@Tag("unit")
class TopicMetadataRegressionTest {
    @Test void outgoingM38PlanWaitsForLeaderAfterSuccessfulCreate() {
        reproduce(false);
    }

    @Test void incomingM38PlanWaitsForCustomDltVisibilityAfterSuccessfulCreate() {
        reproduce(true);
    }

    private void reproduce(boolean incoming) {
        Admin admin = mock(Admin.class);
        var environment = new MockEnvironment()
                .withProperty("apex.messaging.outbox.publisher.enabled", "" + !incoming)
                .withProperty("apex.messaging.publisher.topic", "legacy")
                .withProperty("apex.messaging.consumer.enabled", "" + incoming)
                .withProperty("apex.messaging.consumer.topics", "source");
        var provisioner = new ApexTopicProvisioner(Map.of(),
                new TopicProvisioningProperties(true, 1, (short) -1, Duration.ofSeconds(1)),
                new PublisherRouting(!incoming, incoming ? List.of() : List.of(
                        new PublisherRouting.Route("READY", 1, "routed"))),
                new ConsumerReliabilityProperties(incoming, 3, Duration.ofSeconds(1), Duration.ofSeconds(10),
                        ".quarantine", true, Duration.ofSeconds(10)), environment);
        var calls = new java.util.HashMap<String, Integer>();
        when(admin.describeTopics(anyCollection(), any(DescribeTopicsOptions.class))).thenAnswer(invocation -> {
            String name = ((java.util.Collection<String>) invocation.getArgument(0)).iterator().next();
            int attempt = calls.merge(name, 1, Integer::sum);
            var result = mock(DescribeTopicsResult.class);
            var future = new org.apache.kafka.common.internals.KafkaFutureImpl<Map<String, TopicDescription>>();
            if (attempt == 1 || (incoming && name.endsWith(".quarantine") && attempt == 2)) {
                future.completeExceptionally(new UnknownTopicOrPartitionException("not visible yet"));
            } else {
                var node = new Node(0, "localhost", 9092);
                var leader = !incoming && attempt == 2 ? Node.noNode() : node;
                future.complete(Map.of(name, new TopicDescription(name, false,
                        List.of(new TopicPartitionInfo(0, leader, List.of(node), List.of(node))))));
            }
            when(result.allTopicNames()).thenReturn(future);
            return result;
        });
        var created = mock(CreateTopicsResult.class);
        when(created.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.createTopics(anyCollection(), any(CreateTopicsOptions.class))).thenReturn(created);
        try (var factory = mockStatic(Admin.class)) {
            factory.when(() -> Admin.create(Map.<String, Object>of())).thenReturn(admin);
            assertThatCode(provisioner::afterSingletonsInstantiated).doesNotThrowAnyException();
            assertThat(calls.values()).allMatch(count -> count >= 2);
        }
    }
}
