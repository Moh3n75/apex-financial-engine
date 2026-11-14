package com.apex.platform.messaging.integration;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import com.apex.platform.context.autoconfigure.ApexExecutionContextAutoConfiguration;
import com.apex.platform.messaging.jdbc.*;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TopicProvisioningIntegrationTest {
    private EmbeddedKafkaKraftBroker broker;
    private Admin admin;

    @BeforeAll void startBroker() {
        broker = new EmbeddedKafkaKraftBroker(1, 1);
        broker.brokerProperty("auto.create.topics.enable", "false");
        broker.brokerListProperty("apex.provisioning.test.brokers");
        broker.afterPropertiesSet();
        admin = Admin.create(Map.of("bootstrap.servers", broker.getBrokersAsString()));
    }

    @AfterAll void stopBroker() {
        if (admin != null) { admin.close(Duration.ofSeconds(3)); }
        if (broker != null) { broker.destroy(); }
    }

    private ApplicationContextRunner runner(String source) {
        return new ApplicationContextRunner().withConfiguration(AutoConfigurations.of(
                PropertyPlaceholderAutoConfiguration.class, ApexExecutionContextAutoConfiguration.class,
                KafkaAutoConfiguration.class, PlatformCellMessagingAutoConfiguration.class,
                PlatformConsumerReliabilityAutoConfiguration.class, PlatformTopicProvisioningAutoConfiguration.class))
                .withBean(JdbcTemplate.class, () -> mock(JdbcTemplate.class))
                .withPropertyValues("spring.application.name=provisioning-test", "apex.cell.id=test",
                        "apex.messaging.cell.enabled=true", "apex.messaging.provisioning.enabled=true",
                        "apex.messaging.outbox.publisher.enabled=false", "apex.messaging.consumer.enabled=true",
                        "apex.messaging.consumer.reliability.enabled=true", "apex.messaging.consumer.topics=" + source,
                        "spring.kafka.bootstrap-servers=" + broker.getBrokersAsString(),
                        "spring.kafka.admin.auto-create=false", "spring.kafka.consumer.properties.allow.auto.create.topics=false",
                        "spring.kafka.consumer.enable-auto-commit=false", "spring.kafka.consumer.group-id=provisioning",
                        "spring.kafka.listener.ack-mode=manual_immediate", "spring.kafka.listener.auto-startup=false");
    }

    private String topic() { return "provision-" + UUID.randomUUID(); }
    private TopicDescription describe(String topic) throws Exception {
        // Setup creates topics independently of the SDK too. Create acknowledgement is
        // not a metadata barrier; wait for the before-snapshot without masking context failures.
        return org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(10))
                .pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(50))
                .ignoreExceptionsMatching(failure -> {
                    Throwable reason = failure;
                    while (reason instanceof java.util.concurrent.ExecutionException && reason.getCause() != null) {
                        reason = reason.getCause();
                    }
                    return reason instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException
                            || reason instanceof org.apache.kafka.common.errors.LeaderNotAvailableException
                            || reason instanceof org.apache.kafka.common.errors.NotLeaderOrFollowerException;
                }).until(() -> admin.describeTopics(List.of(topic)).allTopicNames()
                        .get(1, TimeUnit.SECONDS).get(topic),
                        description -> description != null && !description.partitions().isEmpty()
                                && description.partitions().stream().allMatch(partition -> partition.leader() != null
                                        && partition.leader().id() >= 0 && !partition.replicas().isEmpty()));
    }

    @Test void provisionsIncomingAndCustomDltBeforeUnchangedReliabilityPreflight() {
        String source = topic();
        runner(source).withPropertyValues("apex.messaging.provisioning.partitions=3",
                "apex.messaging.consumer.reliability.dlt-suffix=.quarantine").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(ReliableKafkaConsumerRuntime.class).isRunning()).isTrue();
            assertThat(describe(source).partitions()).hasSize(3);
            assertThat(describe(source + ".quarantine").partitions()).hasSize(3);
        });
    }

    @Test void provisionsLegacyOutgoingAndValidatedContractRoutesOnly() {
        String legacy = topic(), routed = topic();
        runner(topic()).withPropertyValues("apex.messaging.consumer.enabled=false",
                "apex.messaging.outbox.publisher.enabled=true", "apex.messaging.publisher.topic=" + legacy,
                "apex.messaging.publisher.routing.enabled=true",
                "apex.messaging.publisher.routing.required-contracts[0]=READY:1",
                "apex.messaging.publisher.routing.routes[0].event-type=READY",
                "apex.messaging.publisher.routing.routes[0].event-version=1",
                "apex.messaging.publisher.routing.routes[0].topic=" + routed).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(describe(legacy).partitions()).hasSize(1);
            assertThat(describe(routed).partitions()).hasSize(1);
            assertThat(admin.listTopics().names().get()).doesNotContain(routed + "-dlt", routed + "-replay");
        });
    }

    @Test void reusesExistingSourceAndDltWithoutChangingMetadataOrConfigs() throws Exception {
        String source = topic();
        admin.createTopics(List.of(new NewTopic(source, 3, (short) 1).configs(Map.of("retention.ms", "123456")),
                new NewTopic(source + "-dlt", 4, (short) 1))).all().get();
        var before = describe(source);
        runner(source).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(describe(source)).isEqualTo(before);
            assertThat(describe(source + "-dlt").partitions()).hasSize(4);
            var resource = new org.apache.kafka.common.config.ConfigResource(
                    org.apache.kafka.common.config.ConfigResource.Type.TOPIC, source);
            assertThat(admin.describeConfigs(List.of(resource)).all().get().get(resource).get("retention.ms").value())
                    .isEqualTo("123456");
        });
    }

    @Test void missingDltUsesExistingSourcePartitionCount() throws Exception {
        String source = topic();
        admin.createTopics(List.of(new NewTopic(source, 4, (short) 1))).all().get();
        runner(source).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(describe(source + "-dlt").partitions()).hasSize(4);
        });
    }

    @Test void incompatibleDltFailsWithoutIncreasingPartitions() throws Exception {
        String source = topic();
        admin.createTopics(List.of(new NewTopic(source, 3, (short) 1), new NewTopic(source + "-dlt", 1, (short) 1)))
                .all().get();
        runner(source).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasMessageContaining(source + "-dlt")
                    .hasMessageContaining("at least 3 partitions");
            assertThat(describe(source + "-dlt").partitions()).hasSize(1);
        });
    }

    @Test void concurrentCellStartsAreIdempotent() {
        String source = topic();
        Runnable start = () -> runner(source).run(context -> assertThat(context).hasNotFailed());
        CompletableFuture.allOf(CompletableFuture.runAsync(start), CompletableFuture.runAsync(start)).join();
    }

    @Test void outgoingDestinationThatIsAlsoADltUsesSourcePartitions() throws Exception {
        String source = topic();
        admin.createTopics(List.of(new NewTopic(source, 4, (short) 1))).all().get();
        runner(source).withPropertyValues("apex.messaging.outbox.publisher.enabled=true",
                "apex.messaging.publisher.topic=" + source + "-dlt").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(describe(source + "-dlt").partitions()).hasSize(4);
        });
    }

    @Test void unsupportedReplicationFactorFailsWithoutFallingBack() throws Exception {
        String source = topic();
        runner(source).withPropertyValues("apex.messaging.provisioning.replication-factor=2").run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasMessageContaining(source)
                    .hasMessageContaining("InvalidReplicationFactorException");
        });
        assertThat(admin.listTopics().names().get()).doesNotContain(source, source + "-dlt");
    }

    @Test void invalidNamesAndRoutesFailBeforeCreatingAnyTopics() throws Exception {
        var before = admin.listTopics().names().get();
        runner("invalid*").run(context -> assertThat(context).hasFailed());
        runner(topic()).withPropertyValues("apex.messaging.publisher.routing.enabled=true")
                .run(context -> assertThat(context).hasFailed());
        runner(topic()).withPropertyValues("apex.messaging.consumer.reliability.dlt-suffix=" + "x".repeat(64),
                "apex.messaging.consumer.topics=" + "s".repeat(240)).run(context -> assertThat(context).hasFailed());
        assertThat(admin.listTopics().names().get()).isEqualTo(before);
    }

    @Test void disabledProvisioningLeavesLegacyPreflightAndMissingTopicsUntouched() throws Exception {
        String source = topic();
        runner(source).withPropertyValues("apex.messaging.provisioning.enabled=false")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasStackTraceContaining("source/DLT provisioning preflight failed");
                });
        runner(source).withPropertyValues("apex.messaging.provisioning.enabled=false",
                "apex.messaging.consumer.reliability.preflight-enabled=false").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(ApexTopicProvisioner.class);
        });
        assertThat(admin.listTopics().names().get()).doesNotContain(source, source + "-dlt");
    }

    @Test void unavailableBrokerFailsStartupWithSafeTopicDiagnostic() {
        String source = topic();
        runner(source).withPropertyValues("spring.kafka.bootstrap-servers=127.0.0.1:1",
                "apex.messaging.provisioning.timeout=100ms",
                "spring.kafka.admin.properties.default.api.timeout.ms=200",
                "spring.kafka.admin.properties.request.timeout.ms=100").run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasMessageContaining(source).hasMessageContaining("TimeoutException");
            assertThat(context.getStartupFailure().getCause()).isNull();
        });
    }
}
