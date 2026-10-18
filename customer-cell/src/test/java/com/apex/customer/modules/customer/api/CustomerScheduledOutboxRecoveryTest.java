package com.apex.customer.modules.customer.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.AbstractTestExecutionListener;
import org.springframework.test.web.servlet.MockMvc;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import com.apex.customer.support.IsolatedTestEnvironment;
import com.apex.platform.messaging.jdbc.OutboxPublisherScheduler;
import com.apex.platform.messaging.kafka.KafkaMessagePublisher;

@SpringBootTest(properties = {"spring.config.location=classpath:application.yml", "spring.config.additional-location=",
        "spring.config.import=", "spring.profiles.active=", "spring.profiles.include="})
@ContextConfiguration(initializers = IsolatedTestEnvironment.class)
@TestExecutionListeners(
        listeners = CustomerScheduledOutboxRecoveryTest.PostgresCleanup.class,
        mergeMode = TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS)
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 1, topics = "apex.customer.recovery.test")
@Import(CustomerScheduledOutboxRecoveryTest.TransportFixture.class)
@DirtiesContext
class CustomerScheduledOutboxRecoveryTest {
    private static EmbeddedPostgres postgres;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry properties) throws Exception {
        postgres = EmbeddedPostgres.builder().start();
        IsolatedTestEnvironment.kafka(properties);
        properties.add("spring.datasource.url", () -> postgres.getJdbcUrl("postgres", "postgres"));
        properties.add("spring.datasource.username", () -> "postgres");
        properties.add("spring.datasource.password", () -> "postgres");
        properties.add("apex.messaging.publisher.topic", () -> "apex.customer.recovery.test");
        properties.add("apex.messaging.outbox.publisher.interval-ms", () -> "100");
        properties.add("apex.messaging.outbox.publisher.enabled", () -> "true");
        properties.add("apex.messaging.consumer.enabled", () -> "false");
    }

    @Autowired MockMvc http;
    @Autowired JdbcTemplate database;
    @Autowired ControlledTransport transport;
    @Autowired ApplicationContext context;

    @Test
    void scheduledRetryPreservesEventAndWaitsForTransportAcknowledgment() throws Exception {
        try {
            assertThat(context.getBeansOfType(OutboxPublisherScheduler.class)).hasSize(1);
            System.out.printf("M22_PRODUCER_RUNTIME scheduler=%s store=%s transactionManager=%s topic=%s bootstrap=%s%n",
                    OutboxPublisherScheduler.class.getName(),
                    context.getBean(com.apex.platform.messaging.outbox.OutboxStore.class).getClass().getName(),
                    context.getBean(org.springframework.transaction.PlatformTransactionManager.class).getClass().getName(),
                    context.getEnvironment().getProperty("apex.messaging.publisher.topic"),
                    context.getEnvironment().getProperty("spring.kafka.bootstrap-servers"));
            http.perform(post("/api/v1/customers").contentType(MediaType.APPLICATION_JSON)
                    .content("{\"externalReference\":\"SCHEDULED-RECOVERY\",\"displayName\":\"آزمایش\"}"))
                    .andExpect(status().isCreated());
            String select = "SELECT * FROM outbox_event WHERE payload->>'externalReference'='SCHEDULED-RECOVERY'";
            var original = database.queryForMap(select);
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                var failed = database.queryForMap(select);
                assertThat(failed.get("published")).isEqualTo(false);
                assertThat(failed.get("retry_count")).isEqualTo(1);
                assertThat(failed.get("dead_letter")).isEqualTo(false);
                assertThat(failed.get("last_error").toString()).contains("TimeoutException");
                assertThat(failed.get("claim_token")).isNull();
            });
            // Only the disposable fixture row is made immediately retryable; production backoff is unchanged.
            database.update("UPDATE outbox_event SET next_retry_at=now() WHERE event_id=?", original.get("event_id"));
            assertThat(transport.brokerAcknowledged.await(15, TimeUnit.SECONDS)).isTrue();
            var waiting = database.queryForMap(select);
            assertThat(waiting.get("published")).isEqualTo(false);
            assertThat(waiting.get("claim_token")).isNotNull();
            transport.release.complete(null);
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(database.queryForMap(select).get("published")).isEqualTo(true));
            var delivered = database.queryForMap(select);
            for (String field : new String[]{"event_id", "correlation_id", "causation_id", "occurred_at",
                    "event_type", "event_version", "aggregate_id", "aggregate_type", "payload", "source_service", "cell_id"}) {
                assertThat(delivered.get(field)).as(field).isEqualTo(original.get(field));
            }
            assertThat(transport.attempts.get()).isEqualTo(2);
        } finally {
            transport.release.complete(null);
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class TransportFixture {
        @Bean ControlledTransport controlledTransport(KafkaTemplate<String, String> kafka) {
            return new ControlledTransport(kafka);
        }
    }

    public static class PostgresCleanup extends AbstractTestExecutionListener {
        @Override public int getOrder() { return 0; }

        @Override
        public void afterTestClass(TestContext context) throws Exception {
            try {
                context.markApplicationContextDirty(DirtiesContext.HierarchyMode.EXHAUSTIVE);
            } finally {
                if (postgres != null) {
                    postgres.close();
                    postgres = null;
                }
            }
        }
    }

    static class ControlledTransport implements KafkaMessagePublisher {
        final AtomicInteger attempts = new AtomicInteger();
        final CountDownLatch brokerAcknowledged = new CountDownLatch(1);
        final CompletableFuture<Void> release = new CompletableFuture<>();
        final KafkaTemplate<String, String> kafka;
        ControlledTransport(KafkaTemplate<String, String> kafka) { this.kafka = kafka; }

        public CompletionStage<Void> publish(String topic, String key, String message) {
            if (attempts.incrementAndGet() == 1) {
                return CompletableFuture.failedFuture(new IllegalStateException("Send failed",
                        new TimeoutException("isolated test send failure")));
            }
            return kafka.send(topic, key, message).thenCompose(result -> {
                brokerAcknowledged.countDown();
                return release;
            });
        }
    }
}
