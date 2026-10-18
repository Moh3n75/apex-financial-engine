package com.apex.reporting.modules.reporting;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.support.TransactionTemplate;

import com.apex.platform.events.EventEnvelope;
import com.apex.platform.events.EventMetadata;
import com.apex.platform.messaging.consumer.ConsumerContext;
import com.apex.platform.messaging.consumer.DispatchResult;
import com.apex.platform.messaging.consumer.EventMessageDispatcher;
import com.apex.reporting.modules.reporting.events.CustomerCreatedV1Payload;
import com.apex.reporting.support.ReportingDatabaseExtension;
import com.apex.reporting.support.IsolatedTestEnvironment;

import tools.jackson.databind.json.JsonMapper;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.config.location=classpath:application.yml", "spring.config.additional-location=",
        "spring.config.import=", "spring.profiles.active=", "spring.profiles.include="})
@ContextConfiguration(initializers = IsolatedTestEnvironment.class)
@AutoConfigureMockMvc
@ExtendWith(ReportingDatabaseExtension.class)
@EmbeddedKafka(partitions = 1, topics = {"apex.customer.events.v1"})
class CustomerCreatedProjectionTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        ReportingDatabaseExtension.registerDatasource(registry);
        IsolatedTestEnvironment.kafka(registry);
        registry.add("apex.messaging.cell.enabled", () -> "true");
        registry.add("apex.messaging.publisher.topic", () -> "apex.customer.events.v1");
        registry.add("apex.messaging.outbox.publisher.enabled", () -> "false");
        registry.add("apex.messaging.consumer.enabled", () -> "true");
        registry.add("apex.messaging.consumer.topics", () -> "apex.customer.events.v1");
        registry.add("spring.kafka.consumer.group-id", () -> "reporting-cell-test");
        registry.add("apex.cell.id", () -> "reporting-cell-test");
    }

    @Autowired
    private EventMessageDispatcher dispatcher;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private JsonMapper jsonMapper;

    @Test
    void projectsCustomerCreatedAndExposesReadApi() throws Exception {
        long before = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM customer_summary", Long.class);
        UUID customerId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        String envelope = envelopeJson(eventId, customerId, "CUST-R-1", "مشتری آزمایشی");

        DispatchResult result = dispatcher.dispatch(envelope, context(0));
        assertThat(result).isEqualTo(DispatchResult.HANDLED);

        mockMvc.perform(get("/api/v1/reports/customers/{id}", customerId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.externalReference").value("CUST-R-1"))
                .andExpect(jsonPath("$.displayName").value("مشتری آزمایشی"));

        mockMvc.perform(get("/api/v1/reports/customers/count"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.count").value(before + 1));
    }

    @Test
    void duplicateEventIdIsIdempotent() throws Exception {
        UUID customerId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        String envelope = envelopeJson(eventId, customerId, "CUST-DUP", "نام");

        assertThat(dispatcher.dispatch(envelope, context(1))).isEqualTo(DispatchResult.HANDLED);
        assertThat(dispatcher.dispatch(envelope, context(2))).isEqualTo(DispatchResult.DUPLICATE);

        Long projections = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM customer_summary WHERE customer_id = ?",
                Long.class,
                customerId
        );
        Long processed = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM processed_event WHERE event_id = ?",
                Long.class,
                eventId
        );
        assertThat(projections).isEqualTo(1L);
        assertThat(processed).isEqualTo(1L);
    }

    @Test
    void concurrentDuplicateEventIdKeepsSingleEffect() throws Exception {
        UUID customerId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        String envelope = envelopeJson(eventId, customerId, "CUST-CONC", "همزمان");

        AtomicInteger handled = new AtomicInteger();
        AtomicInteger duplicates = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);

        Runnable task = () -> {
            try {
                start.await(10, TimeUnit.SECONDS);
                DispatchResult result = dispatcher.dispatch(envelope, context(Thread.currentThread().threadId()));
                if (result == DispatchResult.HANDLED) {
                    handled.incrementAndGet();
                } else if (result == DispatchResult.DUPLICATE) {
                    duplicates.incrementAndGet();
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        };

        var f1 = pool.submit(task);
        var f2 = pool.submit(task);
        start.countDown();
        f1.get(30, TimeUnit.SECONDS);
        f2.get(30, TimeUnit.SECONDS);
        pool.shutdownNow();

        assertThat(handled.get()).isEqualTo(1);
        assertThat(duplicates.get()).isEqualTo(1);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM customer_summary WHERE customer_id = ?", Long.class, customerId)).isEqualTo(1L);
    }

    @Test
    void projectionFailureRollsBackInboxMarker() throws Exception {
        UUID customerId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        String envelope = envelopeJson(eventId, customerId, "CUST-FAIL", "شکست");

        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status -> {
            DispatchResult result = dispatcher.dispatch(envelope, context(99));
            assertThat(result).isEqualTo(DispatchResult.HANDLED);
            throw new IllegalStateException("force projection transaction rollback");
        })).hasMessageContaining("force projection transaction rollback");

        Long processed = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM processed_event WHERE event_id = ?",
                Long.class,
                eventId
        );
        Long projections = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM customer_summary WHERE customer_id = ?",
                Long.class,
                customerId
        );
        assertThat(processed).isZero();
        assertThat(projections).isZero();
        assertThat(dispatcher.dispatch(envelope, context(100))).isEqualTo(DispatchResult.HANDLED);
        assertThat(jdbcTemplate.queryForObject("SELECT COUNT(*) FROM processed_event WHERE event_id = ?",
                Long.class, eventId)).isEqualTo(1L);
        assertThat(jdbcTemplate.queryForObject("SELECT COUNT(*) FROM customer_summary WHERE customer_id = ?",
                Long.class, customerId)).isEqualTo(1L);
    }

    @Test
    void unsupportedEventVersionIsIgnored() throws Exception {
        long before = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM customer_summary", Long.class);
        UUID eventId = UUID.randomUUID();
        Map<String, Object> metadata = Map.of(
                "eventId", eventId.toString(),
                "eventType", "CUSTOMER_CREATED",
                "eventVersion", 99,
                "correlationId", UUID.randomUUID().toString(),
                "aggregateId", UUID.randomUUID().toString(),
                "aggregateType", "CUSTOMER",
                "sourceService", "customer-cell",
                "cellId", "customer-cell",
                "occurredAt", Instant.parse("2026-10-14T12:00:00Z").toString()
        );
        String envelope = JSON.writeValueAsString(Map.of(
                "metadata", metadata,
                "payload", Map.of(
                        "customerId", UUID.randomUUID().toString(),
                        "externalReference", "X",
                        "displayName", "Y",
                        "status", "ACTIVE",
                        "createdAt", Instant.parse("2026-10-14T12:00:00Z").toString()
                )
        ));

        assertThat(dispatcher.dispatch(envelope, context(3))).isEqualTo(DispatchResult.IGNORED);
        assertThat(jdbcTemplate.queryForObject("SELECT COUNT(*) FROM customer_summary", Long.class)).isEqualTo(before);
        assertThat(jdbcTemplate.queryForObject("SELECT COUNT(*) FROM processed_event WHERE event_id = ?",
                Long.class, eventId)).isZero();
    }

    private static ConsumerContext context(long offset) {
        return new ConsumerContext(
                "reporting-cell-test",
                "apex.customer.events.v1",
                0,
                offset,
                "key",
                Instant.parse("2026-10-14T12:00:00Z")
        );
    }

    private String envelopeJson(UUID eventId, UUID customerId, String ref, String name) throws Exception {
        EventMetadata metadata = new EventMetadata(
                eventId,
                CustomerCreatedV1Payload.EVENT_TYPE,
                CustomerCreatedV1Payload.EVENT_VERSION,
                UUID.randomUUID(),
                null,
                customerId.toString(),
                "CUSTOMER",
                "customer-cell",
                "customer-cell",
                Instant.parse("2026-10-14T12:00:00Z")
        );
        CustomerCreatedV1Payload payload = new CustomerCreatedV1Payload(
                customerId,
                ref,
                name,
                "ACTIVE",
                Instant.parse("2026-10-14T12:00:00Z")
        );
        return jsonMapper.writeValueAsString(new EventEnvelope<>(metadata, payload));
    }
}
