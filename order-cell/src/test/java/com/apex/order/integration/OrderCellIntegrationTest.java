package com.apex.order.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.sql.DataSource;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.aop.support.AopUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.apex.order.modules.order.application.CreateOrderUseCase;
import com.apex.order.support.DisposableOrderRuntime;
import com.apex.platform.core.context.ExecutionContext;
import com.apex.platform.core.context.ExecutionContextProvider;
import com.apex.platform.events.EventEnvelopeFactory;
import com.apex.platform.events.api.CellEvents;
import com.apex.platform.messaging.consumer.EventMessageDispatcher;
import com.apex.platform.messaging.consumer.ProcessedEventStore;
import com.apex.platform.messaging.jdbc.OutboxPublisherScheduler;
import com.apex.platform.messaging.kafka.KafkaMessagePublisher;
import com.apex.platform.messaging.outbox.OutboxAppender;
import com.apex.platform.messaging.outbox.OutboxPublisher;
import com.apex.platform.webmvc.ApexHttpExecutionContextFilter;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@Tag("integration")
class OrderCellIntegrationTest {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String PERSIAN = "سفارش آزمایشی";
    private static DisposableOrderRuntime runtime;
    private static HttpClient http;
    private static JdbcTemplate jdbc;
    private static ExecutionContextProvider contextProvider;
    private static CreateOrderUseCase createOrder;

    @BeforeAll
    static void startDisposableApplication() throws Exception {
        runtime = DisposableOrderRuntime.start();
        http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
        jdbc = runtime.application().getBean(JdbcTemplate.class);
        contextProvider = runtime.application().getBean(ExecutionContextProvider.class);
        createOrder = runtime.application().getBean(CreateOrderUseCase.class);
    }

    @AfterAll
    static void closeDisposableApplication() throws Exception {
        try {
            if (runtime != null) {
                runtime.close();
            }
        } finally {
            if (http != null) {
                http.close();
            }
        }
    }

    @Test
    void startersConfigureTheProducerAndHttpContextWithoutCellInfrastructureBeans() {
        for (Class<?> type : List.of(ExecutionContextProvider.class, ApexHttpExecutionContextFilter.class,
                EventEnvelopeFactory.class, CellEvents.class, OutboxAppender.class,
                KafkaMessagePublisher.class, OutboxPublisher.class, OutboxPublisherScheduler.class,
                DataSource.class, PlatformTransactionManager.class)) {
            assertThat(runtime.application().getBeansOfType(type)).as(type.getSimpleName()).hasSize(1);
        }
        assertThat(runtime.application().getBeansOfType(EventMessageDispatcher.class)).isEmpty();
        assertThat(runtime.application().getBeansOfType(ProcessedEventStore.class)).isEmpty();
        assertThat(AopUtils.isAopProxy(createOrder)).isTrue();
        assertThat(runtime.application().getBean(PlatformTransactionManager.class)).isInstanceOf(JdbcTransactionManager.class);
    }

    @Test
    void httpCommitsOrderAndOutboxThenScheduledPublisherDeliversOriginalEvent() throws Exception {
        String reference = reference();
        HttpResponse<String> response = post(reference, PERSIAN);
        assertThat(response.statusCode()).isEqualTo(201);
        JsonNode order = JSON.readTree(response.body());
        UUID id = UUID.fromString(order.get("id").asText());
        assertThat(order.get("description").asText()).isEqualTo(PERSIAN);
        assertThat(orderCount(reference)).isEqualTo(1);
        assertThat(outboxCount(reference)).isEqualTo(1);
        Map<String, Object> stored = jdbc.queryForMap("SELECT * FROM outbox_event WHERE aggregate_id=?", id);
        UUID eventId = (UUID) stored.get("event_id");
        assertThat(stored.get("correlation_id")).isNotNull();
        assertThat(stored.get("causation_id")).isNull();
        assertThat(stored.get("source_service")).isEqualTo("order-cell");
        assertThat(stored.get("cell_id")).isEqualTo("order-cell");

        ConsumerRecord<String, String> record = observe(eventId);
        JsonNode event = JSON.readTree(record.value());
        JsonNode metadata = event.get("metadata");
        assertThat(record.topic()).isEqualTo(DisposableOrderRuntime.TOPIC);
        assertThat(record.key()).isEqualTo("ORDER:" + id);
        assertThat(metadata.get("eventId").asText()).isEqualTo(eventId.toString());
        assertThat(metadata.get("correlationId").asText()).isEqualTo(stored.get("correlation_id").toString());
        assertThat(metadata.get("causationId").isNull()).isTrue();
        assertThat(metadata.get("aggregateId").asText()).isEqualTo(id.toString());
        assertThat(metadata.get("aggregateType").asText()).isEqualTo("ORDER");
        assertThat(metadata.get("eventType").asText()).isEqualTo("ORDER_CREATED");
        assertThat(metadata.get("eventVersion").asInt()).isEqualTo(1);
        Instant occurredAt = jdbc.queryForObject("SELECT occurred_at FROM outbox_event WHERE event_id=?",
                (rs, row) -> rs.getObject(1, OffsetDateTime.class).toInstant(), eventId);
        assertThat(Instant.parse(metadata.get("occurredAt").asText()))
                .isEqualTo(occurredAt);
        assertThat(event.get("payload").get("orderId").asText()).isEqualTo(id.toString());
        assertThat(event.get("payload").get("externalReference").asText()).isEqualTo(reference);
        assertThat(event.get("payload").get("description").asText()).isEqualTo(PERSIAN);
        assertThat(Instant.parse(event.get("payload").get("createdAt").asText()))
                .isEqualTo(Instant.parse(order.get("createdAt").asText()));
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(jdbc.queryForObject("SELECT published FROM outbox_event WHERE event_id=?", Boolean.class, eventId)).isTrue());
        assertThat(jdbc.queryForObject("SELECT published_at FROM outbox_event WHERE event_id=?", OffsetDateTime.class, eventId)).isNotNull();
        assertThat(jdbc.queryForObject("SELECT last_error FROM outbox_event WHERE event_id=?", String.class, eventId)).isNull();
        System.out.printf("M24_ORDER_PUBLICATION_PASS topic=%s eventId=%s correlationId=%s%n",
                record.topic(), eventId, stored.get("correlation_id"));
    }

    @Test
    void outerTransactionRollbackRemovesBothOrderAndOutbox() {
        String reference = reference();
        TransactionTemplate transaction = new TransactionTemplate(runtime.application().getBean(PlatformTransactionManager.class));
        assertThatThrownBy(() -> transaction.executeWithoutResult(status -> {
            try (var scope = contextProvider.open(ExecutionContext.root("order-cell", "order-cell"))) {
                createOrder.execute(reference, PERSIAN);
                assertThat(orderCount(reference)).isEqualTo(1);
                assertThat(outboxCount(reference)).isEqualTo(1);
                throw new IllegalStateException("M24 forced rollback after both INSERTs");
            }
        })).isInstanceOf(IllegalStateException.class).hasMessageContaining("M24 forced rollback");
        assertThat(orderCount(reference)).isZero();
        assertThat(outboxCount(reference)).isZero();
        assertThatIllegalStateException().isThrownBy(contextProvider::requireCurrent);
    }

    @Test
    void httpOutboxInsertFailureRollsBackTheAlreadyInsertedOrder() throws Exception {
        String reference = "M24-OUTBOX-REJECTED";
        jdbc.execute("ALTER TABLE outbox_event ADD CONSTRAINT m24_reject_test_event "
                + "CHECK (payload->>'externalReference' <> 'M24-OUTBOX-REJECTED')");
        try {
            assertThat(post(reference, PERSIAN).statusCode()).isEqualTo(500);
            assertThat(orderCount(reference)).isZero();
            assertThat(outboxCount(reference)).isZero();
        } finally {
            jdbc.execute("ALTER TABLE outbox_event DROP CONSTRAINT m24_reject_test_event");
        }
    }

    @Test
    void nonHttpUseCaseRequiresAnExplicitExecutionContextAndRollsBackWithoutOne() {
        String reference = reference();
        assertThatIllegalStateException().isThrownBy(() -> createOrder.execute(reference, PERSIAN))
                .withMessage("No active APEX execution context");
        assertThat(orderCount(reference)).isZero();
        assertThat(outboxCount(reference)).isZero();
    }

    @Test
    void invalidHttpInputCreatesNeitherOrderNorEvent() throws Exception {
        String reference = reference();
        assertThat(post(reference, " ").statusCode()).isEqualTo(400);
        assertThat(orderCount(reference)).isZero();
        assertThat(outboxCount(reference)).isZero();
    }

    @Test
    void separateHttpRequestsEstablishIndependentRootContexts() throws Exception {
        String first = reference();
        String second = reference();
        assertThat(post(first, PERSIAN).statusCode()).isEqualTo(201);
        assertThat(post(second, PERSIAN).statusCode()).isEqualTo(201);
        UUID firstCorrelation = jdbc.queryForObject(
                "SELECT correlation_id FROM outbox_event WHERE payload->>'externalReference'=?", UUID.class, first);
        UUID secondCorrelation = jdbc.queryForObject(
                "SELECT correlation_id FROM outbox_event WHERE payload->>'externalReference'=?", UUID.class, second);
        assertThat(firstCorrelation).isNotNull().isNotEqualTo(secondCorrelation);
        assertThat(secondCorrelation).isNotNull();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_event "
                + "WHERE payload->>'externalReference' IN (?, ?) AND causation_id IS NOT NULL", Long.class, first, second)).isZero();
    }

    private static HttpResponse<String> post(String reference, String description) throws Exception {
        String body = JSON.writeValueAsString(Map.of("externalReference", reference, "description", description));
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + runtime.port() + "/api/v1/orders"))
                .timeout(Duration.ofSeconds(10)).header("Content-Type", "application/json; charset=UTF-8")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build(),
                HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    private static long orderCount(String reference) {
        return jdbc.queryForObject("SELECT count(*) FROM orders WHERE external_reference=?", Long.class, reference);
    }

    private static long outboxCount(String reference) {
        return jdbc.queryForObject("SELECT count(*) FROM outbox_event WHERE payload->>'externalReference'=?", Long.class, reference);
    }

    private static String reference() {
        return "M24-" + UUID.randomUUID();
    }

    private static ConsumerRecord<String, String> observe(UUID eventId) {
        Map<String, Object> properties = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, runtime.brokers(),
                ConsumerConfig.GROUP_ID_CONFIG, "m24-observer-" + UUID.randomUUID(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (KafkaConsumer<String, String> observer = new KafkaConsumer<>(properties)) {
            observer.assign(List.of(new TopicPartition(DisposableOrderRuntime.TOPIC, 0)));
            observer.seekToBeginning(List.of(new TopicPartition(DisposableOrderRuntime.TOPIC, 0)));
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : observer.poll(Duration.ofMillis(200))) {
                    if (JSON.readTree(record.value()).get("metadata").get("eventId").asText().equals(eventId.toString())) {
                        return record;
                    }
                }
            }
        }
        throw new AssertionError("Scheduled publisher did not deliver event " + eventId);
    }
}
