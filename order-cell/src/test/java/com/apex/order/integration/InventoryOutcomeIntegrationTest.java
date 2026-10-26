package com.apex.order.integration;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import com.apex.order.SagaSubscriptionPreflight;
import com.apex.order.support.DisposableOrderRuntime;
import com.apex.platform.events.EventEnvelope;
import com.apex.platform.events.EventMetadata;
import com.apex.platform.messaging.consumer.EventHandlerRegistry;
import com.apex.platform.messaging.jdbc.ConfigurableKafkaEventConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class InventoryOutcomeIntegrationTest {
    private DisposableOrderRuntime runtime;
    private JdbcTemplate jdbc;
    private JsonMapper mapper;

    @BeforeAll
    void start() throws Exception {
        runtime = DisposableOrderRuntime.start(true);
        jdbc = runtime.application().getBean(JdbcTemplate.class);
        mapper = runtime.application().getBean(JsonMapper.class);
        new SagaSubscriptionPreflight(runtime.application().getBean(EventHandlerRegistry.class)).start();
    }

    @AfterAll
    void close() throws Exception {
        if (runtime != null) {
            runtime.close();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INVENTORY_RESERVED", "INVENTORY_RESERVATION_REJECTED"})
    void appliesOutcomeThroughInboxAndToleratesEventAndBusinessDuplicates(String type) {
        UUID orderId = seed("PENDING_INVENTORY");
        EventEnvelope<?> event = outcome(orderId, type);
        String expected = type.equals("INVENTORY_RESERVED") ? "PENDING_PAYMENT" : "CANCELLED";
        consume(event, () -> {
            assertThat(status(orderId)).isEqualTo(expected);
            assertThat(inbox(event)).isEqualTo(1);
        });
        consume(event, () -> {});
        var second = outcome(orderId, type);
        consume(second, () -> {});
        assertThat(status(orderId)).isEqualTo(expected);
        assertThat(inbox(event)).isEqualTo(1);
        assertThat(inbox(second)).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"CREATED", "CONFIRMED", "CANCELLING", "CANCELLED"})
    void lateReservedOutcomeDoesNotOverwriteConflictingStateOrAck(String current) {
        UUID orderId = seed(current);
        var event = outcome(orderId, "INVENTORY_RESERVED");
        var ack = new AtomicBoolean();
        assertThatThrownBy(() -> consume(event, () -> ack.set(true)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Conflicting or stale");
        assertThat(ack).isFalse();
        assertThat(status(orderId)).isEqualTo(current);
        assertThat(inbox(event)).isZero();
    }

    @Test
    void rejectedAfterReservedFailsClosed() {
        UUID orderId = seed("PENDING_PAYMENT");
        var event = outcome(orderId, "INVENTORY_RESERVATION_REJECTED");
        var ack = new AtomicBoolean();
        assertThatThrownBy(() -> consume(event, () -> ack.set(true))).isInstanceOf(IllegalStateException.class);
        assertThat(ack).isFalse();
        assertThat(status(orderId)).isEqualTo("PENDING_PAYMENT");
        assertThat(inbox(event)).isZero();
    }

    @Test
    void missingOrderRollsBackInboxAndDoesNotAckThenRetriesWhenOrderArrives() {
        UUID orderId = UUID.randomUUID();
        var event = outcome(orderId, "INVENTORY_RESERVED");
        var ack = new AtomicBoolean();
        assertThatThrownBy(() -> consume(event, () -> ack.set(true)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("missing order");
        assertThat(ack).isFalse();
        assertThat(inbox(event)).isZero();
        insert(orderId, "PENDING_INVENTORY");
        consume(event, () -> ack.set(true));
        assertThat(ack).isTrue();
        assertThat(status(orderId)).isEqualTo("PENDING_PAYMENT");
    }

    @Test
    void commitFailureRollsBackStatusAndInboxAndNeverAcknowledges() {
        UUID orderId = seed("PENDING_INVENTORY");
        var event = outcome(orderId, "INVENTORY_RESERVED");
        var ack = new AtomicBoolean();
        jdbc.execute("CREATE FUNCTION saga_reject_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                + "IF NEW.event_id = '" + event.metadata().eventId()
                + "'::uuid THEN RAISE EXCEPTION 'test commit failure'; END IF; RETURN NEW; END $$");
        jdbc.execute("CREATE CONSTRAINT TRIGGER saga_commit_failure AFTER INSERT ON processed_event "
                + "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION saga_reject_commit()");
        try {
            assertThatThrownBy(() -> consume(event, () -> ack.set(true))).isInstanceOf(RuntimeException.class);
            assertThat(ack).isFalse();
            assertThat(status(orderId)).isEqualTo("PENDING_INVENTORY");
            assertThat(inbox(event)).isZero();
        } finally {
            jdbc.execute("DROP TRIGGER saga_commit_failure ON processed_event");
            jdbc.execute("DROP FUNCTION saga_reject_commit()");
        }
        consume(event, () -> ack.set(true));
        assertThat(ack).isTrue();
        assertThat(status(orderId)).isEqualTo("PENDING_PAYMENT");
    }

    @Test
    void malformedOutcomeCannotAckOrClaimInbox() {
        var valid = outcome(UUID.randomUUID(), "INVENTORY_RESERVED");
        var invalid = new EventEnvelope<>(valid.metadata(), Map.of());
        var ack = new AtomicBoolean();
        assertThatThrownBy(() -> consume(invalid, () -> ack.set(true))).isInstanceOf(RuntimeException.class);
        assertThat(ack).isFalse();
        assertThat(inbox(valid)).isZero();
    }

    @Test
    void preflightRejectsIncompleteHandlerCoverage() {
        var registry = org.mockito.Mockito.mock(EventHandlerRegistry.class);
        org.mockito.Mockito.when(registry.find("INVENTORY_RESERVED", 1))
                .thenReturn(runtime.application().getBean(EventHandlerRegistry.class).find("INVENTORY_RESERVED", 1));
        org.mockito.Mockito.when(registry.find("INVENTORY_RESERVATION_REJECTED", 1))
                .thenReturn(java.util.Optional.empty());
        assertThatThrownBy(() -> new SagaSubscriptionPreflight(registry).start())
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("INVENTORY_RESERVATION_REJECTED V1");
        try (var context = new org.springframework.context.annotation.AnnotationConfigApplicationContext()) {
            context.getEnvironment().setActiveProfiles("saga");
            context.registerBean(EventHandlerRegistry.class, () -> registry);
            context.register(SagaSubscriptionPreflight.class);
            assertThatThrownBy(context::refresh).hasRootCauseInstanceOf(IllegalStateException.class)
                    .hasStackTraceContaining("INVENTORY_RESERVATION_REJECTED V1");
        }
    }

    @Test
    void placementHttpCommitsOrderItemsAndPlacedOutboxAtomicallyAndCanBeRead() throws Exception {
        String reference = "saga-" + UUID.randomUUID();
        var response = post(reference);
        assertThat(response.statusCode()).isEqualTo(201);
        var body = mapper.readTree(response.body());
        assertThat(body.get("status").asText()).isEqualTo("PENDING_INVENTORY");
        UUID orderId = UUID.fromString(body.get("orderId").asText());
        assertThat(status(orderId)).isEqualTo("PENDING_INVENTORY");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM order_items WHERE order_id = ?", Long.class, orderId))
                .isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT event_type FROM outbox_event WHERE aggregate_id = ?", String.class, orderId))
                .isEqualTo("ORDER_PLACED");
        try (HttpClient client = HttpClient.newHttpClient()) {
            var result = client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + runtime.port()
                    + "/api/v1/order-placements/" + orderId)).GET().build(), HttpResponse.BodyHandlers.ofString());
            assertThat(result.statusCode()).isEqualTo(200);
            assertThat(mapper.readTree(result.body()).get("status").asText()).isEqualTo("PENDING_INVENTORY");
        }
    }

    @Test
    void placementOutboxFailureRollsBackOrderAndItems() throws Exception {
        String reference = "saga-" + UUID.randomUUID();
        jdbc.execute("ALTER TABLE outbox_event ADD CONSTRAINT saga_fail_append "
                + "CHECK (payload->>'externalReference' <> '" + reference + "')");
        try {
            assertThat(post(reference).statusCode()).isEqualTo(500);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM orders WHERE external_reference = ?", Long.class, reference))
                    .isZero();
            assertThat(jdbc.queryForObject("SELECT count(*) FROM order_items i LEFT JOIN orders o ON o.id=i.order_id "
                    + "WHERE o.id IS NULL", Long.class)).isZero();
        } finally {
            jdbc.execute("ALTER TABLE outbox_event DROP CONSTRAINT saga_fail_append");
        }
    }

    private HttpResponse<String> post(String reference) throws Exception {
        String body = mapper.writeValueAsString(Map.of("externalReference", reference, "description", "سفارش تست",
                "items", java.util.List.of(Map.of("productId", UUID.randomUUID(), "quantity", 2),
                        Map.of("productId", UUID.randomUUID(), "quantity", 1))));
        try (HttpClient client = HttpClient.newHttpClient()) {
            return client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + runtime.port()
                    + "/api/v1/order-placements")).header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
        }
    }

    private EventEnvelope<?> outcome(UUID orderId, String type) {
        return new EventEnvelope<>(new EventMetadata(UUID.randomUUID(), type, 1, UUID.randomUUID(), UUID.randomUUID(),
                orderId.toString(), "INVENTORY_RESERVATION", "inventory-cell", "inventory-cell", Instant.now()),
                Map.of("orderId", orderId));
    }

    private void consume(EventEnvelope<?> event, Runnable ack) {
        runtime.application().getBean(ConfigurableKafkaEventConsumer.class).consume(
                new ConsumerRecord<>("apex.inventory.events.v1", 0, 0, event.metadata().aggregateId(),
                        mapper.writeValueAsString(event)), ack::run);
    }

    private long inbox(EventEnvelope<?> event) {
        return jdbc.queryForObject("SELECT count(*) FROM processed_event WHERE event_id = ?",
                Long.class, event.metadata().eventId());
    }

    private String status(UUID id) {
        return jdbc.queryForObject("SELECT status FROM orders WHERE id = ?", String.class, id);
    }

    private UUID seed(String status) {
        UUID id = UUID.randomUUID();
        insert(id, status);
        return id;
    }

    private void insert(UUID id, String status) {
        jdbc.update("INSERT INTO orders(id, external_reference, description, status, created_at) "
                + "VALUES (?, ?, 'test', ?, CURRENT_TIMESTAMP)", id, "saga-" + id, status);
    }
}
