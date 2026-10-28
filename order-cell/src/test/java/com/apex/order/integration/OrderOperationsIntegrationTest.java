package com.apex.order.integration;

import static org.assertj.core.api.Assertions.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import com.apex.order.support.DisposableOrderRuntime;
import com.apex.platform.events.EventEnvelope;
import com.apex.platform.events.EventMetadata;
import com.apex.platform.messaging.jdbc.ConfigurableKafkaEventConsumer;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import tools.jackson.databind.json.JsonMapper;

@Tag("integration")
class OrderOperationsIntegrationTest {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String TOKEN = "isolated-test-read-token-" + UUID.randomUUID();

    static DisposableOrderRuntime start() throws Exception {
        return DisposableOrderRuntime.start(true, "--spring.profiles.active=three-cell-saga,operations",
                "--apex.order.operations.read-token=" + TOKEN, "--apex.order.operations.age-threshold=5m",
                "--apex.messaging.outbox.publisher.enabled=false");
    }

    static HttpResponse<String> get(HttpClient client, DisposableOrderRuntime app, String query, String token) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + app.port() + "/internal/operations/stalled-orders" + query));
        if (token != null) request.header("Authorization", "Bearer " + token);
        return client.send(request.GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    static UUID seed(JdbcTemplate jdbc, String status, String entry) {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO orders(id,external_reference,description,status,created_at,status_entered_at) VALUES (?,?,?,?,now()," + entry + ")",
                id, id.toString(), "isolated operations fixture", status);
        return id;
    }

    static void event(DisposableOrderRuntime app, UUID id, String type, UUID eventId) {
        String aggregate = type.startsWith("PAYMENT_") ? "PAYMENT_AUTHORIZATION" : "INVENTORY_RESERVATION";
        var envelope = new EventEnvelope<>(new EventMetadata(eventId, type, 1, UUID.randomUUID(), null,
                id.toString(), aggregate, "test", "test", Instant.now()), Map.of("orderId", id));
        app.application().getBean(ConfigurableKafkaEventConsumer.class).consume(
                new ConsumerRecord<>("outcomes", 0, 0, "test", JSON.writeValueAsString(envelope)), () -> { });
    }

    @Test
    void authenticatedBoundedReadDistinguishesUnknownHistoryFromPotentialStall() throws Exception {
        try (var app = start(); var client = HttpClient.newHttpClient()) {
            var jdbc = app.application().getBean(JdbcTemplate.class);
            UUID old = seed(jdbc, "PENDING_PAYMENT", "now()-interval '10 minutes'");
            UUID fresh = seed(jdbc, "PENDING_PAYMENT", "now()");
            UUID unknown = seed(jdbc, "CANCELLING", "NULL");
            seed(jdbc, "CONFIRMED", "now()-interval '1 day'");
            assertThat(get(client, app, "", null).statusCode()).isEqualTo(401);
            assertThat(get(client, app, "", "wrong").statusCode()).isEqualTo(401);
            var response = get(client, app, "?limit=1", TOKEN);
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.headers().firstValue("cache-control")).hasValue("no-store");
            var page = JSON.readTree(response.body());
            assertThat(page.path("hasMore").asBoolean()).isTrue();
            var row = page.path("orders").get(0);
            assertThat(row.path("orderId").asText()).isEqualTo(unknown.toString());
            assertThat(row.path("assessment").asText()).isEqualTo("STATE_ENTRY_TIME_UNKNOWN");
            assertThat(row.path("ageMillis").isNull()).isTrue();
            var second = JSON.readTree(get(client, app, "?limit=1&offset=1", TOKEN).body()).path("orders").get(0);
            assertThat(second.path("orderId").asText()).isEqualTo(old.toString());
            assertThat(second.path("assessment").asText()).isEqualTo("POTENTIALLY_STALLED");
            assertThat(second.path("paymentDecision").isNull()).isTrue();
            assertThat(second.path("quarantineInspection").asText()).isEqualTo("NOT_INSPECTED");
            assertThat(get(client, app, "", TOKEN).body()).doesNotContain(fresh.toString());
            for (String invalid : new String[]{"?limit=0", "?limit=101", "?offset=-1", "?offset=10001"}) {
                assertThat(get(client, app, invalid, TOKEN).statusCode()).isEqualTo(400);
            }
        }
    }

    @Test
    void transitionTimeChangesOnlyForCommittedNewStateAndTerminalOrdersDisappear() throws Exception {
        try (var app = start(); var client = HttpClient.newHttpClient()) {
            var jdbc = app.application().getBean(JdbcTemplate.class);
            UUID id = seed(jdbc, "PENDING_INVENTORY", "now()-interval '10 minutes'");
            OffsetDateTime before = jdbc.queryForObject("SELECT status_entered_at FROM orders WHERE id=?", OffsetDateTime.class, id);
            event(app, id, "INVENTORY_RESERVED", UUID.randomUUID());
            OffsetDateTime entered = jdbc.queryForObject("SELECT status_entered_at FROM orders WHERE id=?", OffsetDateTime.class, id);
            assertThat(entered).isAfter(before);
            event(app, id, "INVENTORY_RESERVED", UUID.randomUUID());
            assertThat(jdbc.queryForObject("SELECT status_entered_at FROM orders WHERE id=?", OffsetDateTime.class, id)).isEqualTo(entered);
            event(app, id, "PAYMENT_DECLINED", UUID.randomUUID());
            jdbc.update("UPDATE orders SET status_entered_at=now()-interval '10 minutes' WHERE id=?", id);
            var page = JSON.readTree(get(client, app, "", TOKEN).body());
            assertThat(page.path("orders").get(0).path("status").asText()).isEqualTo("CANCELLING");
            assertThat(page.path("orders").get(0).path("paymentDecision").asText()).isEqualTo("DECLINED");
            event(app, id, "INVENTORY_RELEASED", UUID.randomUUID());
            assertThat(JSON.readTree(get(client, app, "", TOKEN).body()).path("orders").isEmpty()).isTrue();
        }
    }

    @Test
    void localOutboxDiagnosticsNeverInventQuarantineOrChangeBusinessState() throws Exception {
        try (var app = start(); var client = HttpClient.newHttpClient()) {
            var jdbc = app.application().getBean(JdbcTemplate.class);
            String body = JSON.writeValueAsString(Map.of("externalReference", UUID.randomUUID().toString(), "description", "isolated",
                    "items", java.util.List.of(Map.of("productId", UUID.randomUUID(), "quantity", 1))));
            var response = client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + app.port() + "/api/v1/order-placements"))
                    .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(201);
            UUID id = UUID.fromString(JSON.readTree(response.body()).path("orderId").asText());
            assertThat(jdbc.queryForObject("SELECT status_entered_at FROM orders WHERE id=?", OffsetDateTime.class, id)).isNotNull();
            jdbc.update("UPDATE orders SET status_entered_at=now()-interval '10 minutes' WHERE id=?", id);
            var pending = JSON.readTree(get(client, app, "", TOKEN).body()).path("orders").get(0);
            assertThat(pending.path("pendingOutbox").asLong()).isEqualTo(1);
            assertThat(pending.path("diagnostic").asText()).isEqualTo("ORDER_OUTBOX_PENDING_PUBLICATION");
            jdbc.update("UPDATE outbox_event SET dead_letter=true WHERE aggregate_id=?", id);
            var dead = JSON.readTree(get(client, app, "", TOKEN).body()).path("orders").get(0);
            assertThat(dead.path("deadOutbox").asLong()).isEqualTo(1);
            assertThat(dead.path("diagnostic").asText()).isEqualTo("ORDER_OUTBOX_REQUIRES_OPERATOR_REVIEW");
            assertThat(dead.path("quarantineInspection").asText()).isEqualTo("NOT_INSPECTED");
            assertThat(jdbc.queryForObject("SELECT status FROM orders WHERE id=?", String.class, id)).isEqualTo("PENDING_INVENTORY");
        }
    }

    @Test
    void failedOutboxAppendRollsBackStatusTimeAndInbox() throws Exception {
        try (var app = start()) {
            var jdbc = app.application().getBean(JdbcTemplate.class);
            UUID id = seed(jdbc, "PENDING_INVENTORY", "now()-interval '10 minutes'");
            UUID eventId = UUID.randomUUID();
            var before = jdbc.queryForObject("SELECT status_entered_at FROM orders WHERE id=?", OffsetDateTime.class, id);
            jdbc.execute("CREATE FUNCTION fail_operational_transition() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test append failure'; END $$");
            jdbc.execute("CREATE TRIGGER fail_operational_transition BEFORE INSERT ON outbox_event FOR EACH ROW EXECUTE FUNCTION fail_operational_transition()");
            assertThatThrownBy(() -> event(app, id, "INVENTORY_RESERVED", eventId)).isInstanceOf(RuntimeException.class);
            assertThat(jdbc.queryForObject("SELECT status_entered_at FROM orders WHERE id=?", OffsetDateTime.class, id)).isEqualTo(before);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM processed_event WHERE event_id=?", Long.class, eventId)).isZero();
            jdbc.execute("DROP TRIGGER fail_operational_transition ON outbox_event");
            event(app, id, "INVENTORY_RESERVED", eventId);
            assertThat(jdbc.queryForObject("SELECT status_entered_at FROM orders WHERE id=?", OffsetDateTime.class, id)).isAfter(before);
        }
    }

    @Test
    void operatorFeatureIsAbsentWithoutExplicitProfile() throws Exception {
        try (var app = DisposableOrderRuntime.start(); var client = HttpClient.newHttpClient()) {
            assertThat(get(client, app, "", TOKEN).statusCode()).isEqualTo(404);
        }
    }

    @Test
    void operatorProfileWithoutExplicitTokenFailsStartup() {
        assertThatThrownBy(() -> DisposableOrderRuntime.start(false, "--spring.profiles.active=operations",
                "--apex.messaging.outbox.publisher.enabled=false")).hasStackTraceContaining("apex.order.operations");
    }

    @Test
    void upgradeKeepsHistoricalStateTimingUnknown() throws Exception {
        try (var db = EmbeddedPostgres.builder().start()) {
            Flyway.configure().dataSource(db.getPostgresDatabase()).locations("classpath:db/migration", "classpath:db/platform")
                    .target("2000").load().migrate();
            var jdbc = new JdbcTemplate(db.getPostgresDatabase());
            UUID id = UUID.randomUUID();
            jdbc.update("INSERT INTO orders(id,external_reference,description,status,created_at) VALUES (?,?,?,'PENDING_PAYMENT',now()-interval '1 day')",
                    id, id.toString(), "historical");
            Flyway.configure().dataSource(db.getPostgresDatabase()).locations("classpath:db/migration", "classpath:db/platform").load().migrate();
            assertThat(jdbc.queryForObject("SELECT status_entered_at FROM orders WHERE id=?", OffsetDateTime.class, id)).isNull();
            assertThat(jdbc.queryForObject("SELECT max(version::integer) FROM flyway_schema_history WHERE success", Integer.class)).isEqualTo(2002);
        }
    }
}
