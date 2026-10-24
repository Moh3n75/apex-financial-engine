package com.apex.inventory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

import com.apex.platform.core.context.ExecutionContextProvider;
import com.apex.platform.events.EventEnvelopeFactory;
import com.apex.platform.events.api.CellEvents;
import com.apex.platform.messaging.consumer.EventHandlerRegistry;
import com.apex.platform.messaging.consumer.ProcessedEventStore;
import com.apex.platform.messaging.jdbc.ConfigurableKafkaEventConsumer;
import com.apex.platform.messaging.jdbc.OutboxPublisherScheduler;
import com.apex.platform.messaging.kafka.KafkaMessagePublisher;
import com.apex.platform.messaging.outbox.OutboxAppender;
import com.apex.platform.messaging.outbox.OutboxPublisher;
import com.apex.platform.webmvc.ApexHttpExecutionContextFilter;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real Boot discovery and PostgreSQL migrations; never uses workstation service settings. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class InventoryCellSmokeTest {
    private static final String ORDER_TOPIC = "apex.order.events.v1";
    private static final String INVENTORY_TOPIC = "apex.inventory.events.v1";

    private EmbeddedPostgres database;
    private EmbeddedKafkaKraftBroker broker;
    private ConfigurableApplicationContext application;
    private JdbcTemplate jdbc;

    @BeforeAll
    void startDisposableRuntime() throws Exception {
        try {
            database = EmbeddedPostgres.builder().start();
            broker = new EmbeddedKafkaKraftBroker(1, 1, ORDER_TOPIC, INVENTORY_TOPIC);
            broker.brokerProperty("auto.create.topics.enable", "false");
            broker.brokerListProperty("apex.inventory.test.brokers");
            broker.afterPropertiesSet();

            StandardEnvironment environment = new StandardEnvironment();
            environment.getPropertySources().remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
            environment.getPropertySources().remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
            application = new SpringApplicationBuilder(InventoryCellApplication.class)
                    .environment(environment)
                    .run("--server.address=127.0.0.1", "--server.port=0",
                            "--spring.config.location=classpath:application.yml",
                            "--spring.config.additional-location=", "--spring.config.import=",
                            "--spring.profiles.active=", "--spring.profiles.include=",
                            "--spring.datasource.url=" + database.getJdbcUrl("postgres", "postgres"),
                            "--spring.datasource.username=postgres", "--spring.datasource.password=",
                            "--spring.kafka.bootstrap-servers=" + broker.getBrokersAsString());
            jdbc = application.getBean(JdbcTemplate.class);
        } catch (Exception | Error failure) {
            try {
                closeDisposableRuntime();
            } catch (Exception cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    @AfterAll
    void closeDisposableRuntime() throws Exception {
        try {
            if (application != null) {
                application.close();
            }
        } finally {
            try {
                if (broker != null) {
                    broker.destroy();
                }
            } finally {
                if (database != null) {
                    database.close();
                }
            }
        }
    }

    @Test
    void bootsWithBothApexRolesButDoesNotConsumeWithoutABusinessHandler() throws Exception {
        for (Class<?> type : List.of(ExecutionContextProvider.class, ApexHttpExecutionContextFilter.class,
                CellEvents.class, OutboxAppender.class, EventEnvelopeFactory.class, KafkaMessagePublisher.class,
                OutboxPublisher.class, OutboxPublisherScheduler.class, ProcessedEventStore.class,
                EventHandlerRegistry.class, ConfigurableKafkaEventConsumer.class)) {
            assertThat(application.getBeansOfType(type)).as(type.getSimpleName()).hasSize(1);
        }
        assertThat(application.getBean(EventHandlerRegistry.class).find("ORDER_PLACED", 1)).isEmpty();
        var containers = application.getBean(KafkaListenerEndpointRegistry.class).getListenerContainers();
        assertThat(containers).hasSize(1).allSatisfy(container -> {
            assertThat(container.isRunning()).isFalse();
            assertThat(container.getContainerProperties().getTopics()).containsExactly(ORDER_TOPIC);
            assertThat(container.getContainerProperties().getGroupId())
                    .isEqualTo("inventory-cell-order-placed-v1");
        });
        try (Admin admin = Admin.create(java.util.Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                broker.getBrokersAsString()))) {
            assertThat(admin.listTopics().names().get(10, TimeUnit.SECONDS))
                    .contains(ORDER_TOPIC, INVENTORY_TOPIC);
        }
        int port = ((WebServerApplicationContext) application).getWebServer().getPort();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            var response = client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/"))
                    .timeout(Duration.ofSeconds(10)).GET().build(), HttpResponse.BodyHandlers.discarding());
            // Scaffold deliberately exposes no business or administration endpoint.
            assertThat(response.statusCode()).isEqualTo(404);
        }
    }

    @Test
    void migratesPlatformAndInventoryInOrderAndCanValidateAndRestartMigrations() {
        assertThat(jdbc.queryForList("SELECT version FROM flyway_schema_history WHERE success ORDER BY installed_rank",
                String.class)).containsExactly("1001", "1002", "2000");
        assertThat(jdbc.queryForList("SELECT tablename FROM pg_tables WHERE schemaname = 'public'", String.class))
                .contains("inventory_stock", "inventory_reservations", "inventory_reservation_items",
                        "outbox_event", "processed_event")
                .doesNotContain("orders", "order_items", "customers", "customer_projection");
        assertThat(jdbc.queryForObject("SELECT data_type FROM information_schema.columns WHERE table_schema = 'public' "
                + "AND table_name = 'inventory_reservations' AND column_name = 'created_at'", String.class))
                .isEqualTo("timestamp with time zone");
        Flyway flyway = application.getBean(Flyway.class);
        flyway.validate();
        assertThat(flyway.migrate().migrationsExecuted).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_event", Long.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM processed_event", Long.class)).isZero();
    }

    @Test
    void enforcesInventoryPrimaryForeignAndCheckConstraints() {
        UUID product = UUID.randomUUID();
        UUID order = UUID.randomUUID();
        try {
            jdbc.update("INSERT INTO inventory_stock(product_id, available_quantity) VALUES (?, 0)", product);
            assertThat(jdbc.queryForObject("SELECT reserved_quantity FROM inventory_stock WHERE product_id = ?",
                    Integer.class, product)).isZero();
            rejects("INSERT INTO inventory_stock(product_id, available_quantity) VALUES (?, 1)", product);
            rejects("UPDATE inventory_stock SET available_quantity = -1 WHERE product_id = ?", product);
            rejects("UPDATE inventory_stock SET reserved_quantity = -1 WHERE product_id = ?", product);
            rejects("UPDATE inventory_stock SET available_quantity = NULL WHERE product_id = ?", product);
            for (String status : List.of("RESERVED", "REJECTED", "RELEASED")) {
                jdbc.update("INSERT INTO inventory_reservations VALUES (?, ?, CURRENT_TIMESTAMP)", order, status);
                jdbc.update("DELETE FROM inventory_reservations WHERE order_id = ?", order);
            }
            rejects("INSERT INTO inventory_reservations VALUES (?, 'PENDING', CURRENT_TIMESTAMP)", order);
            rejects("INSERT INTO inventory_reservations VALUES (?, 'RESERVED', NULL)", order);
            rejects("INSERT INTO inventory_reservation_items VALUES (?, ?, 1)", order, product);
            jdbc.update("INSERT INTO inventory_reservations VALUES (?, 'RESERVED', CURRENT_TIMESTAMP)", order);
            rejects("INSERT INTO inventory_reservations VALUES (?, 'RESERVED', CURRENT_TIMESTAMP)", order);
            rejects("INSERT INTO inventory_reservation_items VALUES (?, ?, 0)", order, product);
            rejects("INSERT INTO inventory_reservation_items VALUES (?, ?, -1)", order, product);
            jdbc.update("INSERT INTO inventory_reservation_items VALUES (?, ?, 1)", order, product);
            rejects("INSERT INTO inventory_reservation_items VALUES (?, ?, 2)", order, product);
            rejects("DELETE FROM inventory_reservations WHERE order_id = ?", order);
        } finally {
            jdbc.update("DELETE FROM inventory_reservation_items WHERE order_id = ?", order);
            jdbc.update("DELETE FROM inventory_reservations WHERE order_id = ?", order);
            jdbc.update("DELETE FROM inventory_stock WHERE product_id = ?", product);
        }
    }

    private void rejects(String sql, Object... arguments) {
        assertThatThrownBy(() -> jdbc.update(sql, arguments)).isInstanceOf(DataIntegrityViolationException.class);
    }
}
