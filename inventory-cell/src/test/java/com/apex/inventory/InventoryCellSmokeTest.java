package com.apex.inventory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.stream.Stream;

import com.apex.inventory.modules.inventory.application.ReserveInventoryUseCase;
import com.apex.inventory.modules.inventory.domain.ReservationItem;
import com.apex.inventory.modules.inventory.domain.ReservationRequest;
import com.apex.inventory.modules.inventory.domain.ReservationResult;
import com.apex.inventory.modules.inventory.domain.ReservationStatus;
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
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

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
import static org.junit.jupiter.api.Assertions.*;

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
    private ReserveInventoryUseCase reserveInventory;

    @Test
    void multiSkuReleaseRollsBackEveryCounterWhenOneStockRowIsInconsistent() {
        UUID a=UUID.fromString("11111111-1111-4111-8111-111111111111");
        UUID b=UUID.fromString("22222222-2222-4222-8222-222222222222");
        // Unique products for this fixture, retaining deterministic Java UUID ordering.
        a=new UUID(a.getMostSignificantBits(),UUID.randomUUID().getLeastSignificantBits());
        b=new UUID(b.getMostSignificantBits(),UUID.randomUUID().getLeastSignificantBits());
        UUID id=UUID.randomUUID();
        jdbc.update("INSERT INTO inventory_stock VALUES (?,10,0),(?,10,0)",a,b);
        reserveInventory.execute(new ReservationRequest(id,List.of(new ReservationItem(a,3),new ReservationItem(b,2))));
        var release=application.getBean(com.apex.inventory.modules.inventory.application.ReleaseInventoryUseCase.class);
        jdbc.update("UPDATE inventory_stock SET reserved_quantity=0 WHERE product_id=?",b);
        assertThatThrownBy(()->release.execute(id)).isInstanceOf(IllegalStateException.class);
        assertThat(jdbc.queryForObject("SELECT available_quantity FROM inventory_stock WHERE product_id=?",Integer.class,a)).isEqualTo(7);
        assertThat(jdbc.queryForObject("SELECT reserved_quantity FROM inventory_stock WHERE product_id=?",Integer.class,a)).isEqualTo(3);
        assertThat(jdbc.queryForObject("SELECT status FROM inventory_reservations WHERE order_id=?",String.class,id)).isEqualTo("RESERVED");
        jdbc.update("UPDATE inventory_stock SET reserved_quantity=2 WHERE product_id=?",b);
        assertThat(release.execute(id)).isTrue();assertThat(release.execute(id)).isFalse();
        assertThat(jdbc.queryForList("SELECT available_quantity FROM inventory_stock WHERE product_id IN (?,?)",Integer.class,a,b)).containsExactly(10,10);
    }

    @Test
    void concurrentReleaseAndReserveRestoreOnlyOriginalStock() throws Exception {
        UUID product=UUID.randomUUID(), first=UUID.randomUUID(), second=UUID.randomUUID();
        jdbc.update("INSERT INTO inventory_stock VALUES (?,10,0)",product);
        reserveInventory.execute(new ReservationRequest(first,List.of(new ReservationItem(product,3))));
        var release=application.getBean(com.apex.inventory.modules.inventory.application.ReleaseInventoryUseCase.class);
        try(var pool=Executors.newFixedThreadPool(3)) {
            var gate=new CountDownLatch(1);
            var a=pool.submit(()->{gate.await();return release.execute(first);});
            var b=pool.submit(()->{gate.await();return release.execute(first);});
            var c=pool.submit(()->{gate.await();return reserveInventory.execute(new ReservationRequest(second,List.of(new ReservationItem(product,2))));});
            gate.countDown();
            assertThat(List.of(a.get(15,TimeUnit.SECONDS),b.get(15,TimeUnit.SECONDS))).containsExactlyInAnyOrder(true,false);
            assertThat(c.get(15,TimeUnit.SECONDS).status()).isEqualTo(ReservationStatus.RESERVED);
        }
        assertThat(jdbc.queryForObject("SELECT available_quantity FROM inventory_stock WHERE product_id=?",Integer.class,product)).isEqualTo(8);
        assertThat(jdbc.queryForObject("SELECT reserved_quantity FROM inventory_stock WHERE product_id=?",Integer.class,product)).isEqualTo(2);
        assertThat(reserveInventory.execute(new ReservationRequest(first,List.of(new ReservationItem(product,3)))).status()).isEqualTo(ReservationStatus.RELEASED);
    }

    @Test
    void releaseRollbackAndInvalidReservationsFailClosed() {
        UUID product=UUID.randomUUID(), id=UUID.randomUUID();
        jdbc.update("INSERT INTO inventory_stock VALUES (?,10,0)",product);
        reserveInventory.execute(new ReservationRequest(id,List.of(new ReservationItem(product,3))));
        var release=application.getBean(com.apex.inventory.modules.inventory.application.ReleaseInventoryUseCase.class);
        var tx=new TransactionTemplate(application.getBean(PlatformTransactionManager.class));
        assertThatThrownBy(()->tx.execute(s->{release.execute(id);throw new IllegalStateException("test rollback");})).isInstanceOf(IllegalStateException.class);
        assertThat(jdbc.queryForObject("SELECT status FROM inventory_reservations WHERE order_id=?",String.class,id)).isEqualTo("RESERVED");
        assertThat(jdbc.queryForObject("SELECT available_quantity FROM inventory_stock WHERE product_id=?",Integer.class,product)).isEqualTo(7);
        assertThatThrownBy(()->release.execute(UUID.randomUUID())).isInstanceOf(RuntimeException.class);
        UUID rejected=UUID.randomUUID(); reserveInventory.execute(new ReservationRequest(rejected,List.of(new ReservationItem(product,100))));
        assertThatThrownBy(()->release.execute(rejected)).isInstanceOf(IllegalStateException.class);
        jdbc.update("UPDATE inventory_stock SET reserved_quantity=0 WHERE product_id=?",product);
        assertThatThrownBy(()->release.execute(id)).isInstanceOf(IllegalStateException.class);
        assertThat(jdbc.queryForObject("SELECT status FROM inventory_reservations WHERE order_id=?",String.class,id)).isEqualTo("RESERVED");
    }

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
            reserveInventory = application.getBean(ReserveInventoryUseCase.class);
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
    void bootsWithBothApexRolesButKeepsBaseListenerPaused() throws Exception {
        for (Class<?> type : List.of(ExecutionContextProvider.class, ApexHttpExecutionContextFilter.class,
                CellEvents.class, OutboxAppender.class, EventEnvelopeFactory.class, KafkaMessagePublisher.class,
                OutboxPublisher.class, OutboxPublisherScheduler.class, ProcessedEventStore.class,
                EventHandlerRegistry.class, ConfigurableKafkaEventConsumer.class)) {
            assertThat(application.getBeansOfType(type)).as(type.getSimpleName()).hasSize(1);
        }
        assertThat(application.getBean(EventHandlerRegistry.class).find("ORDER_PLACED", 1)).isPresent();
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

    @Test
    void reservesStockAndDoesNotApplyDuplicateRequest() {
        UUID productId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();

        try {
            jdbc.update("""
                    INSERT INTO inventory_stock (
                        product_id,
                        available_quantity,
                        reserved_quantity
                    )
                    VALUES (?, 10, 0)
                    """, productId);

            var request = new ReservationRequest(
                    orderId,
                    List.of(new ReservationItem(productId, 3))
            );

            var first = reserveInventory.execute(request);

            assertEquals(ReservationStatus.RESERVED, first.status());
            assertEquals(7, jdbc.queryForObject("""
                    SELECT available_quantity
                    FROM inventory_stock
                    WHERE product_id = ?
                    """, Integer.class, productId));
            assertEquals(3, jdbc.queryForObject("""
                    SELECT reserved_quantity
                    FROM inventory_stock
                    WHERE product_id = ?
                    """, Integer.class, productId));

            // Same business operation, delivered again through the transactional Spring Bean.
            var second = reserveInventory.execute(request);

            assertEquals(ReservationStatus.RESERVED, second.status());

            // Stock must not change a second time.
            assertEquals(7, jdbc.queryForObject("""
                    SELECT available_quantity
                    FROM inventory_stock
                    WHERE product_id = ?
                    """, Integer.class, productId));
            assertEquals(3, jdbc.queryForObject("""
                    SELECT reserved_quantity
                    FROM inventory_stock
                    WHERE product_id = ?
                    """, Integer.class, productId));
        } finally {
            jdbc.update("DELETE FROM inventory_reservation_items WHERE order_id = ?", orderId);
            jdbc.update("DELETE FROM inventory_reservations WHERE order_id = ?", orderId);
            jdbc.update("DELETE FROM inventory_stock WHERE product_id = ?", productId);
        }
    }


    @Test
    void concurrentOrdersMustNotOversell() throws Exception {
        UUID productId = UUID.randomUUID();
        UUID orderA = UUID.randomUUID();
        UUID orderB = UUID.randomUUID();

        jdbc.update("""
        INSERT INTO inventory_stock (
            product_id,
            available_quantity,
            reserved_quantity
        )
        VALUES (?, 10, 0)
        """, productId);

        var requestA = new ReservationRequest(
                orderA,
                List.of(new ReservationItem(productId, 7))
        );

        var requestB = new ReservationRequest(
                orderB,
                List.of(new ReservationItem(productId, 7))
        );

        var ready = new CountDownLatch(2);
        var start = new CountDownLatch(1);

        try (var executor = Executors.newFixedThreadPool(2)) {

            Callable<ReservationResult> taskA = () -> {
                ready.countDown();
                if (!start.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Start timeout");
                }
                return reserveInventory.execute(requestA);
            };

            Callable<ReservationResult> taskB = () -> {
                ready.countDown();
                if (!start.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Start timeout");
                }
                return reserveInventory.execute(requestB);
            };

            Future<ReservationResult> futureA =
                    executor.submit(taskA);

            Future<ReservationResult> futureB =
                    executor.submit(taskB);

            assertTrue(
                    ready.await(10, TimeUnit.SECONDS),
                    "Workers did not become ready"
            );

            start.countDown();

            var resultA = futureA.get(20, TimeUnit.SECONDS);
            var resultB = futureB.get(20, TimeUnit.SECONDS);

            long reservedCount = Stream.of(resultA, resultB)
                    .filter(result ->
                            result.status() == ReservationStatus.RESERVED)
                    .count();

            long rejectedCount = Stream.of(resultA, resultB)
                    .filter(result ->
                            result.status() == ReservationStatus.REJECTED)
                    .count();

            assertEquals(1, reservedCount);
            assertEquals(1, rejectedCount);
        }

        Integer available = jdbc.queryForObject("""
        SELECT available_quantity
        FROM inventory_stock
        WHERE product_id = ?
        """, Integer.class, productId);

        Integer reserved = jdbc.queryForObject("""
        SELECT reserved_quantity
        FROM inventory_stock
        WHERE product_id = ?
        """, Integer.class, productId);

        assertEquals(3, available);
        assertEquals(7, reserved);

        Integer reservationCount = jdbc.queryForObject("""
        SELECT COUNT(*)
        FROM inventory_reservations
        WHERE order_id IN (?, ?)
        """, Integer.class, orderA, orderB);

        assertEquals(2, reservationCount);
    }


    @Test
    void concurrentDuplicateRequestMustReserveOnlyOnce()
            throws Exception {

        UUID productId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();

        jdbc.update("""
        INSERT INTO inventory_stock
            (product_id, available_quantity, reserved_quantity)
        VALUES (?, 10, 0)
        """, productId);

        var request = new ReservationRequest(
                orderId,
                List.of(new ReservationItem(productId, 4))
        );

        var ready = new CountDownLatch(2);
        var start = new CountDownLatch(1);

        try (var executor = Executors.newFixedThreadPool(2)) {

            Callable<ReservationResult> task = () -> {
                ready.countDown();

                if (!start.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Start timeout");
                }

                return reserveInventory.execute(request);
            };

            Future<ReservationResult> first = executor.submit(task);
            Future<ReservationResult> second = executor.submit(task);

            try {
                assertTrue(ready.await(10, TimeUnit.SECONDS));
            } finally {
                start.countDown();
            }

            assertEquals(
                    ReservationStatus.RESERVED,
                    first.get(20, TimeUnit.SECONDS).status()
            );

            assertEquals(
                    ReservationStatus.RESERVED,
                    second.get(20, TimeUnit.SECONDS).status()
            );
        }

        assertEquals(6, jdbc.queryForObject("""
        SELECT available_quantity
        FROM inventory_stock
        WHERE product_id = ?
        """, Integer.class, productId));

        assertEquals(4, jdbc.queryForObject("""
        SELECT reserved_quantity
        FROM inventory_stock
        WHERE product_id = ?
        """, Integer.class, productId));

        assertEquals(1, jdbc.queryForObject("""
        SELECT COUNT(*)
        FROM inventory_reservations
        WHERE order_id = ?
        """, Integer.class, orderId));

        assertEquals(1, jdbc.queryForObject("""
        SELECT COUNT(*)
        FROM inventory_reservation_items
        WHERE order_id = ?
        """, Integer.class, orderId));
    }


    @Test
    void failureAfterReservationMustRollbackEverything() {

        UUID productId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();

        jdbc.update("""
        INSERT INTO inventory_stock
            (product_id, available_quantity, reserved_quantity)
        VALUES (?, 10, 0)
        """, productId);

        var request = new ReservationRequest(
                orderId,
                List.of(new ReservationItem(productId, 3))
        );

        PlatformTransactionManager transactionManager =
                application.getBean(
                        PlatformTransactionManager.class
                );

        var transaction = new TransactionTemplate(
                transactionManager
        );

        assertThrows(IllegalStateException.class, () -> {
            transaction.executeWithoutResult(status -> {

                var result = reserveInventory.execute(request);

                assertEquals(
                        ReservationStatus.RESERVED,
                        result.status()
                );

                // The update is visible inside this transaction.
                assertEquals(7, jdbc.queryForObject("""
                SELECT available_quantity
                FROM inventory_stock
                WHERE product_id = ?
                """, Integer.class, productId));

                // Simulate an unexpected failure before commit.
                throw new IllegalStateException(
                        "Simulated crash before commit"
                );
            });
        });

        // After rollback, original stock must remain unchanged.
        assertEquals(10, jdbc.queryForObject("""
        SELECT available_quantity
        FROM inventory_stock
        WHERE product_id = ?
        """, Integer.class, productId));

        assertEquals(0, jdbc.queryForObject("""
        SELECT reserved_quantity
        FROM inventory_stock
        WHERE product_id = ?
        """, Integer.class, productId));

        assertEquals(0, jdbc.queryForObject("""
        SELECT COUNT(*)
        FROM inventory_reservations
        WHERE order_id = ?
        """, Integer.class, orderId));

        assertEquals(0, jdbc.queryForObject("""
        SELECT COUNT(*)
        FROM inventory_reservation_items
        WHERE order_id = ?
        """, Integer.class, orderId));
    }



    @Test
    void realWireReservationPreservesMetadataAndSuppressesDuplicateEventEffects() {
        UUID orderId = UUID.randomUUID();
        UUID productA = UUID.fromString("11111111-1111-4111-8111-111111111111");
        UUID productB = UUID.fromString("88888888-2222-4222-8222-222222222222");
        var event = placed(orderId, List.of(new ReservationItem(productA, 2), new ReservationItem(productB, 1)));
        var replay = new com.apex.platform.events.EventEnvelope<>(
                new com.apex.platform.events.EventMetadata(UUID.randomUUID(), "ORDER_PLACED", 1,
                        event.metadata().correlationId(), null, orderId.toString(), "ORDER", "order-cell",
                        "order-cell", event.metadata().occurredAt()), event.payload());
        try {
            jdbc.update("INSERT INTO inventory_stock VALUES (?, 10, 0), (?, 5, 0)", productA, productB);
            consume(event, () -> {
                assertEquals(1L, sagaCount("processed_event", "event_id", event.metadata().eventId()));
                assertEquals(1L, sagaCount("outbox_event", "aggregate_id", orderId));
            });
            consume(event, () -> {});
            assertEquals(1L, sagaCount("outbox_event", "aggregate_id", orderId));
            consume(replay, () -> {});
            assertEquals(2L, sagaCount("outbox_event", "aggregate_id", orderId));
            assertEquals(1L, sagaCount("inventory_reservations", "order_id", orderId));
            assertEquals(List.of(8, 2), stock(productA));
            assertEquals(List.of(4, 1), stock(productB));
            var row = jdbc.queryForMap("SELECT * FROM outbox_event WHERE causation_id = ?",
                    event.metadata().eventId());
            assertEquals("INVENTORY_RESERVED", row.get("event_type"));
            assertEquals(event.metadata().correlationId(), row.get("correlation_id"));
            assertEquals("inventory-cell", row.get("source_service"));
            assertEquals("inventory-cell", row.get("cell_id"));
            assertEquals("INVENTORY_RESERVATION", row.get("aggregate_type"));
            assertThatThrownBy(() -> application.getBean(ExecutionContextProvider.class).requireCurrent())
                    .isInstanceOf(IllegalStateException.class);
        } finally {
            cleanupSaga(orderId, List.of(productA, productB), event.metadata().eventId(), replay.metadata().eventId());
        }
    }

    @Test
    void rejectedWireRequestDoesNotPartiallyDeductOtherProducts() {
        UUID orderId = UUID.randomUUID();
        UUID productA = UUID.randomUUID();
        UUID productB = UUID.randomUUID();
        var event = placed(orderId, List.of(new ReservationItem(productA, 2), new ReservationItem(productB, 100)));
        try {
            jdbc.update("INSERT INTO inventory_stock VALUES (?, 10, 0), (?, 5, 0)", productA, productB);
            consume(event, () -> {});
            assertEquals(List.of(10, 0), stock(productA));
            assertEquals(List.of(5, 0), stock(productB));
            assertEquals("REJECTED", jdbc.queryForObject(
                    "SELECT status FROM inventory_reservations WHERE order_id = ?", String.class, orderId));
            assertEquals("INVENTORY_RESERVATION_REJECTED", jdbc.queryForObject(
                    "SELECT event_type FROM outbox_event WHERE aggregate_id = ?", String.class, orderId));
        } finally {
            cleanupSaga(orderId, List.of(productA, productB), event.metadata().eventId());
        }
    }

    @Test
    void outboxFailureRollsBackInboxReservationAndStockAndDoesNotAckThenCanRetry() {
        UUID orderId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();
        var event = placed(orderId, List.of(new ReservationItem(productId, 3)));
        var ack = new java.util.concurrent.atomic.AtomicBoolean();
        try {
            jdbc.update("INSERT INTO inventory_stock VALUES (?, 10, 0)", productId);
            jdbc.execute("ALTER TABLE outbox_event ADD CONSTRAINT saga_test_failure CHECK (aggregate_id <> '"
                    + orderId + "'::uuid)");
            try {
                assertThatThrownBy(() -> consume(event, () -> ack.set(true))).isInstanceOf(RuntimeException.class);
                assertFalse(ack.get());
                assertEquals(List.of(10, 0), stock(productId));
                assertEquals(0L, sagaCount("inventory_reservations", "order_id", orderId));
                assertEquals(0L, sagaCount("processed_event", "event_id", event.metadata().eventId()));
                assertEquals(0L, sagaCount("outbox_event", "aggregate_id", orderId));
                assertThatThrownBy(() -> application.getBean(ExecutionContextProvider.class).requireCurrent())
                        .isInstanceOf(IllegalStateException.class);
            } finally {
                jdbc.execute("ALTER TABLE outbox_event DROP CONSTRAINT saga_test_failure");
            }
            consume(event, () -> ack.set(true));
            assertTrue(ack.get());
            assertEquals(List.of(7, 3), stock(productId));
        } finally {
            cleanupSaga(orderId, List.of(productId), event.metadata().eventId());
        }
    }

    @Test
    void malformedWirePayloadDoesNotAckOrRegisterInbox() {
        UUID orderId = UUID.randomUUID();
        var event = placed(orderId, List.of(new ReservationItem(UUID.randomUUID(), 1)));
        var invalid = new com.apex.platform.events.EventEnvelope<>(event.metadata(), java.util.Map.of("orderId", orderId));
        var ack = new java.util.concurrent.atomic.AtomicBoolean();
        assertThatThrownBy(() -> consume(invalid, () -> ack.set(true))).isInstanceOf(RuntimeException.class);
        assertFalse(ack.get());
        assertEquals(0L, sagaCount("processed_event", "event_id", event.metadata().eventId()));
        assertEquals(0L, sagaCount("inventory_reservations", "order_id", orderId));
        assertEquals(0L, sagaCount("outbox_event", "aggregate_id", orderId));
        // This fails inside the handler, AFTER the transactional executor has inserted Inbox.
        UUID productId = UUID.randomUUID();
        var duplicateItems = placed(orderId, List.of(
                new ReservationItem(productId, 1), new ReservationItem(productId, 2)));
        assertThatThrownBy(() -> consume(duplicateItems, () -> ack.set(true)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Duplicate product");
        assertFalse(ack.get());
        assertEquals(0L, sagaCount("processed_event", "event_id", duplicateItems.metadata().eventId()));
        assertEquals(0L, sagaCount("inventory_reservations", "order_id", orderId));
        assertEquals(0L, sagaCount("outbox_event", "aggregate_id", orderId));
    }

    @Test
    void conflictingRequestAndReleasedReservationFailClosedWithoutAck() {
        UUID orderId = UUID.randomUUID();
        UUID productId = UUID.randomUUID();
        var first = placed(orderId, List.of(new ReservationItem(productId, 3)));
        var conflict = placed(orderId, List.of(new ReservationItem(productId, 4)));
        var released = placed(orderId, List.of(new ReservationItem(productId, 3)));
        var ack = new java.util.concurrent.atomic.AtomicBoolean();
        try {
            jdbc.update("INSERT INTO inventory_stock VALUES (?, 10, 0)", productId);
            consume(first, () -> {});
            assertThatThrownBy(() -> consume(conflict, () -> ack.set(true)))
                    .isInstanceOf(com.apex.inventory.modules.inventory.domain.ReservationConflictException.class);
            jdbc.update("UPDATE inventory_reservations SET status = 'RELEASED' WHERE order_id = ?", orderId);
            assertThatThrownBy(() -> consume(released, () -> ack.set(true))).isInstanceOf(IllegalStateException.class);
            assertFalse(ack.get());
            assertEquals(List.of(7, 3), stock(productId));
            assertEquals(0L, sagaCount("processed_event", "event_id", conflict.metadata().eventId()));
            assertEquals(0L, sagaCount("processed_event", "event_id", released.metadata().eventId()));
            assertEquals(1L, sagaCount("outbox_event", "aggregate_id", orderId));
        } finally {
            cleanupSaga(orderId, List.of(productId), first.metadata().eventId(),
                    conflict.metadata().eventId(), released.metadata().eventId());
        }
    }

    @Test
    void sagaPreflightRejectsMissingOrWrongVersionHandler() {
        var registry = org.mockito.Mockito.mock(EventHandlerRegistry.class);
        org.mockito.Mockito.when(registry.find("ORDER_PLACED", 1)).thenReturn(java.util.Optional.empty());
        assertThatThrownBy(() -> new SagaSubscriptionPreflight(registry).start())
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("ORDER_PLACED V1");
        try (var context = new org.springframework.context.annotation.AnnotationConfigApplicationContext()) {
            context.getEnvironment().setActiveProfiles("saga");
            context.registerBean(EventHandlerRegistry.class, () -> registry);
            context.register(SagaSubscriptionPreflight.class);
            assertThatThrownBy(context::refresh).hasRootCauseInstanceOf(IllegalStateException.class)
                    .hasStackTraceContaining("ORDER_PLACED V1");
        }
    }

    private com.apex.platform.events.EventEnvelope<?> placed(UUID orderId, List<ReservationItem> items) {
        var metadata = new com.apex.platform.events.EventMetadata(UUID.randomUUID(), "ORDER_PLACED", 1,
                UUID.randomUUID(), null, orderId.toString(), "ORDER", "order-cell", "order-cell", java.time.Instant.now());
        return new com.apex.platform.events.EventEnvelope<>(metadata, java.util.Map.of("orderId", orderId,
                "externalReference", "saga-" + orderId, "items", items, "placedAt", java.time.Instant.now()));
    }

    private void consume(com.apex.platform.events.EventEnvelope<?> event, Runnable afterCommit) {
        var mapper = application.getBean(tools.jackson.databind.json.JsonMapper.class);
        application.getBean(ConfigurableKafkaEventConsumer.class).consume(
                new org.apache.kafka.clients.consumer.ConsumerRecord<>(ORDER_TOPIC, 0, 0,
                        event.metadata().aggregateId(), mapper.writeValueAsString(event)), afterCommit::run);
    }

    private long sagaCount(String table, String column, UUID id) {
        return jdbc.queryForObject("SELECT count(*) FROM " + table + " WHERE " + column + " = ?", Long.class, id);
    }

    private List<Integer> stock(UUID productId) {
        return jdbc.queryForObject("SELECT available_quantity, reserved_quantity FROM inventory_stock WHERE product_id = ?",
                (rs, row) -> List.of(rs.getInt(1), rs.getInt(2)), productId);
    }

    private void cleanupSaga(UUID orderId, List<UUID> products, UUID... events) {
        for (UUID eventId : events) {
            jdbc.update("DELETE FROM processed_event WHERE event_id = ?", eventId);
        }
        jdbc.update("DELETE FROM outbox_event WHERE aggregate_id = ?", orderId);
        jdbc.update("DELETE FROM inventory_reservation_items WHERE order_id = ?", orderId);
        jdbc.update("DELETE FROM inventory_reservations WHERE order_id = ?", orderId);
        for (UUID product : products) {
            jdbc.update("DELETE FROM inventory_stock WHERE product_id = ?", product);
        }
    }

    private void rejects(String sql, Object... arguments) {
        assertThatThrownBy(() -> jdbc.update(sql, arguments)).isInstanceOf(DataIntegrityViolationException.class);
    }
}
