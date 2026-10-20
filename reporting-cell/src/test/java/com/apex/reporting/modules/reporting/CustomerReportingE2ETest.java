package com.apex.reporting.modules.reporting;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

import com.apex.platform.messaging.consumer.EventHandlerRegistry;
import com.apex.platform.messaging.consumer.EventHandler;
import com.apex.platform.messaging.jdbc.ConfigurableKafkaEventConsumer;
import com.apex.platform.messaging.jdbc.SimpleTransactionalEventHandlerExecutor;
import com.apex.reporting.ReportingCellApplication;
import com.apex.reporting.modules.reporting.application.ProjectCustomerCreatedHandler;
import com.apex.reporting.modules.reporting.events.CustomerCreatedV1Payload;
import com.apex.reporting.support.M22BuildPrerequisite;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Real HTTP + packaged Customer + real Reporting Boot runtime; all brokers/databases disposable. */
class CustomerReportingE2ETest {
    private static final String TOPIC = "apex.customer.events.v1";
    private static final String GROUP = "reporting-m22-e2e-" + UUID.randomUUID();
    private static final String PERSIAN = "مشتری آزمایشی";
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    @Test
    void postScheduledPublicationRealListenerRollbackRetryAndDuplicateSuppression() throws Exception {
        Path root = Path.of(System.getProperty("m22.repositoryRoot", "..")).toAbsolutePath().normalize();
        Path customerJar = M22BuildPrerequisite.verify(root);
        EmbeddedKafkaKraftBroker broker = new EmbeddedKafkaKraftBroker(1, 1, TOPIC);
        broker.brokerProperty("auto.create.topics.enable", "false");
        // Do not overwrite another Spring test's bootstrap property in this JVM.
        broker.brokerListProperty("apex.m22.e2e.bootstrap");
        Process customer = null;
        ConfigurableApplicationContext reporting = null;
        try (EmbeddedPostgres customerDb = EmbeddedPostgres.builder().start();
             EmbeddedPostgres reportingDb = EmbeddedPostgres.builder().start()) {
            try {
            broker.afterPropertiesSet(); // Explicitly provisions TOPIC only on this disposable broker.
            String bootstrap = broker.getBrokersAsString();
            int customerPort;
            try (ServerSocket reservation = new ServerSocket(0)) { customerPort = reservation.getLocalPort(); }
            Path output = Path.of("target/m22-e2e-customer.log").toAbsolutePath();
            ProcessBuilder launch = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-jar", customerJar.toString(), "--server.port=" + customerPort,
                    "--spring.config.location=classpath:application.yml", "--spring.config.import=", "--spring.profiles.active=",
                    "--apex.messaging.outbox.publisher.interval-ms=100", "--logging.level.root=WARN");
            // Remove inherited application configuration before supplying only disposable dependencies.
            launch.environment().keySet().removeIf(key -> key.matches(
                    "(?i)^(SPRING_.*|CELL_.*|APEX_.*|KAFKA_.*|SERVER_PORT|JAVA_TOOL_OPTIONS|JDK_JAVA_OPTIONS|_JAVA_OPTIONS)$"));
            launch.environment().putAll(Map.of("KAFKA_BOOTSTRAP_SERVERS", bootstrap,
                    "APEX_CUSTOMER_EVENTS_TOPIC", TOPIC,
                    "CELL_DB_URL", customerDb.getJdbcUrl("postgres", "postgres"),
                    "CELL_DB_USERNAME", "postgres", "CELL_DB_PASSWORD", "postgres"));
            launch.directory(root.resolve("customer-cell").toFile());
            customer = launch.redirectErrorStream(true).redirectOutput(output.toFile()).start();
            Process process = customer;
            await().atMost(Duration.ofSeconds(45)).ignoreExceptions().until(() -> {
                assertThat(process.isAlive()).as("Customer process, see " + output).isTrue();
                return get(customerPort, "/actuator/health").statusCode() == 200;
            });

            try (Connection customerConnection = connect(customerDb);
                 Connection reportingConnection = connect(reportingDb);
                 Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap));
                 KafkaConsumer<String, String> observer = observer(bootstrap);
                 KafkaProducer<String, String> replay = producer(bootstrap)) {
                JdbcTemplate customers = jdbc(customerConnection);
                JdbcTemplate reports = jdbc(reportingConnection);
                TopicPartition partition = new TopicPartition(TOPIC, 0);
                observer.assign(List.of(partition));
                observer.seekToBeginning(List.of(partition));

                // Reporting has not started yet: durable publication is independent of it.
                JsonNode created = create(customerPort, "M22-E2E-ONE");
                UUID id = UUID.fromString(created.get("id").asText());
                var stored = customers.queryForMap("SELECT * FROM outbox_event WHERE aggregate_id=?", id);
                UUID eventId = (UUID) stored.get("event_id");
                assertThat(customers.queryForObject("SELECT count(*) FROM customers WHERE id=?", Long.class, id)).isEqualTo(1);
                assertThat(customers.queryForObject("SELECT count(*) FROM outbox_event WHERE aggregate_id=?", Long.class, id)).isEqualTo(1);
                var event = observe(observer, eventId);
                assertEnvelope(stored, event.value(), id);
                assertThat(event.key()).isEqualTo("CUSTOMER:" + id);
                await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                        assertThat(customers.queryForObject("SELECT published FROM outbox_event WHERE event_id=?", Boolean.class, eventId)).isTrue());

                reporting = startReporting(reportingDb, bootstrap);
                int reportingPort = ((WebServerApplicationContext) reporting).getWebServer().getPort();
                assertThat(reporting.getBeansOfType(EventHandlerRegistry.class)).hasSize(1);
                assertThat(reporting.getBeansOfType(EventHandler.class)).isEmpty();
                assertThat(reporting.getBean(ProjectCustomerCreatedHandler.class)).isNotInstanceOf(EventHandler.class);
                assertThat(reporting.getBeansOfType(ConfigurableKafkaEventConsumer.class)).hasSize(1);
                assertThat(reporting.getBeansOfType(SimpleTransactionalEventHandlerExecutor.class)).hasSize(1);
                assertThat(reporting.getBean(org.springframework.transaction.PlatformTransactionManager.class))
                        .isInstanceOf(org.springframework.jdbc.support.JdbcTransactionManager.class);
                assertThat(reporting.getBean(EventHandlerRegistry.class).find("CUSTOMER_CREATED", 1).orElseThrow().payloadType())
                        .isEqualTo(CustomerCreatedV1Payload.class);
                var containers = reporting.getBean(KafkaListenerEndpointRegistry.class).getListenerContainers();
                assertThat(containers).hasSize(1);
                assertThat(containers.iterator().next().isRunning()).isTrue();
                assertThat(reporting.getEnvironment().getProperty("spring.datasource.url"))
                        .isEqualTo(reportingDb.getJdbcUrl("postgres", "postgres"))
                        .isNotEqualTo(customerDb.getJdbcUrl("postgres", "postgres"));
                System.out.printf("M22_REPORTING_RUNTIME profiles=%s listener=%s transactionManager=%s topic=%s bootstrap=%s%n",
                        java.util.Arrays.toString(reporting.getEnvironment().getActiveProfiles()),
                        ConfigurableKafkaEventConsumer.class.getName(),
                        reporting.getBean(org.springframework.transaction.PlatformTransactionManager.class).getClass().getName(),
                        reporting.getEnvironment().getProperty("apex.messaging.consumer.topics"), bootstrap);
                await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                    var response = get(reportingPort, "/api/v1/reports/customers/" + id);
                    assertThat(response.statusCode()).isEqualTo(200);
                    assertThat(JSON.readTree(response.body()).get("displayName").asText()).isEqualTo(PERSIAN);
                });
                assertThat(reports.queryForObject("SELECT count(*) FROM processed_event WHERE event_id=?", Long.class, eventId)).isEqualTo(1);
                Object projectedAt = reports.queryForObject("SELECT projected_at FROM customer_summary WHERE customer_id=?", Object.class, id);
                var duplicate = replay.send(new ProducerRecord<>(TOPIC, event.key(), event.value())).get(10, TimeUnit.SECONDS);
                awaitCommitted(admin, partition, duplicate.offset() + 1);
                assertThat(reports.queryForObject("SELECT count(*) FROM customer_summary WHERE customer_id=?", Long.class, id)).isEqualTo(1);
                assertThat(reports.queryForObject("SELECT count(*) FROM processed_event WHERE event_id=?", Long.class, eventId)).isEqualTo(1);
                assertThat(reports.queryForObject("SELECT projected_at FROM customer_summary WHERE customer_id=?", Object.class, id)).isEqualTo(projectedAt);

                // Test-only DB trigger: one real repository failure, then a lock before projection commit.
                reports.execute("CREATE SEQUENCE m22_projection_attempt");
                reports.execute("""
                        CREATE FUNCTION m22_projection_gate() RETURNS trigger LANGUAGE plpgsql AS $$
                        BEGIN
                          IF NEW.external_reference='M22-E2E-RETRY' THEN
                            IF nextval('m22_projection_attempt')=1 THEN
                              RAISE EXCEPTION 'isolated projection failure';
                            END IF;
                            PERFORM pg_advisory_xact_lock(7222026);
                          END IF;
                          RETURN NEW;
                        END $$
                        """);
                reports.execute("CREATE TRIGGER m22_projection_gate BEFORE INSERT ON customer_summary FOR EACH ROW EXECUTE FUNCTION m22_projection_gate()");
                reports.execute("SELECT pg_advisory_lock(7222026)");
                UUID retryId = UUID.fromString(create(customerPort, "M22-E2E-RETRY").get("id").asText());
                var retryStored = customers.queryForMap("SELECT * FROM outbox_event WHERE aggregate_id=?", retryId);
                UUID retryEventId = (UUID) retryStored.get("event_id");
                var retryEvent = observe(observer, retryEventId);
                assertEnvelope(retryStored, retryEvent.value(), retryId);
                try {
                    await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                            assertThat(reports.queryForObject("SELECT last_value FROM m22_projection_attempt", Long.class)).isGreaterThanOrEqualTo(2));
                    // First attempt rolled back; retry's inbox INSERT is still uncommitted behind the gate.
                    assertThat(reports.queryForObject("SELECT count(*) FROM processed_event WHERE event_id=?", Long.class, retryEventId)).isZero();
                    assertThat(reports.queryForObject("SELECT count(*) FROM customer_summary WHERE customer_id=?", Long.class, retryId)).isZero();
                    assertThat(committed(admin, partition)).isLessThanOrEqualTo(retryEvent.offset());
                } finally {
                    reports.execute("SELECT pg_advisory_unlock(7222026)");
                }
                awaitCommitted(admin, partition, retryEvent.offset() + 1);
                assertThat(reports.queryForObject("SELECT count(*) FROM processed_event WHERE event_id=?", Long.class, retryEventId)).isEqualTo(1);
                assertThat(reports.queryForObject("SELECT display_name FROM customer_summary WHERE customer_id=?", String.class, retryId)).isEqualTo(PERSIAN);

                // Stop Customer and its entire DB before replay: Reporting has no Customer DB dependency.
                stop(customer);
                customer = null;
                customerConnection.close();
                customerDb.close();
                var independent = replay.send(new ProducerRecord<>(TOPIC, retryEvent.key(), retryEvent.value())).get(10, TimeUnit.SECONDS);
                awaitCommitted(admin, partition, independent.offset() + 1);
                assertThat(get(reportingPort, "/api/v1/reports/customers/" + retryId).statusCode()).isEqualTo(200);
                assertThat(reports.queryForObject("SELECT count(*) FROM processed_event WHERE event_id=?", Long.class, retryEventId)).isEqualTo(1);
                // A NEW valid event must also execute the business handler with Customer DB offline.
                UUID independentId = UUID.randomUUID();
                UUID independentEventId = UUID.randomUUID();
                var independentEnvelope = (tools.jackson.databind.node.ObjectNode) JSON.readTree(event.value());
                var metadata = (tools.jackson.databind.node.ObjectNode) independentEnvelope.get("metadata");
                metadata.put("eventId", independentEventId.toString());
                metadata.put("aggregateId", independentId.toString());
                var payload = (tools.jackson.databind.node.ObjectNode) independentEnvelope.get("payload");
                payload.put("customerId", independentId.toString());
                payload.put("externalReference", "M22-E2E-INDEPENDENT");
                var newDelivery = replay.send(new ProducerRecord<>(TOPIC, "CUSTOMER:" + independentId,
                        JSON.writeValueAsString(independentEnvelope))).get(10, TimeUnit.SECONDS);
                awaitCommitted(admin, partition, newDelivery.offset() + 1);
                assertThat(get(reportingPort, "/api/v1/reports/customers/" + independentId).statusCode()).isEqualTo(200);
                assertThat(reports.queryForObject("SELECT display_name FROM customer_summary WHERE customer_id=?",
                        String.class, independentId)).isEqualTo(PERSIAN);
                System.out.printf("M22_E2E_PASS topic=%s group=%s bootstrap=%s eventId=%s correlationId=%s retryEventId=%s%n",
                        TOPIC, GROUP, bootstrap, eventId, stored.get("correlation_id"), retryEventId);
                reporting.close();
                reporting = null;
            }
            } finally {
                try {
                    if (reporting != null) reporting.close();
                } finally {
                    if (customer != null) stop(customer);
                }
            }
        } finally {
            broker.destroy();
        }
    }

    private static ConfigurableApplicationContext startReporting(EmbeddedPostgres database, String bootstrap) {
        var environment = new StandardEnvironment();
        environment.getPropertySources().remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
        environment.getPropertySources().remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
        return new SpringApplicationBuilder(ReportingCellApplication.class).environment(environment).run(
                "--spring.config.location=classpath:application.yml", "--spring.config.import=", "--spring.profiles.active=",
                "--server.port=0", "--spring.datasource.url=" + database.getJdbcUrl("postgres", "postgres"),
                "--spring.datasource.username=postgres", "--spring.datasource.password=postgres",
                "--spring.kafka.bootstrap-servers=" + bootstrap, "--spring.kafka.consumer.group-id=" + GROUP,
                "--apex.messaging.consumer.topics=" + TOPIC, "--apex.messaging.publisher.topic=" + TOPIC,
                "--apex.messaging.cell.enabled=true", "--apex.messaging.consumer.enabled=true",
                "--apex.messaging.outbox.publisher.enabled=false", "--spring.kafka.consumer.properties.allow.auto.create.topics=false",
                "--logging.level.root=WARN");
    }

    private static Connection connect(EmbeddedPostgres postgres) throws Exception {
        return DriverManager.getConnection(postgres.getJdbcUrl("postgres", "postgres"), "postgres", "postgres");
    }
    private static JdbcTemplate jdbc(Connection connection) {
        return new JdbcTemplate(new SingleConnectionDataSource(connection, true));
    }
    private static HttpResponse<String> get(int port, String path) throws Exception {
        return HTTP.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(3)).GET().build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }
    private static JsonNode create(int port, String reference) throws Exception {
        String body = JSON.writeValueAsString(Map.of("externalReference", reference, "displayName", PERSIAN));
        var response = HTTP.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/v1/customers"))
                .timeout(Duration.ofSeconds(5)).header("Content-Type", "application/json; charset=utf-8")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build(),
                HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        assertThat(response.statusCode()).isEqualTo(201);
        return JSON.readTree(response.body());
    }
    private static KafkaConsumer<String, String> observer(String bootstrap) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        return new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer());
    }
    private static KafkaProducer<String, String> producer(String bootstrap) {
        return new KafkaProducer<>(Map.of("bootstrap.servers", bootstrap, "acks", "all"), new StringSerializer(), new StringSerializer());
    }
    private static org.apache.kafka.clients.consumer.ConsumerRecord<String, String> observe(KafkaConsumer<String, String> consumer, UUID eventId) {
        Instant deadline = Instant.now().plusSeconds(20);
        while (Instant.now().isBefore(deadline)) {
            for (var record : consumer.poll(Duration.ofMillis(200))) {
                if (JSON.readTree(record.value()).get("metadata").get("eventId").asText().equals(eventId.toString())) return record;
            }
        }
        throw new AssertionError("Scheduled Kafka event not observed: " + eventId);
    }
    private static void assertEnvelope(Map<String, Object> stored, String message, UUID id) {
        JsonNode envelope = JSON.readTree(message);
        JsonNode metadata = envelope.get("metadata");
        String[][] fields = {{"eventId","event_id"},{"eventType","event_type"},{"eventVersion","event_version"},
                {"correlationId","correlation_id"},{"aggregateId","aggregate_id"},{"aggregateType","aggregate_type"},
                {"sourceService","source_service"},{"cellId","cell_id"}};
        for (var field : fields) assertThat(metadata.get(field[0]).asText()).as(field[0]).isEqualTo(stored.get(field[1]).toString());
        assertThat(Instant.parse(metadata.get("occurredAt").asText())).isEqualTo(((java.sql.Timestamp) stored.get("occurred_at")).toInstant());
        assertThat(metadata.get("causationId").isNull()).isTrue();
        assertThat(stored.get("causation_id")).isNull();
        assertThat(envelope.get("payload")).isEqualTo(JSON.readTree(stored.get("payload").toString()));
        assertThat(envelope.get("payload").get("customerId").asText()).isEqualTo(id.toString());
        assertThat(envelope.get("payload").get("displayName").asText()).isEqualTo(PERSIAN);
    }
    private static long committed(Admin admin, TopicPartition partition) throws Exception {
        var offsets = admin.listConsumerGroupOffsets(GROUP).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
        return offsets.containsKey(partition) ? offsets.get(partition).offset() : -1;
    }
    private static void awaitCommitted(Admin admin, TopicPartition partition, long offset) {
        await().atMost(Duration.ofSeconds(20)).ignoreExceptions().until(() -> committed(admin, partition) >= offset);
    }
    private static void stop(Process process) throws Exception {
        process.destroy();
        if (!process.waitFor(10, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            assertThat(process.waitFor(10, TimeUnit.SECONDS)).isTrue();
        }
    }
}
