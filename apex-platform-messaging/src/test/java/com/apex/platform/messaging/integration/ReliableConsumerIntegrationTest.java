package com.apex.platform.messaging.integration;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.apex.platform.events.EventEnvelope;
import com.apex.platform.events.EventMetadata;
import com.apex.platform.events.IntegrationEvent;
import com.apex.platform.events.api.CellEvents;
import com.apex.platform.events.api.EventContracts;
import com.apex.platform.messaging.consumer.*;
import com.apex.platform.messaging.jdbc.*;
import com.apex.platform.messaging.kafka.KafkaMessagePublisher;
import com.apex.platform.messaging.outbox.OutboxPublisher;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/** Independent Kafka Admin offset checks and real PostgreSQL transactions, no shared service configuration. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReliableConsumerIntegrationTest {
    private EmbeddedPostgres database;
    private EmbeddedKafkaKraftBroker broker;
    private ConfigurableApplicationContext application;
    private Admin admin;
    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> dlt;
    private JdbcTemplate jdbc;
    private String topic;
    private String group;
    private final JsonMapper json = JsonMapper.builder().build();

    @BeforeAll
    void resources() throws Exception {
        try {
            database = EmbeddedPostgres.builder().start();
            broker = new EmbeddedKafkaKraftBroker(1, 2);
            broker.brokerProperty("auto.create.topics.enable", "false");
            broker.brokerListProperty("apex.m268.test.brokers");
            broker.afterPropertiesSet();
            admin = Admin.create(Map.of("bootstrap.servers", broker.getBrokersAsString()));
            producer = new KafkaProducer<>(Map.of("bootstrap.servers", broker.getBrokersAsString(), "acks", "all",
                    "key.serializer", StringSerializer.class, "value.serializer", StringSerializer.class));
            jdbc = new JdbcTemplate(database.getPostgresDatabase());
            // Apply existing Platform DDL only in this disposable fixture. No extra Boot
            // Flyway auto-configuration dependency is added to unrelated mocked-DB tests.
            new org.springframework.jdbc.datasource.init.ResourceDatabasePopulator(
                    new org.springframework.core.io.ClassPathResource("db/platform/V1001__platform_outbox_event.sql"),
                    new org.springframework.core.io.ClassPathResource("db/platform/V1002__platform_processed_event.sql"))
                    .execute(database.getPostgresDatabase());
        } catch (Exception | Error failure) {
            try { closeResources(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    @AfterAll
    void closeResources() throws Exception {
        try {
            if (producer != null) { producer.close(Duration.ofSeconds(3)); }
            if (admin != null) { admin.close(Duration.ofSeconds(3)); }
        } finally {
            try { if (broker != null) { broker.destroy(); } }
            finally { if (database != null) { database.close(); } }
        }
    }

    @AfterEach
    void closeApplication() {
        try { if (application != null) { application.close(); application = null; } }
        finally { if (dlt != null) { dlt.close(Duration.ofSeconds(3)); dlt = null; } }
    }

    private void start(boolean createDlt, String... extra) throws Exception {
        topic = "apex.m268." + UUID.randomUUID();
        group = "m268-" + UUID.randomUUID();
        List<NewTopic> topics = new ArrayList<>(List.of(new NewTopic(topic, 2, (short) 1)));
        if (createDlt) { topics.add(new NewTopic(topic + "-dlt", 2, (short) 1)); }
        admin.createTopics(topics).all().get(10, TimeUnit.SECONDS);
        boot(extra);
        dlt = new KafkaConsumer<>(Map.of("bootstrap.servers", broker.getBrokersAsString(),
                "group.id", "observer-" + UUID.randomUUID(), "enable.auto.commit", false,
                "allow.auto.create.topics", false, "key.deserializer", StringDeserializer.class,
                "value.deserializer", StringDeserializer.class));
        if (createDlt) {
            dlt.assign(List.of(new TopicPartition(topic + "-dlt", 0), new TopicPartition(topic + "-dlt", 1)));
            dlt.seekToBeginning(dlt.assignment());
        }
    }

    private void boot(String... extra) {
        var environment = new StandardEnvironment();
        environment.getPropertySources().remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
        environment.getPropertySources().remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
        List<String> args = new ArrayList<>(List.of("--spring.config.location=optional:classpath:/m268-no-config.yml",
                "--spring.config.additional-location=", "--spring.config.import=", "--spring.profiles.active=",
                "--spring.profiles.include=", "--spring.application.name=m268-test", "--apex.cell.id=m268-test",
                "--spring.datasource.url=" + database.getJdbcUrl("postgres", "postgres"),
                "--spring.datasource.username=postgres", "--spring.datasource.password=",
                "--spring.kafka.bootstrap-servers=" + broker.getBrokersAsString(),
                "--spring.kafka.admin.auto-create=false", "--spring.kafka.consumer.group-id=" + group,
                "--spring.kafka.consumer.enable-auto-commit=false", "--spring.kafka.consumer.auto-offset-reset=earliest",
                "--spring.kafka.consumer.properties.allow.auto.create.topics=false",
                "--spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                "--spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                "--spring.kafka.listener.ack-mode=manual_immediate", "--spring.kafka.listener.poll-timeout=100ms",
                "--spring.kafka.listener.auto-startup=true", "--apex.messaging.cell.enabled=true",
                "--apex.messaging.consumer.enabled=true", "--apex.messaging.consumer.topics=" + topic,
                "--apex.messaging.outbox.publisher.enabled=false", "--apex.messaging.consumer.reliability.enabled=true",
                "--apex.messaging.consumer.reliability.max-attempts=3",
                "--apex.messaging.consumer.reliability.retry-delay=100ms",
                "--apex.messaging.consumer.reliability.send-timeout=1s",
                "--apex.messaging.consumer.reliability.preflight-timeout=2s"));
        for (String override : extra) {
            String name = override.substring(0, override.indexOf('=') + 1);
            args.removeIf(arg -> arg.startsWith(name));
            args.add(override);
        }
        application = new SpringApplicationBuilder(TestApplication.class).environment(environment)
                .web(WebApplicationType.NONE).run(args.toArray(String[]::new));
        jdbc.execute("CREATE TABLE IF NOT EXISTS reliability_probe(id UUID PRIMARY KEY)");
    }

    @Test
    void routedScheduledPublisherConfirmsBrokerBeforeMarkingOutbox() throws Exception {
        start(true); application.close(); application=null;
        String destination=topic+".results";
        admin.createTopics(List.of(new NewTopic(destination,2,(short)1))).all().get(10,TimeUnit.SECONDS);
        boot("--apex.messaging.outbox.publisher.enabled=true","--apex.messaging.publisher.topic=unused-legacy",
                "--apex.messaging.publisher.routing.enabled=true",
                "--apex.messaging.publisher.routing.routes[0].event-type=PROBE_RESULT",
                "--apex.messaging.publisher.routing.routes[0].event-version=1",
                "--apex.messaging.publisher.routing.routes[0].topic="+destination,
                "--apex.messaging.outbox.publisher.interval-ms=100");
        UUID id=UUID.randomUUID(); send(id,"OK","PROBE",1);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(()->assertThat(jdbc.queryForObject(
                "SELECT published FROM outbox_event WHERE aggregate_id=?",Boolean.class,id)).isTrue());
        dlt.assign(List.of(new TopicPartition(destination,0),new TopicPartition(destination,1)));dlt.seekToBeginning(dlt.assignment());
        var received=quarantine("PROBE:"+id);
        assertThat(json.readTree(received.value()).path("metadata").path("eventType").asText()).isEqualTo("PROBE_RESULT");
        application.close();application=null;
        boot("--apex.messaging.outbox.publisher.enabled=true","--apex.messaging.publisher.topic=unused-legacy",
                "--apex.messaging.publisher.routing.enabled=true",
                "--apex.messaging.publisher.routing.routes[0].event-type=OTHER",
                "--apex.messaging.publisher.routing.routes[0].event-version=1",
                "--apex.messaging.publisher.routing.routes[0].topic="+destination,
                "--apex.messaging.outbox.publisher.interval-ms=100");
        UUID missing=UUID.randomUUID();send(missing,"OK","PROBE",1);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(()->assertThat(jdbc.queryForObject(
                "SELECT retry_count FROM outbox_event WHERE aggregate_id=?",Integer.class,missing)).isGreaterThan(0));
        assertThat(jdbc.queryForObject("SELECT published FROM outbox_event WHERE aggregate_id=?",Boolean.class,missing)).isFalse();
        application.close(); application=null;
        String absent=topic+".unprovisioned";
        boot("--apex.messaging.outbox.publisher.enabled=true","--apex.messaging.publisher.topic=unused-legacy",
                "--apex.messaging.publisher.routing.enabled=true",
                "--apex.messaging.publisher.routing.routes[0].event-type=PROBE_RESULT",
                "--apex.messaging.publisher.routing.routes[0].event-version=1",
                "--apex.messaging.publisher.routing.routes[0].topic="+absent,
                "--spring.kafka.producer.properties.max.block.ms=500",
                "--spring.kafka.producer.properties.request.timeout.ms=500",
                "--spring.kafka.producer.properties.delivery.timeout.ms=500",
                "--spring.kafka.producer.properties.linger.ms=0",
                "--apex.messaging.outbox.publisher.interval-ms=100");
        UUID unavailable=UUID.randomUUID();send(unavailable,"OK","PROBE",1);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(()->assertThat(jdbc.queryForObject(
                "SELECT retry_count FROM outbox_event WHERE aggregate_id=?",Integer.class,unavailable)).isGreaterThan(0));
        assertThat(jdbc.queryForObject("SELECT published FROM outbox_event WHERE aggregate_id=?",Boolean.class,unavailable)).isFalse();
        assertThat(admin.listTopics().names().get(10,TimeUnit.SECONDS)).doesNotContain(absent);
    }

    @Test
    void transientRetryCommitsOneBusinessEffectAndNoDlt() throws Exception {
        start(true);
        UUID id = UUID.randomUUID();
        var sent = send(id, "TRANSIENT", "PROBE", 1);
        committed(sent.partition(), sent.offset());
        assertThat(handler().attempts.get(id).get()).isEqualTo(3);
        assertThat(count("reliability_probe", "id", id)).isEqualTo(1);
        assertThat(count("processed_event", "event_id", id)).isEqualTo(1);
        assertThat(count("outbox_event", "aggregate_id", id)).isEqualTo(1);
        assertThat(dlt.poll(Duration.ofMillis(300))).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"PERMANENT", "ALTERNATING"})
    void permanentFailureRetriesThenStoresRawDltBeforeSourceOffsetCommits(String mode) throws Exception {
        start(true);
        UUID id = UUID.randomUUID();
        String raw = envelope(id, mode, "PROBE", 1);
        var sent = producer.send(new ProducerRecord<>(topic, 1, id.toString(), raw)).get(10, TimeUnit.SECONDS);
        ConsumerRecord<String, String> quarantined = quarantine(id.toString());
        assertDlt(quarantined, raw, sent.partition(), sent.offset());
        committed(sent.partition(), sent.offset());
        assertThat(handler().attempts.get(id).get()).isEqualTo(3);
        rolledBack(id);
        // Restart with the same group: quarantine persists, no automatic replay or another business attempt.
        application.close(); application = null;
        boot();
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            var containers = application.getBean(KafkaListenerEndpointRegistry.class).getListenerContainers();
            assertThat(containers).hasSize(1).allSatisfy(container -> {
                assertThat(container.getContainerProperties().getTopics()).containsExactly(topic);
                assertThat(container.getAssignedPartitions()).contains(new TopicPartition(topic, 1));
            });
        });
        var recoveredOffsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                .get(10, TimeUnit.SECONDS);
        assertThat(recoveredOffsets.get(new TopicPartition(topic, 1)).offset()).isEqualTo(sent.offset() + 1);
        assertThat(handler().attempts).doesNotContainKey(id);
        dlt.seekToBeginning(dlt.assignment());
        assertDlt(quarantine(id.toString()), raw, sent.partition(), sent.offset());
    }

    @ParameterizedTest
    @ValueSource(strings = {"UNKNOWN", "VERSION", "JSON", "PAYLOAD", "NULL"})
    void permanentContractFailuresQuarantineWithoutBusinessMutation(String kind) throws Exception {
        if (kind.equals("UNKNOWN")) {
            // Weak general-producer settings must not weaken the private, confirmed DLT sender.
            start(true, "--spring.kafka.producer.acks=0", "--spring.kafka.producer.retries=0",
                    "--spring.kafka.producer.properties.enable.idempotence=false");
        } else {
            start(true);
        }
        UUID id = UUID.randomUUID();
        String raw = switch (kind) {
            case "JSON" -> "{not-json";
            case "NULL" -> "null";
            case "PAYLOAD" -> json.writeValueAsString(new EventEnvelope<>(metadata(id, "PROBE", 1), Map.of()));
            case "VERSION" -> envelope(id, "OK", "PROBE", 9);
            default -> envelope(id, "OK", "UNRELATED", 1);
        };
        var sent = producer.send(new ProducerRecord<>(topic, 1, id.toString(), raw)).get(10, TimeUnit.SECONDS);
        var quarantined = quarantine(id.toString());
        assertDlt(quarantined, raw, sent.partition(), sent.offset());
        String exception = text(quarantined, KafkaHeaders.DLT_EXCEPTION_FQCN);
        assertThat(exception).contains(kind.equals("UNKNOWN") || kind.equals("VERSION")
                ? "UnsupportedEventContractException" : "MalformedEventException");
        committed(sent.partition(), sent.offset());
        assertThat(handler().attempts).doesNotContainKey(id);
        rolledBack(id);
    }

    @Test
    void exactMultiTopicSubscriptionSharesInboxAndReliableReplayDlt() throws Exception {
        start(true);
        application.close(); application = null;
        String replay = topic + ".replay";
        admin.createTopics(List.of(new NewTopic(replay, 2, (short) 1), new NewTopic(replay + "-dlt", 2, (short) 1)))
                .all().get(10, TimeUnit.SECONDS);
        boot("--apex.messaging.consumer.topics= " + topic + " , " + replay + " ");
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> application.getBean(KafkaListenerEndpointRegistry.class)
                .getListenerContainers().forEach(container -> {
                    assertThat(container.getContainerProperties().getTopics()).containsExactly(topic, replay);
                    assertThat(container.getAssignedPartitions()).contains(new TopicPartition(replay, 1), new TopicPartition(topic, 1));
                }));
        UUID id = UUID.randomUUID();
        var source = send(id, "OK", "PROBE", 1); committed(1, source.offset());
        var duplicate = producer.send(new ProducerRecord<>(replay, 1, id.toString(), envelope(id, "OK", "PROBE", 1))).get(10, TimeUnit.SECONDS);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
            assertThat(offsets.get(new TopicPartition(replay, 1)).offset()).isGreaterThan(duplicate.offset());
        });
        assertThat(count("reliability_probe", "id", id)).isEqualTo(1);
        assertThat(handler().attempts.get(id).get()).isEqualTo(1);
        UUID failure = UUID.randomUUID(); String raw = envelope(failure, "PERMANENT", "PROBE", 1);
        var failed = producer.send(new ProducerRecord<>(replay, 1, failure.toString(), raw)).get(10, TimeUnit.SECONDS);
        dlt.assign(List.of(new TopicPartition(replay + "-dlt", 1))); dlt.seekToBeginning(dlt.assignment());
        var quarantined = quarantine(failure.toString());
        assertThat(quarantined.value()).isEqualTo(raw);
        assertThat(text(quarantined, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo(replay);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(admin.listConsumerGroupOffsets(group)
                .partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS).get(new TopicPartition(replay, 1)).offset())
                .isGreaterThan(failed.offset()));
        rolledBack(failure);
    }

    @Test
    void secondSubscriptionWithoutDltFailsBeforeListenerStarts() throws Exception {
        start(true); application.close(); application = null;
        String replay = topic + ".missing-dlt";
        admin.createTopics(List.of(new NewTopic(replay, 2, (short) 1))).all().get(10, TimeUnit.SECONDS);
        assertThatThrownBy(() -> boot("--apex.messaging.consumer.topics=" + topic + "," + replay))
                .hasStackTraceContaining("provisioning preflight failed");
    }

    @Test
    void missingDltCannotCommitSourceAndRecoversAfterExplicitProvisioning() throws Exception {
        start(false, "--apex.messaging.consumer.reliability.preflight-enabled=false");
        UUID id = UUID.randomUUID();
        var sent = send(id, "PERMANENT", "PROBE", 1);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(handler().attempts).containsKey(id);
            assertThat(handler().attempts.get(id).get()).isGreaterThanOrEqualTo(4);
        });
        // A fourth handler attempt proves at least one exhausted recovery failed and the record redelivered.
        var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
        var original = offsets.get(new TopicPartition(topic, sent.partition()));
        assertThat(original == null || original.offset() <= sent.offset()).isTrue();
        rolledBack(id);
        assertThat(admin.listTopics().names().get(10, TimeUnit.SECONDS)).doesNotContain(topic + "-dlt");
        admin.createTopics(List.of(new NewTopic(topic + "-dlt", 2, (short) 1))).all().get(10, TimeUnit.SECONDS);
        dlt.assign(List.of(new TopicPartition(topic + "-dlt", 0), new TopicPartition(topic + "-dlt", 1)));
        dlt.seekToBeginning(dlt.assignment());
        assertDlt(quarantine(id.toString()), envelope(id, "PERMANENT", "PROBE", 1), sent.partition(), sent.offset());
        committed(sent.partition(), sent.offset());
        rolledBack(id);
    }

    @Test
    void deferredCommitFailureRollsBackInboxBusinessOutboxAndQuarantines() throws Exception {
        start(true);
        UUID id = UUID.randomUUID();
        jdbc.execute("CREATE FUNCTION m268_fail_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.event_id='"
                + id + "'::uuid THEN RAISE EXCEPTION 'private business details'; END IF; RETURN NEW; END $$");
        jdbc.execute("CREATE CONSTRAINT TRIGGER m268_fail_commit AFTER INSERT ON processed_event "
                + "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION m268_fail_commit()");
        try {
            var sent = send(id, "OK", "PROBE", 1);
            quarantine(id.toString());
            committed(sent.partition(), sent.offset());
            assertThat(handler().attempts.get(id).get()).isEqualTo(3);
            rolledBack(id);
        } finally {
            jdbc.execute("DROP TRIGGER m268_fail_commit ON processed_event");
            jdbc.execute("DROP FUNCTION m268_fail_commit()");
        }
    }

    @Test
    void consumerOnlyHasPrivateDltSenderAndNoOutboxPublisherOrGlobalErrorHandler() throws Exception {
        start(true);
        assertThat(application.getBeansOfType(ReliableKafkaConsumerRuntime.class)).hasSize(1);
        assertThat(application.getBeansOfType(KafkaMessagePublisher.class)).isEmpty();
        assertThat(application.getBeansOfType(OutboxPublisher.class)).isEmpty();
        assertThat(application.getBeansOfType(CommonErrorHandler.class)).isEmpty();
        assertThat(application.containsBean("kafkaListenerContainerFactory")).isTrue();
        assertThat(application.containsBean("apexReliableKafkaListenerContainerFactory")).isTrue();
        UUID id = UUID.randomUUID();
        var sent = send(id, "OK", "PROBE", 1);
        committed(sent.partition(), sent.offset());
        assertThat(count("reliability_probe", "id", id)).isEqualTo(1);
        producer.send(new ProducerRecord<>(topic, sent.partition(), id.toString(), envelope(id, "OK", "PROBE", 1)))
                .get(10, TimeUnit.SECONDS);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
            assertThat(offsets.get(new TopicPartition(topic, sent.partition())).offset()).isEqualTo(sent.offset() + 2);
        });
        assertThat(handler().attempts.get(id).get()).isEqualTo(1);
        assertThat(count("outbox_event", "aggregate_id", id)).isEqualTo(1);
    }

    @Test
    void provisioningPreflightFailsBeforeConsumptionWhenDltMissing() throws Exception {
        topic = "apex.m268." + UUID.randomUUID();
        group = "m268-" + UUID.randomUUID();
        admin.createTopics(List.of(new NewTopic(topic, 2, (short) 1))).all().get(10, TimeUnit.SECONDS);
        assertThatThrownBy(this::boot).hasStackTraceContaining("provisioning preflight failed");
        assertThat(admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS))
                .isEmpty();
    }

    @Test
    void preflightRejectsInsufficientDltPartitionsBeforeConsumption() throws Exception {
        topic = "apex.m268." + UUID.randomUUID();
        group = "m268-" + UUID.randomUUID();
        admin.createTopics(List.of(new NewTopic(topic, 2, (short) 1), new NewTopic(topic + "-dlt", 1, (short) 1)))
                .all().get(10, TimeUnit.SECONDS);
        assertThatThrownBy(this::boot).hasStackTraceContaining("DLT must have at least as many partitions");
        assertThat(admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS))
                .isEmpty();
    }

    private org.apache.kafka.clients.producer.RecordMetadata send(UUID id, String mode, String type, int version) throws Exception {
        return producer.send(new ProducerRecord<>(topic, 1, id.toString(), envelope(id, mode, type, version)))
                .get(10, TimeUnit.SECONDS);
    }

    private String envelope(UUID id, String mode, String type, int version) {
        return json.writeValueAsString(new EventEnvelope<>(metadata(id, type, version), new Probe(id, mode)));
    }

    private EventMetadata metadata(UUID id, String type, int version) {
        return new EventMetadata(id, type, version, id, null, id.toString(), "PROBE", "test", "test", Instant.EPOCH);
    }

    private ProbeHandler handler() { return application.getBean(ProbeHandler.class); }

    private long count(String table, String column, UUID id) {
        return jdbc.queryForObject("SELECT count(*) FROM " + table + " WHERE " + column + "=?", Long.class, id);
    }

    private void rolledBack(UUID id) {
        assertThat(count("reliability_probe", "id", id)).isZero();
        assertThat(count("processed_event", "event_id", id)).isZero();
        assertThat(count("outbox_event", "aggregate_id", id)).isZero();
    }

    private void committed(int partition, long offset) {
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
            assertThat(offsets.get(new TopicPartition(topic, partition))).isNotNull();
            assertThat(offsets.get(new TopicPartition(topic, partition)).offset()).isEqualTo(offset + 1);
        });
    }

    private ConsumerRecord<String, String> quarantine(String key) {
        var result = new ArrayList<ConsumerRecord<String, String>>();
        await().atMost(Duration.ofSeconds(25)).until(() -> {
            for (var record : dlt.poll(Duration.ofMillis(100))) {
                if (key.equals(record.key())) { result.add(record); }
            }
            return !result.isEmpty();
        });
        return result.getFirst();
    }

    private void assertDlt(ConsumerRecord<String, String> record, String raw, int partition, long offset) {
        assertThat(record.topic()).isEqualTo(topic + "-dlt");
        assertThat(record.partition()).isEqualTo(partition);
        assertThat(record.value()).isEqualTo(raw);
        assertThat(text(record, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo(topic);
        assertThat(text(record, KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP)).isEqualTo(group);
        assertThat(ByteBuffer.wrap(record.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION).value()).getInt())
                .isEqualTo(partition);
        assertThat(ByteBuffer.wrap(record.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET).value()).getLong())
                .isEqualTo(offset);
        assertThat(text(record, KafkaHeaders.DLT_EXCEPTION_FQCN)).isNotBlank();
        assertThat(text(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE)).doesNotContain("private business details");
        assertThat(record.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_STACKTRACE)).isNull();
    }

    private static String text(ConsumerRecord<?, ?> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import(ProbeHandler.class)
    static class TestApplication {
        @org.springframework.context.annotation.Bean
        org.springframework.kafka.listener.adapter.RecordFilterStrategy<Object, Object> unrelatedGlobalDiscardFilter() {
            // This belongs to unrelated default-factory listeners, never the reliable APEX factory.
            return record -> true;
        }
    }

    public record Probe(UUID id, String mode) {
        public Probe { Objects.requireNonNull(id); Objects.requireNonNull(mode); }
    }
    public record ProbeResult(UUID id) implements IntegrationEvent {}

    public static class ProbeHandler {
        final Map<UUID, AtomicInteger> attempts = new ConcurrentHashMap<>();
        private final JdbcTemplate jdbc;
        private final CellEvents events;
        public ProbeHandler(JdbcTemplate jdbc, CellEvents events) { this.jdbc = jdbc; this.events = events; }
        @OnCellEvent(type = "PROBE", version = 1)
        public void handle(Probe payload) {
            int attempt = attempts.computeIfAbsent(payload.id(), id -> new AtomicInteger()).incrementAndGet();
            jdbc.update("INSERT INTO reliability_probe VALUES (?)", payload.id());
            events.append(EventContracts.define("PROBE_RESULT", 1, "PROBE", ProbeResult.class, "test.probe.v1"),
                    payload.id(), new ProbeResult(payload.id()));
            if (payload.mode().equals("ALTERNATING") && attempt % 2 == 0) {
                throw new IllegalArgumentException("private business details");
            }
            if (payload.mode().equals("PERMANENT") || payload.mode().equals("ALTERNATING")
                    || payload.mode().equals("TRANSIENT") && attempt < 3) {
                throw new IllegalStateException("private business details");
            }
        }
    }
}
