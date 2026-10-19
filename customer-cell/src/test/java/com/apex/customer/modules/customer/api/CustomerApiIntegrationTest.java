package com.apex.customer.modules.customer.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.apex.platform.core.context.ExecutionContext;
import com.apex.platform.core.context.ExecutionContextProvider;
import com.apex.platform.webmvc.ApexHttpExecutionContextFilter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionEvaluationReport;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.transaction.support.TransactionTemplate;

import com.apex.customer.modules.customer.application.CreateCustomerUseCase;
import com.apex.customer.modules.customer.domain.DuplicateCustomerReferenceException;
import com.apex.customer.modules.customer.events.CustomerCreatedIntegrationEvent;
import com.apex.customer.support.CustomerDatabaseExtension;
import com.apex.customer.support.IsolatedTestEnvironment;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.config.location=classpath:application.yml", "spring.config.additional-location=",
        "spring.config.import=", "spring.profiles.active=", "spring.profiles.include="})
@ContextConfiguration(initializers = IsolatedTestEnvironment.class)
@AutoConfigureMockMvc
@ExtendWith(CustomerDatabaseExtension.class)
@EmbeddedKafka(partitions = 1, topics = {"apex.customer.events.v1"})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Import(CustomerApiIntegrationTest.ContextProbeController.class)
class CustomerApiIntegrationTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String TOPIC = "apex.customer.events.v1";

    private static UUID persistedId;
    private static UUID persistedEventId;

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        CustomerDatabaseExtension.registerDatasource(registry);
        IsolatedTestEnvironment.kafka(registry);
        registry.add("apex.messaging.cell.enabled", () -> "true");
        registry.add("apex.messaging.publisher.topic", () -> TOPIC);
        registry.add("apex.messaging.outbox.publisher.enabled", () -> "true");
        registry.add("apex.messaging.consumer.enabled", () -> "false");
        registry.add("apex.cell.id", () -> "customer-cell-test");
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private CreateCustomerUseCase createCustomerUseCase;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private ExecutionContextProvider contextProvider;

    @Autowired
    private ConfigurableApplicationContext applicationContext;

    @RestController
    static class ContextProbeController {

        private final ExecutionContextProvider provider;

        ContextProbeController(
                ExecutionContextProvider provider
        ) {
            this.provider = provider;
        }

        @GetMapping("/__test/apex-context")
        ExecutionContext currentContext() {
            return provider.requireCurrent();
        }
    }


    @Test
    @Order(1)
    void flywayCreatesCustomersAndOutbox() {
        Integer customers = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='customers'",
                Integer.class
        );
        Integer outbox = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='outbox_event'",
                Integer.class
        );
        assertThat(customers).isEqualTo(1);
        assertThat(outbox).isEqualTo(1);
    }

    @Test
    @Order(2)
    void postCreatesCustomerAndOutboxAtomically() throws Exception {
        MvcResult result = mockMvc.perform(
                        post("/api/v1/customers")
                                .contentType(new MediaType(MediaType.APPLICATION_JSON, StandardCharsets.UTF_8))
                                .content("""
                                        {"externalReference":"CUST-M22-0001","displayName":"مشتری آزمایشی"}
                                        """)
                )
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", org.hamcrest.Matchers.containsString("/api/v1/customers/")))
                .andExpect(jsonPath("$.externalReference").value("CUST-M22-0001"))
                .andExpect(jsonPath("$.displayName").value("مشتری آزمایشی"))
                .andReturn();

        JsonNode body = JSON.readTree(result.getResponse().getContentAsString(StandardCharsets.UTF_8));
        persistedId = UUID.fromString(body.get("id").asText());

        Long customerRows = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM customers WHERE id = ?", Long.class, persistedId);
        Long outboxRows = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_event WHERE aggregate_id = ? AND event_type = ?",
                Long.class,
                persistedId,
                CustomerCreatedIntegrationEvent.EVENT_TYPE
        );
        assertThat(customerRows).isEqualTo(1L);
        assertThat(outboxRows).isEqualTo(1L);

        persistedEventId = jdbcTemplate.queryForObject(
                "SELECT event_id FROM outbox_event WHERE aggregate_id = ?",
                UUID.class,
                persistedId
        );
        assertThat(persistedEventId).isNotNull();
    }


    @Test
    @Order(3)
    void forcedRollbackCreatesNeitherCustomerNorOutbox() {

        String ref = "CUST-ROLLBACK-1";

        assertThatThrownBy(() ->
                transactionTemplate.executeWithoutResult(status -> {

                    ExecutionContext context = ExecutionContext.root(
                            "customer-cell-test",
                            "customer-cell-test"
                    );

                    try (var scope = contextProvider.open(context)) {

                        createCustomerUseCase.execute(
                                ref,
                                "Rollback"
                        );

                        throw new IllegalStateException("force rollback");
                    }
                })
        ).hasMessageContaining("force rollback");

        assertThat(jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM customers WHERE external_reference = ?",
                Long.class,
                ref
        )).isZero();

        assertThat(jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_event WHERE payload::text LIKE ?",
                Long.class,
                "%CUST-ROLLBACK-1%"
        )).isZero();
    }


    @Test
    @Order(4)
    void duplicateExternalReferenceReturns409AndNoExtraOutbox() throws Exception {
        Long outboxBefore = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM outbox_event", Long.class);

        mockMvc.perform(
                        post("/api/v1/customers")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content("""
                                        {"externalReference":"CUST-M22-0001","displayName":"دیگر"}
                                        """)
                )
                .andExpect(status().isConflict());

        Long outboxAfter = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM outbox_event", Long.class);
        assertThat(outboxAfter).isEqualTo(outboxBefore);
    }

    @Test
    @Order(5)
    void scheduledPublisherDeliversOriginalEnvelopeMetadataToKafka() {
        var consumerProps = KafkaTestUtils.consumerProps("customer-outbox-assert", "true", embeddedKafka);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(consumerProps, new StringDeserializer(), new StringDeserializer())) {
            embeddedKafka.consumeFromAnEmbeddedTopic(consumer, TOPIC);
            ConsumerRecord<String, String> record =
                    KafkaTestUtils.getSingleRecord(consumer, TOPIC, Duration.ofSeconds(20));

            JsonNode envelope = JSON.readTree(record.value());
            JsonNode metadata = envelope.get("metadata");
            assertThat(metadata.get("eventId").asText()).isEqualTo(persistedEventId.toString());
            assertThat(metadata.get("eventType").asText()).isEqualTo("CUSTOMER_CREATED");
            assertThat(metadata.get("eventVersion").asInt()).isEqualTo(1);
            assertThat(metadata.get("aggregateId").asText()).isEqualTo(persistedId.toString());
            assertThat(metadata.get("aggregateType").asText()).isEqualTo("CUSTOMER");
            var stored = jdbcTemplate.queryForMap(
                    "SELECT correlation_id, causation_id, occurred_at, source_service, cell_id FROM outbox_event WHERE event_id = ?",
                    persistedEventId);
            assertThat(metadata.get("correlationId").asText()).isEqualTo(stored.get("correlation_id").toString());
            assertThat(metadata.get("causationId").isNull()).isTrue();
            assertThat(stored.get("causation_id")).isNull();
            assertThat(java.time.Instant.parse(metadata.get("occurredAt").asText())).isEqualTo(
                    ((java.sql.Timestamp) stored.get("occurred_at")).toInstant());
            assertThat(metadata.get("sourceService").asText()).isEqualTo(stored.get("source_service"));
            assertThat(metadata.get("cellId").asText()).isEqualTo(stored.get("cell_id"));
            assertThat(envelope.get("payload").get("displayName").asText()).isEqualTo("مشتری آزمایشی");
            assertThat(record.key()).isEqualTo("CUSTOMER:" + persistedId);

            Boolean published = jdbcTemplate.queryForObject(
                    "SELECT published FROM outbox_event WHERE event_id = ?",
                    Boolean.class,
                    persistedEventId
            );
            assertThat(published).isTrue();
        }
    }

    @Test
    @Order(6)
    void getReturnsCommittedCustomer() throws Exception {
        mockMvc.perform(get("/api/v1/customers/{id}", persistedId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.externalReference").value("CUST-M22-0001"));
    }

    @Test
    @Order(7)
    void concurrentDuplicateInsertsKeepSingleRow() throws Exception {
        String ref = "CUST-CONCURRENT-1";
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger duplicates = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);

        Future<?> f1 = pool.submit(() -> runCreate(ref, start, successes, duplicates));
        Future<?> f2 = pool.submit(() -> runCreate(ref, start, successes, duplicates));
        start.countDown();
        f1.get(30, TimeUnit.SECONDS);
        f2.get(30, TimeUnit.SECONDS);
        pool.shutdownNow();

        assertThat(successes.get()).isEqualTo(1);
        assertThat(duplicates.get()).isEqualTo(1);
    }

    @Test
    @Order(8)
    void unknownCustomerReturns404() throws Exception {
        mockMvc.perform(get("/api/v1/customers/{id}", UUID.randomUUID()))
                .andExpect(status().isNotFound());
    }

    @Test
    @Order(9)
    void malformedUuidReturns400() throws Exception {
        mockMvc.perform(get("/api/v1/customers/{id}", "not-a-uuid"))
                .andExpect(status().isBadRequest());
    }


    @Test
    @Order(10)
    void httpContextIsSharedAndCleanedUp() throws Exception {

        MvcResult first = mockMvc.perform(
                        get("/__test/apex-context")
                )
                .andExpect(status().isOk())
                .andReturn();

        JsonNode firstContext = JSON.readTree(
                first.getResponse().getContentAsString(
                        StandardCharsets.UTF_8
                )
        );

        UUID firstCorrelationId = UUID.fromString(
                firstContext.get("correlationId").asText()
        );

        assertThat(firstContext.get("causationId").isNull())
                .isTrue();

        assertThat(firstContext.get("sourceService").asText())
                .isNotBlank();

        assertThat(firstContext.get("cellId").asText())
                .isEqualTo("customer-cell-test");

        assertThatThrownBy(contextProvider::requireCurrent)
                .isInstanceOf(IllegalStateException.class);

        MvcResult second = mockMvc.perform(
                        get("/__test/apex-context")
                )
                .andExpect(status().isOk())
                .andReturn();

        JsonNode secondContext = JSON.readTree(
                second.getResponse().getContentAsString(
                        StandardCharsets.UTF_8
                )
        );

        UUID secondCorrelationId = UUID.fromString(
                secondContext.get("correlationId").asText()
        );

        assertThat(secondCorrelationId)
                .isNotEqualTo(firstCorrelationId);

        assertThatThrownBy(contextProvider::requireCurrent)
                .isInstanceOf(IllegalStateException.class);
    }


    @Test
    @Order(11)
    void httpCreatedEventShouldHaveRootExecutionContext()
            throws Exception {

        String ref = "CUST-CONTEXT-" + UUID.randomUUID();

        MvcResult result = mockMvc.perform(
                        post("/api/v1/customers")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content("""
                                    {
                                      "externalReference": "%s",
                                      "displayName": "Context Test"
                                    }
                                    """.formatted(ref))
                )
                .andExpect(status().isCreated())
                .andReturn();

        JsonNode body = JSON.readTree(
                result.getResponse().getContentAsString(
                        StandardCharsets.UTF_8
                )
        );

        UUID customerId = UUID.fromString(
                body.get("id").asText()
        );

        var stored = jdbcTemplate.queryForMap(
                """
                SELECT correlation_id,
                       causation_id,
                       source_service,
                       cell_id
                FROM outbox_event
                WHERE aggregate_id = ?
                  AND event_type = ?
                """,
                customerId,
                CustomerCreatedIntegrationEvent.EVENT_TYPE
        );

        assertThat(stored.get("correlation_id"))
                .isNotNull();

        assertThat(stored.get("causation_id"))
                .isNull();

        assertThat(stored.get("cell_id"))
                .isEqualTo("customer-cell-test");
    }


    @Test
    @Order(12)
    void shouldUseSharedApexWebMvcFilter() {

        var filters = applicationContext.getBeansOfType(
                ApexHttpExecutionContextFilter.class
        );

        assertThat(filters).hasSize(1);

        assertThat(filters)
                .containsKey("apexHttpExecutionContextFilter");

        var filter = filters.get(
                "apexHttpExecutionContextFilter"
        );

        assertThat(filter)
                .isInstanceOf(
                        ApexHttpExecutionContextFilter.class
                );

        assertThat(
                applicationContext.getBeansOfType(
                        ExecutionContextProvider.class
                )
        ).hasSize(1);
    }


    @Test
    void diagnoseWebMvcAutoConfiguration() {

        System.out.println("=== APEX DIAGNOSTICS ===");

        System.out.println(
                "ExecutionContextProvider beans: " +
                        Arrays.toString(
                                applicationContext.getBeanNamesForType(
                                        ExecutionContextProvider.class
                                )
                        )
        );

        System.out.println(
                "APEX WebMVC Filter beans: " +
                        Arrays.toString(
                                applicationContext.getBeanNamesForType(
                                        ApexHttpExecutionContextFilter.class
                                )
                        )
        );

        System.out.println(
                "Existing filter name: " +
                        applicationContext.containsBean(
                                "apexHttpExecutionContextFilter"
                        )
        );

        ConditionEvaluationReport report =
                ConditionEvaluationReport.get(
                        applicationContext.getBeanFactory()
                );

        report.getConditionAndOutcomesBySource()
                .forEach((source, outcomes) -> {

                    if (!source.contains(
                            "ApexWebMvcAutoConfiguration"
                    )) {
                        return;
                    }

                    System.out.println("SOURCE: " + source);

                    outcomes.forEach(conditionAndOutcome -> {
                        System.out.println(
                                conditionAndOutcome.getCondition()
                                        .getClass()
                                        .getSimpleName()
                                        + " => "
                                        + conditionAndOutcome.getOutcome()
                                        .isMatch()
                                        + " | "
                                        + conditionAndOutcome.getOutcome()
                                        .getMessage()
                        );
                    });
                });

        var bean = applicationContext.getBean(
                "apexHttpExecutionContextFilter"
        );

        System.out.println(
                "FILTER CLASS: " + bean.getClass().getName()
        );

        var definition = applicationContext
                .getBeanFactory()
                .getBeanDefinition("apexHttpExecutionContextFilter");

        System.out.println(
                "BEAN SOURCE: " + definition.getResourceDescription()
        );
    }



    private void runCreate(
            String ref,
            CountDownLatch start,
            AtomicInteger successes,
            AtomicInteger duplicates
    ) {
        try {
            start.await(10, TimeUnit.SECONDS);

            ExecutionContext context = ExecutionContext.root(
                    "customer-cell-test",
                    "customer-cell-test"
            );

            try (var scope = contextProvider.open(context)) {
                createCustomerUseCase.execute(ref, "Concurrent");
            }

            successes.incrementAndGet();

        } catch (DuplicateCustomerReferenceException ex) {
            duplicates.incrementAndGet();

        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

}
