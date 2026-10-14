package com.apex.customer.modules.customer.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import com.apex.customer.modules.customer.application.CreateCustomerUseCase;
import com.apex.customer.modules.customer.domain.DuplicateCustomerReferenceException;
import com.apex.customer.support.CustomerDatabaseExtension;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@ExtendWith(CustomerDatabaseExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class CustomerApiIntegrationTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    private static UUID persistedId;

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        CustomerDatabaseExtension.registerDatasource(registry);
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private CreateCustomerUseCase createCustomerUseCase;

    @Test
    @Order(1)
    void flywayCreatesCustomersTableOnly() {
        Integer tables = jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'customers'
                """,
                Integer.class
        );
        assertThat(tables).isEqualTo(1);

        Integer foreignDomain = jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name IN ('outbox_event', 'saga', 'payment')
                """,
                Integer.class
        );
        assertThat(foreignDomain).isZero();
    }

    @Test
    @Order(2)
    void postCreatesCustomerAndPersistsRow() throws Exception {
        MvcResult result = mockMvc.perform(
                        post("/api/v1/customers")
                                .contentType(new MediaType(MediaType.APPLICATION_JSON, StandardCharsets.UTF_8))
                                .content("""
                                        {"externalReference":"CUST-10001","displayName":"مشتری آزمایشی"}
                                        """)
                )
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", org.hamcrest.Matchers.containsString("/api/v1/customers/")))
                .andExpect(jsonPath("$.externalReference").value("CUST-10001"))
                .andExpect(jsonPath("$.displayName").value("مشتری آزمایشی"))
                .andExpect(jsonPath("$.status").value("ACTIVE"))
                .andExpect(jsonPath("$.id").exists())
                .andExpect(jsonPath("$.createdAt").exists())
                .andReturn();

        JsonNode body = JSON.readTree(result.getResponse().getContentAsString(StandardCharsets.UTF_8));
        persistedId = UUID.fromString(body.get("id").asText());

        Long rows = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM customers WHERE id = ?",
                Long.class,
                persistedId
        );
        assertThat(rows).isEqualTo(1L);
    }

    @Test
    @Order(3)
    void getReturnsCommittedCustomer() throws Exception {
        mockMvc.perform(get("/api/v1/customers/{id}", persistedId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(persistedId.toString()))
                .andExpect(jsonPath("$.externalReference").value("CUST-10001"))
                .andExpect(jsonPath("$.displayName").value("مشتری آزمایشی"));
    }

    @Test
    @Order(4)
    void duplicateExternalReferenceReturns409AndNoExtraRow() throws Exception {
        Long before = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM customers", Long.class);

        mockMvc.perform(
                        post("/api/v1/customers")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content("""
                                        {"externalReference":"CUST-10001","displayName":"دیگر"}
                                        """)
                )
                .andExpect(status().isConflict());

        Long after = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM customers", Long.class);
        assertThat(after).isEqualTo(before);
    }

    @Test
    @Order(5)
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
        Long count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM customers WHERE external_reference = ?",
                Long.class,
                ref
        );
        assertThat(count).isEqualTo(1L);
    }

    @Test
    @Order(6)
    void unknownCustomerReturns404() throws Exception {
        mockMvc.perform(get("/api/v1/customers/{id}", UUID.randomUUID()))
                .andExpect(status().isNotFound());
    }

    @Test
    @Order(7)
    void malformedUuidReturns400() throws Exception {
        mockMvc.perform(get("/api/v1/customers/{id}", "not-a-uuid"))
                .andExpect(status().isBadRequest());
    }

    private void runCreate(
            String ref,
            CountDownLatch start,
            AtomicInteger successes,
            AtomicInteger duplicates
    ) {
        try {
            start.await(10, TimeUnit.SECONDS);
            createCustomerUseCase.execute(ref, "Concurrent");
            successes.incrementAndGet();
        } catch (DuplicateCustomerReferenceException ex) {
            duplicates.incrementAndGet();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
