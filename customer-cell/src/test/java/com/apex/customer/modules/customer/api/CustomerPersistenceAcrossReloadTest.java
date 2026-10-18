package com.apex.customer.modules.customer.api;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import com.apex.customer.support.CustomerDatabaseExtension;
import com.apex.customer.support.IsolatedTestEnvironment;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Proves persistence is not in-memory: create in one Spring context, reload context,
 * GET still succeeds against the same dedicated PostgreSQL.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.config.location=classpath:application.yml", "spring.config.additional-location=",
        "spring.config.import=", "spring.profiles.active=", "spring.profiles.include="})
@ContextConfiguration(initializers = IsolatedTestEnvironment.class)
@AutoConfigureMockMvc
@ExtendWith(CustomerDatabaseExtension.class)
@EmbeddedKafka(partitions = 1, topics = {"apex.customer.events.v1"})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CustomerPersistenceAcrossReloadTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    private static UUID idAfterCreate;

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        CustomerDatabaseExtension.registerDatasource(registry);
        IsolatedTestEnvironment.kafka(registry);
        registry.add("apex.messaging.cell.enabled", () -> "true");
        registry.add("apex.messaging.publisher.topic", () -> "apex.customer.events.v1");
        registry.add("apex.messaging.outbox.publisher.enabled", () -> "false");
        registry.add("apex.messaging.consumer.enabled", () -> "false");
        registry.add("apex.cell.id", () -> "customer-cell-reload-test");
    }

    @Autowired
    private MockMvc mockMvc;

    @Test
    @Order(1)
    @DirtiesContext(methodMode = DirtiesContext.MethodMode.AFTER_METHOD)
    void createCustomerThenReloadContext() throws Exception {
        MvcResult result = mockMvc.perform(
                        post("/api/v1/customers")
                                .contentType(new MediaType(MediaType.APPLICATION_JSON, StandardCharsets.UTF_8))
                                .content("""
                                        {"externalReference":"CUST-RELOAD-1","displayName":"پایدار"}
                                        """)
                )
                .andExpect(status().isCreated())
                .andReturn();

        JsonNode body = JSON.readTree(result.getResponse().getContentAsString(StandardCharsets.UTF_8));
        idAfterCreate = UUID.fromString(body.get("id").asText());
    }

    @Test
    @Order(2)
    void getStillSucceedsAfterApplicationContextReload() throws Exception {
        mockMvc.perform(get("/api/v1/customers/{id}", idAfterCreate))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.externalReference").value("CUST-RELOAD-1"))
                .andExpect(jsonPath("$.displayName").value("پایدار"));
    }
}
