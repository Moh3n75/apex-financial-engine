package com.apex.apexbootstrap.testsupport;

import org.junit.jupiter.api.BeforeAll;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;


@SpringBootTest
@Testcontainers
public abstract class AbstractIntegrationTest {


    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>(
                    "postgres:18"
            )
                    .withDatabaseName("postgres")
                    .withUsername("postgres")
                    .withPassword("123456789");



    @Container
    static KafkaContainer kafka =
            new KafkaContainer(
                    "apache/kafka:3.8.0"
            );



    @DynamicPropertySource
    static void configureProperties(
            DynamicPropertyRegistry registry
    ){

        registry.add(
                "spring.datasource.url",
                postgres::getJdbcUrl
        );


        registry.add(
                "spring.datasource.username",
                postgres::getUsername
        );


        registry.add(
                "spring.datasource.password",
                postgres::getPassword
        );


        registry.add(
                "spring.kafka.bootstrap-servers",
                kafka::getBootstrapServers
        );

    }

}