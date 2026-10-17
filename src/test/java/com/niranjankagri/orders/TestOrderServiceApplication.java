package com.niranjankagri.orders;

import org.springframework.boot.SpringApplication;

/**
 * Runs the service locally with the same containers the tests use: no database or Kafka to install.
 * ./mvnw spring-boot:test-run, then open http://localhost:8080/graphiql
 */
public class TestOrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.from(OrderServiceApplication::main)
                .with(TestcontainersConfiguration.class)
                .run(args);
    }
}
