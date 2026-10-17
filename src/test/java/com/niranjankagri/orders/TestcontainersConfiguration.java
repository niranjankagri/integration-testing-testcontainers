package com.niranjankagri.orders;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The real dependencies, started in Docker. @ServiceConnection hands their addresses to Spring Boot, so no URL or
 * port is configured anywhere. Every test class uses the same Spring context, so the containers start once per run.
 *
 * Versions are pinned: "latest" would let a new PostgreSQL or Kafka release change test results without any change
 * in this repository.
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

    @Bean
    @ServiceConnection
    public PostgreSQLContainer postgres() {
        return new PostgreSQLContainer(DockerImageName.parse("postgres:18-alpine"));
    }

    /** The GraalVM native Kafka image starts in about a second, much faster than the JVM one. */
    @Bean
    @ServiceConnection
    public KafkaContainer kafka() {
        return new KafkaContainer(DockerImageName.parse("apache/kafka-native:4.1.1"));
    }
}
