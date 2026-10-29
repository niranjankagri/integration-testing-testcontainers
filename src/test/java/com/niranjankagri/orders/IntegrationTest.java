package com.niranjankagri.orders;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.boot.graphql.test.autoconfigure.tester.AutoConfigureHttpGraphQlTester;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

/**
 * The whole service on a random port, with real PostgreSQL and Kafka. All test classes carry exactly this
 * annotation, so Spring caches one context and the containers start once for the whole run.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
// No trace collector runs during tests; without this, sampled spans fail to export to localhost:4318 and log errors.
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "management.tracing.export.enabled=false")
@AutoConfigureHttpGraphQlTester
@Import(TestcontainersConfiguration.class)
public @interface IntegrationTest {}
