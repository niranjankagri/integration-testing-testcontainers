package com.niranjankagri.orders;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.TypeExcludeFilter;
import org.springframework.boot.test.context.TestComponent;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Starts a complete instance of the service on given containers, the way a second process or a restarted process
 * would run: for tests that need several instances (crash recovery, consumer rebalancing). Close the returned context
 * to stop the instance.
 */
final class ServiceInstance {

    private ServiceInstance() {}

    /**
     * Starts one instance. The settings are command-line arguments, because they override application.properties;
     * SpringApplicationBuilder.properties() would only set defaults. extraArguments are more "--name=value" settings;
     * beans adds beans before the context refreshes (null for none).
     */
    static ConfigurableApplicationContext start(
            PostgreSQLContainer postgres,
            KafkaContainer kafka,
            String groupId,
            Consumer<GenericApplicationContext> beans,
            String... extraArguments) {
        List<String> arguments = new ArrayList<>(List.of(
                "--spring.datasource.url=" + postgres.getJdbcUrl(),
                "--spring.datasource.username=" + postgres.getUsername(),
                "--spring.datasource.password=" + postgres.getPassword(),
                "--spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers(),
                "--spring.kafka.consumer.group-id=" + groupId,
                "--server.port=0",
                // No trace collector runs in tests: the exporter would retry on shutdown for seconds.
                "--management.tracing.export.enabled=false"));
        arguments.addAll(List.of(extraArguments));
        return new SpringApplicationBuilder(OrderServiceApplication.class)
                .initializers(context -> {
                    GenericApplicationContext generic = (GenericApplicationContext) context;
                    // Component scanning would also find TestcontainersConfiguration and start extra containers.
                    generic.getBeanFactory().registerSingleton("skipTestComponents", new SkipTestComponents());
                    if (beans != null) {
                        beans.accept(generic);
                    }
                })
                .run(arguments.toArray(String[]::new));
    }

    /** Leaves test-only configuration (TestcontainersConfiguration) out of the component scan, as @SpringBootTest does. */
    static final class SkipTestComponents extends TypeExcludeFilter {

        @Override
        public boolean match(MetadataReader reader, MetadataReaderFactory factory) {
            return reader.getAnnotationMetadata().isAnnotated(TestComponent.class.getName());
        }

        @Override
        public boolean equals(Object other) {
            return other != null && other.getClass() == getClass();
        }

        @Override
        public int hashCode() {
            return getClass().hashCode();
        }
    }
}
