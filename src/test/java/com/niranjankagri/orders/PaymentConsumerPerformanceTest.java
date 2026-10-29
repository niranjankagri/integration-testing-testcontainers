package com.niranjankagri.orders;

import static org.assertj.core.api.Assertions.assertThat;

import com.niranjankagri.orders.events.PaymentCompleted;
import com.niranjankagri.orders.events.PaymentCompleted.Result;
import com.niranjankagri.orders.events.Topics;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.json.JsonMapper;

/**
 * How fast the payment consumer works through a backlog, and whether consumer-group rebalances (an instance joining,
 * an instance leaving) lose or double-apply events. Not part of the normal build: run with `mvnw verify -Pperformance`
 * (CI runs it weekly and by hand). Results go to the console and to target/performance/consumer-performance.md.
 *
 * Own PostgreSQL and Kafka, so the numbers are not mixed with other tests' traffic. The thresholds are floors that catch
 * a large regression (a listener that suddenly takes a lock per event, a rebalance storm), not benchmarks: a shared CI
 * runner is too noisy for tight limits.
 */
@Tag("performance")
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PaymentConsumerPerformanceTest {

    private static final int EVENTS = 6_000;
    /** The rate of the steady stream in the rebalance test: 6,000 events take about 20 seconds. */
    private static final int EVENTS_PER_SECOND = 300;
    /**
     * Far below what one instance reached (186 events/s on a Windows laptop with Docker Desktop, where every database
     * round trip crosses the VM): only a large drop fails the build. One listener thread and one transaction per event
     * set the pace.
     */
    private static final double MIN_EVENTS_PER_SECOND = 50;
    /** The longest time without any progress while an instance joins or leaves (a rebalance takes seconds, not a minute). */
    private static final Duration MAX_PAUSE = Duration.ofSeconds(30);
    private static final Duration TIMEOUT = Duration.ofMinutes(5);
    private static final String GROUP = "payment-performance-test";

    @Container
    static final PostgreSQLContainer postgres = new PostgreSQLContainer(DockerImageName.parse("postgres:18-alpine"));

    @Container
    static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka-native:4.1.1"));

    private static final JsonMapper json = JsonMapper.builder().build();
    private static final List<String> report = new ArrayList<>();
    private static final List<String> notes = new ArrayList<>();
    private static ConfigurableApplicationContext instance;
    private static JdbcClient jdbc;

    @BeforeAll
    static void startTheService() {
        instance = ServiceInstance.start(postgres, kafka, GROUP, null);
        jdbc = JdbcClient.create(
                new DriverManagerDataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword()));
        report.add("| Phase | Events | Seconds | Events/s | Longest pause |");
        report.add("|---|---:|---:|---:|---:|");
    }

    @AfterAll
    static void stopAndWriteTheReport() throws IOException {
        if (instance != null) {
            instance.close();
        }
        Path file = Path.of("target", "performance", "consumer-performance.md");
        Files.createDirectories(file.getParent());
        String text = "## Payment consumer performance\n\n" + String.join("\n", report) + "\n\n"
                + String.join("\n", notes) + "\n";
        Files.writeString(file, text, StandardCharsets.UTF_8);
        System.out.println(text);
    }

    /** One instance works through a backlog of payments for existing orders. */
    @Test
    @Order(1)
    void oneInstanceProcessesABacklogAboveTheFloor() {
        List<Long> orders = insertOrders("throughput", EVENTS);

        long start = System.nanoTime();
        sendPayments(orders);
        Progress progress = awaitAllPaid("throughput", EVENTS, null);
        double seconds = (System.nanoTime() - start) / 1e9;

        record("one instance", EVENTS, seconds, progress.longestPause());
        assertThat(EVENTS / seconds).as("events per second").isGreaterThan(MIN_EVENTS_PER_SECOND);
    }

    /**
     * A second instance joins and leaves while payments keep arriving: two rebalances in the middle of the stream, on
     * any machine. (Against a ready backlog, a fast machine finished before the second instance had even started.)
     * Every event must be applied exactly once, and progress may stop only briefly.
     */
    @Test
    @Order(2)
    void rebalancesWhileProcessingLoseNothingAndApplyNothingTwice() {
        List<Long> orders = insertOrders("rebalance", EVENTS);
        long processedBefore = processedEventCount();
        Rebalances rebalances = new Rebalances();

        long start = System.nanoTime();
        CompletableFuture<Void> stream = CompletableFuture.runAsync(() -> sendSteadily(orders, rebalances));
        Progress progress = awaitAllPaid("rebalance", EVENTS, rebalances);
        stream.join();
        double seconds = (System.nanoTime() - start) / 1e9;

        record("instance joins and leaves (stream of " + EVENTS_PER_SECOND + "/s)", EVENTS, seconds,
                progress.longestPause());
        notes.add(String.format("While %d payments were being sent, the second instance owned partitions after %d were"
                        + " sent and left after %d; each event was recorded once.",
                EVENTS, rebalances.sentWhenJoined, rebalances.sentWhenLeft));
        assertThat(rebalances.sentWhenJoined).as("the second instance joined while events were still sent")
                .isBetween(1, EVENTS - 1);
        assertThat(rebalances.sentWhenLeft).as("the second instance left while events were still sent")
                .isBetween(rebalances.sentWhenJoined, EVENTS - 1);
        assertThat(processedEventCount() - processedBefore).as("each event recorded exactly once").isEqualTo(EVENTS);
        assertThat(progress.longestPause()).isLessThan(MAX_PAUSE);
    }

    /** Inserts orders with plain SQL (the API is not what is measured) and returns their ids. */
    private static List<Long> insertOrders(String batch, int count) {
        return jdbc.sql("""
                        INSERT INTO orders (customer, sku, quantity, status, created_at)
                        SELECT ? || '-' || n, 'SKU-1', 1, 'PLACED', now() FROM generate_series(1, ?) AS n
                        RETURNING id
                        """)
                .params(batch, count)
                .query(Long.class)
                .list();
    }

    /** Sends one SUCCEEDED payment per order, keyed by order id like the payment service. */
    private static void sendPayments(List<Long> orders) {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
            for (long order : orders) {
                String payment = json.writeValueAsString(new PaymentCompleted(UUID.randomUUID(), order, Result.SUCCEEDED));
                producer.send(new ProducerRecord<>(Topics.PAYMENTS_COMPLETED, Long.toString(order), payment));
            }
            producer.flush();
        }
    }

    /** Sends one payment per order at EVENTS_PER_SECOND (in slices every 100 ms), counting what has been sent. */
    private static void sendSteadily(List<Long> orders, Rebalances rebalances) {
        int slice = EVENTS_PER_SECOND / 10;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
            for (int i = 0; i < orders.size(); i++) {
                long order = orders.get(i);
                String payment = json.writeValueAsString(new PaymentCompleted(UUID.randomUUID(), order, Result.SUCCEEDED));
                producer.send(new ProducerRecord<>(Topics.PAYMENTS_COMPLETED, Long.toString(order), payment));
                if ((i + 1) % slice == 0) {
                    producer.flush();
                    rebalances.sent.set(i + 1);
                    sleep(100);
                }
            }
            producer.flush();
            rebalances.sent.set(orders.size());
        }
    }

    /**
     * Polls every 100 ms until all orders of the batch are PAID, keeping the longest time without progress. With
     * rebalances, a second instance starts at once and stops when two thirds of the events have been sent.
     */
    private static Progress awaitAllPaid(String batch, int count, Rebalances rebalances) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        long lastChange = System.nanoTime();
        long longestPause = 0;
        long paid = 0;
        while (paid < count) {
            assertThat(System.nanoTime()).as("all %d orders paid within %s", count, TIMEOUT).isLessThan(deadline);
            long now = paidCount(batch);
            long time = System.nanoTime();
            if (now > paid) {
                longestPause = Math.max(longestPause, time - lastChange);
                lastChange = time;
                paid = now;
            }
            if (rebalances != null) {
                rebalances.step(count);
            }
            sleep(100);
        }
        if (rebalances != null) {
            rebalances.stopSecond();
        }
        return new Progress(Duration.ofNanos(longestPause));
    }

    private static long paidCount(String batch) {
        return jdbc.sql("SELECT count(*) FROM orders WHERE customer LIKE ? AND status = 'PAID'")
                .param(batch + "-%")
                .query(Long.class)
                .single();
    }

    private static long processedEventCount() {
        return jdbc.sql("SELECT count(*) FROM processed_events").query(Long.class).single();
    }

    private static void record(String phase, int events, double seconds, Duration pause) {
        report.add(String.format("| %s | %d | %.1f | %.0f | %.1f s |",
                phase, events, seconds, events / seconds, pause.toMillis() / 1000.0));
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }

    /**
     * The second instance: started as soon as the stream begins, stopped once two thirds of the events are sent (the
     * stream takes about 20 seconds, the start-up a few). Both happen in the background, so the progress loop keeps
     * sampling: a blocked loop would report the start-up time as a pause in processing.
     */
    private static final class Rebalances {

        private final AtomicInteger sent = new AtomicInteger();
        private CompletableFuture<ConfigurableApplicationContext> joining;
        private CompletableFuture<Void> leaving;
        private volatile int sentWhenJoined = -1;
        private volatile int sentWhenLeft = -1;

        void step(int count) {
            if (joining == null) {
                joining = CompletableFuture.supplyAsync(() -> {
                    ConfigurableApplicationContext second = ServiceInstance.start(postgres, kafka, GROUP, null);
                    awaitPartitions(second);
                    sentWhenJoined = sent.get();
                    return second;
                });
            } else if (joining.isDone() && leaving == null && sent.get() >= 2 * count / 3) {
                sentWhenLeft = sent.get();
                leaving = joining.thenAccept(ConfigurableApplicationContext::close);
            }
        }

        /** Makes sure the second instance is stopped at the end (also if the stream finished first). */
        void stopSecond() {
            if (joining != null && leaving == null) {
                sentWhenLeft = sent.get();
                leaving = joining.thenAccept(ConfigurableApplicationContext::close);
            }
            if (leaving != null) {
                leaving.join();
            }
        }

        /** Waits until the group has rebalanced and the new instance owns partitions. */
        private static void awaitPartitions(ConfigurableApplicationContext context) {
            KafkaListenerEndpointRegistry registry = context.getBean(KafkaListenerEndpointRegistry.class);
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (registry.getListenerContainers().stream().allMatch(container -> {
                var assigned = container.getAssignedPartitions();
                return assigned == null || assigned.isEmpty();
            })) {
                assertThat(System.nanoTime()).as("the second instance got partitions").isLessThan(deadline);
                sleep(100);
            }
        }
    }

    private record Progress(Duration longestPause) {}
}
