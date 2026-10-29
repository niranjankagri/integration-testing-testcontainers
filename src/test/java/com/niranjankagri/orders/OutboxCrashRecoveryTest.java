package com.niranjankagri.orders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.niranjankagri.orders.events.OrderPlaced;
import com.niranjankagri.orders.events.OutboxEvent;
import com.niranjankagri.orders.events.OutboxEventStore;
import com.niranjankagri.orders.events.Topics;
import com.niranjankagri.orders.order.Order;
import com.niranjankagri.orders.order.OrderService;
import com.niranjankagri.orders.order.PlaceOrderInput;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.json.JsonMapper;

/**
 * The outbox's hardest case: the relay process dies after Kafka acknowledged an event but before the database marked
 * it published. A second service instance, started afterwards like a restarted process, must take the event over once
 * the dead instance's claim lease expires, send it again with the same event id, and mark it published.
 *
 * This class runs two real instances of the service one after the other, so it has its own PostgreSQL and Kafka:
 * the shared test context's relay polls its own database and cannot claim this test's event first.
 */
@Testcontainers
class OutboxCrashRecoveryTest {

    /** Short, so the takeover happens within seconds; production keeps the 30-second default. */
    private static final int LEASE_SECONDS = 3;

    @Container
    static final PostgreSQLContainer postgres = new PostgreSQLContainer(DockerImageName.parse("postgres:18-alpine"));

    @Container
    static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka-native:4.1.1"));

    @Test
    void aRestartedInstanceRepublishesAnEventWhoseRelayCrashedAfterTheKafkaAcknowledgement() {
        JsonMapper json = JsonMapper.builder().build();
        KafkaProbe probe = null;
        try {
            // Instance 1: its relay sends the event, Kafka acknowledges it, then the "process" dies before the mark.
            Order order;
            try (ConfigurableApplicationContext crashing = startInstance(true)) {
                // Opened after the instance has created its topics: asking for a missing topic could auto-create it
                // with one partition. Opened before the order, so it sees the first send.
                probe = new KafkaProbe(kafka.getBootstrapServers(), Topics.ORDERS_PLACED);
                order = crashing.getBean(OrderService.class)
                        .place(new PlaceOrderInput("crash-" + UUID.randomUUID(), "SKU-TEA", 1));
                assertThat(CrashAfterAcknowledgement.crashed.await(15, TimeUnit.SECONDS))
                        .as("the first relay reached the point between Kafka acknowledgement and database mark")
                        .isTrue();
            }
            String key = Long.toString(order.id());

            ConsumerRecord<String, String> first = probe.awaitRecords(record -> key.equals(record.key()), 1).getFirst();
            UUID eventId = json.readValue(first.value(), OrderPlaced.class).eventId();
            Map<String, Object> row = outboxRow(eventId);
            assertThat(row.get("published_at")).as("Kafka has the event, the database does not know").isNull();
            assertThat(row.get("claim_token")).as("the dead relay's claim is still there").isNotNull();

            // Instance 2, started like a restarted process: once the old lease expires, it sends the event again.
            try (ConfigurableApplicationContext restarted = startInstance(false)) {
                await().atMost(Duration.ofSeconds(LEASE_SECONDS + 15))
                        .untilAsserted(() -> assertThat(outboxRow(eventId).get("published_at")).isNotNull());
            }

            // At least once: the same event twice, byte for byte, with the same event id for consumers to deduplicate.
            List<ConsumerRecord<String, String>> sent = probe.awaitRecords(record -> key.equals(record.key()), 2);
            assertThat(sent).hasSize(2);
            assertThat(sent).extracting(ConsumerRecord::value).containsOnly(first.value());
            Map<String, Object> published = outboxRow(eventId);
            assertThat(published.get("claim_token")).as("the claim is cleared once published").isNull();
            assertThat(published.get("claimed_until")).isNull();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        } finally {
            if (probe != null) {
                probe.close();
            }
        }
    }

    /**
     * Starts one instance of the service on this class's containers. With crashAfterAcknowledgement, its outbox store
     * throws an Error when the relay tries to mark the first event published: the relay catches Exception only, so the
     * Error leaves the claim in place, exactly as a process that died at that moment would.
     */
    private static ConfigurableApplicationContext startInstance(boolean crashAfterAcknowledgement) {
        return ServiceInstance.start(
                postgres,
                kafka,
                "outbox-crash-recovery-test",
                crashAfterAcknowledgement
                        ? context -> context.registerBean(
                                "crashingOutbox", CrashAfterAcknowledgement.class, bean -> bean.setPrimary(true))
                        : null,
                "--orders.outbox.poll-interval-ms=100",
                "--orders.outbox.claim-lease-seconds=" + LEASE_SECONDS);
    }

    /** The outbox row of one event, read with plain JDBC from this class's database. */
    private static Map<String, Object> outboxRow(UUID eventId) {
        var dataSource = new DriverManagerDataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        return JdbcClient.create(dataSource)
                .sql("SELECT published_at, claim_token, claimed_until FROM outbox_events WHERE event_id = ?")
                .param(eventId)
                .query()
                .singleRow();
    }

    /**
     * An outbox store whose markPublished "kills the process": it throws an Error the relay does not catch. After that
     * it claims nothing more, as a dead process would not, until the test has shut the instance down.
     */
    static class CrashAfterAcknowledgement extends OutboxEventStore {

        static final CountDownLatch crashed = new CountDownLatch(1);

        CrashAfterAcknowledgement(JdbcClient jdbc, JsonMapper json) {
            super(jdbc, json);
        }

        @Override
        public List<OutboxEvent> claimPending(int limit, UUID claimToken, int leaseSeconds) {
            return crashed.getCount() == 0 ? List.of() : super.claimPending(limit, claimToken, leaseSeconds);
        }

        @Override
        public boolean markPublished(UUID eventId, UUID claimToken) {
            crashed.countDown();
            throw new SimulatedCrash();
        }
    }

    /** Stands in for the process dying; an Error, so the relay's catch (Exception) does not handle it. */
    static class SimulatedCrash extends Error {
        SimulatedCrash() {
            super("simulated process crash after the Kafka acknowledgement");
        }
    }
}
