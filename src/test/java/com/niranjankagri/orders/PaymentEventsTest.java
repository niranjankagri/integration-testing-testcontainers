package com.niranjankagri.orders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.niranjankagri.orders.events.PaymentCompleted;
import com.niranjankagri.orders.events.PaymentCompleted.Result;
import com.niranjankagri.orders.events.Topics;
import com.niranjankagri.orders.order.Order;
import com.niranjankagri.orders.order.OrderRepository;
import com.niranjankagri.orders.order.OrderService;
import com.niranjankagri.orders.order.OrderStatus;
import com.niranjankagri.orders.order.PlaceOrderInput;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.json.JsonMapper;

/**
 * Events this service consumes, sent the way the payment service would send them. The checks wait for the outcome
 * with Awaitility (poll until true, fail after a timeout) instead of sleeping for a guessed time.
 */
@IntegrationTest
class PaymentEventsTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    @Autowired
    KafkaTemplate<String, String> kafka;

    @Autowired
    KafkaContainer kafkaContainer;

    @Autowired
    OrderService service;

    @Autowired
    OrderRepository orders;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    JsonMapper json;

    @Autowired
    MeterRegistry metrics;

    /** A successful payment marks the order PAID. */
    @Test
    void successfulPaymentMarksTheOrderPaid() {
        double before = paymentEventCount("paid");
        Order order = newOrder();
        sendPayment(new PaymentCompleted(UUID.randomUUID(), order.id(), Result.SUCCEEDED));
        awaitStatus(order, OrderStatus.PAID);
        await().untilAsserted(() -> assertThat(paymentEventCount("paid")).isEqualTo(before + 1));
    }

    /** A failed payment cancels the order. */
    @Test
    void failedPaymentCancelsTheOrder() {
        Order order = newOrder();
        sendPayment(new PaymentCompleted(UUID.randomUUID(), order.id(), Result.FAILED));
        awaitStatus(order, OrderStatus.CANCELLED);
    }

    /** The same event id delivered twice is applied once, even when the copy points at another order. */
    @Test
    void aRedeliveredEventIsAppliedOnlyOnce() {
        // Kafka delivers at least once. The same event id must not be applied twice, even if the copy differs:
        // here it points at another order, and that order must stay untouched.
        Order first = newOrder();
        Order second = newOrder();
        UUID eventId = UUID.randomUUID();
        double duplicatesBefore = paymentEventCount("duplicate");

        sendPayment(new PaymentCompleted(eventId, first.id(), Result.FAILED));
        awaitStatus(first, OrderStatus.CANCELLED);
        sendPayment(new PaymentCompleted(eventId, second.id(), Result.SUCCEEDED));

        // Bound the wait with a later event on the same order (same key, same partition, so it is processed after
        // the duplicate): once it is applied, the duplicate has been handled.
        sendPayment(new PaymentCompleted(UUID.randomUUID(), second.id(), Result.FAILED));
        awaitStatus(second, OrderStatus.CANCELLED);
        await().untilAsserted(() -> assertThat(paymentEventCount("duplicate")).isEqualTo(duplicatesBefore + 1));
        Integer applied = jdbc.sql("SELECT count(*) FROM processed_events WHERE event_id = ?")
                .param(eventId)
                .query(Integer.class)
                .single();
        assertThat(applied).isEqualTo(1);
    }

    /**
     * Broken JSON goes to the dead-letter topic at once, and the next event for the same order is still processed.
     */
    @Test
    void brokenJsonGoesToTheDeadLetterTopicAndDoesNotBlockLaterEvents() {
        Order order = newOrder();
        try (KafkaProbe dlt = new KafkaProbe(kafkaContainer.getBootstrapServers(), Topics.PAYMENTS_COMPLETED_DLT)) {
            String broken = "{\"eventId\": \"not json at all";
            kafka.send(Topics.PAYMENTS_COMPLETED, String.valueOf(order.id()), broken);

            ConsumerRecord<String, String> dead = dlt.awaitRecord(r -> broken.equals(r.value()));
            // The exception header names Spring's wrapper; the cause header says what really failed.
            assertThat(header(dead, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).startsWith("tools.jackson.");
        }
        // A poison message must not stop the partition: the next event for the same order is still processed.
        sendPayment(new PaymentCompleted(UUID.randomUUID(), order.id(), Result.SUCCEEDED));
        awaitStatus(order, OrderStatus.PAID);
    }

    /**
     * An event without its required fields goes to the dead-letter topic, and the header says which field was
     * missing.
     */
    @Test
    void anEventWithoutRequiredFieldsGoesToTheDeadLetterTopic() {
        try (KafkaProbe dlt = new KafkaProbe(kafkaContainer.getBootstrapServers(), Topics.PAYMENTS_COMPLETED_DLT)) {
            String incomplete = "{\"eventId\": \"" + UUID.randomUUID() + "\", \"result\": \"SUCCEEDED\"}";
            kafka.send(Topics.PAYMENTS_COMPLETED, "no-order", incomplete);

            ConsumerRecord<String, String> dead = dlt.awaitRecord(r -> incomplete.equals(r.value()));
            assertThat(header(dead, KafkaHeaders.DLT_EXCEPTION_MESSAGE)).contains("orderId");
        }
    }

    /** An event for an unknown order is retried, then dead-lettered, and leaves no processed-event mark behind. */
    @Test
    void anEventForAnUnknownOrderIsRetriedThenDeadLettered() {
        long unknown = 987_654_321L;
        UUID eventId = UUID.randomUUID();
        double failedBefore = paymentEventCount("delivery_failed");
        double deadLetteredBefore = paymentEventCount("dead_lettered");
        try (KafkaProbe dlt = new KafkaProbe(kafkaContainer.getBootstrapServers(), Topics.PAYMENTS_COMPLETED_DLT)) {
            sendPayment(new PaymentCompleted(eventId, unknown, Result.SUCCEEDED));

            ConsumerRecord<String, String> dead = dlt.awaitRecord(r -> r.value().contains(eventId.toString()));
            assertThat(header(dead, KafkaHeaders.DLT_EXCEPTION_MESSAGE)).contains("No order " + unknown);
        }
        await().untilAsserted(() -> {
            assertThat(paymentEventCount("delivery_failed")).isGreaterThan(failedBefore);
            assertThat(paymentEventCount("dead_lettered")).isEqualTo(deadLetteredBefore + 1);
        });
        // Each attempt rolled back, including the processed-event mark: replaying the event from the dead-letter
        // topic later (once the order exists) would still be applied.
        Integer marked = jdbc.sql("SELECT count(*) FROM processed_events WHERE event_id = ?")
                .param(eventId)
                .query(Integer.class)
                .single();
        assertThat(marked).isZero();
    }

    /** A dead-lettered event can be replayed after its temporarily unknown order becomes available. */
    @Test
    void aDeadLetteredPaymentCanBeReplayedWhenItsOrderAppears() {
        long orderId = 9_876_543_210L;
        UUID eventId = UUID.randomUUID();
        PaymentCompleted payment = new PaymentCompleted(eventId, orderId, Result.SUCCEEDED);
        String payload = json.writeValueAsString(payment);

        try (KafkaProbe dlt = new KafkaProbe(kafkaContainer.getBootstrapServers(), Topics.PAYMENTS_COMPLETED_DLT)) {
            kafka.send(Topics.PAYMENTS_COMPLETED, String.valueOf(orderId), payload);
            ConsumerRecord<String, String> dead = dlt.awaitRecord(record -> payload.equals(record.value()));
            assertThat(header(dead, KafkaHeaders.DLT_EXCEPTION_MESSAGE)).contains("No order " + orderId);

            // The order service catches up after the payment event has already been dead-lettered.
            jdbc.sql("""
                            INSERT INTO orders (id, customer, sku, quantity, status, created_at)
                            OVERRIDING SYSTEM VALUE
                            VALUES (?, 'late-order', 'SKU-1', 1, 'PLACED', now())
                            """)
                    .param(orderId)
                    .update();

            // Replay the original payload to the input topic, as an operator or replay job would.
            kafka.send(Topics.PAYMENTS_COMPLETED, String.valueOf(orderId), dead.value());
        }

        await().atMost(TIMEOUT)
                .untilAsserted(() -> assertThat(orders.findById(orderId).orElseThrow().status())
                        .isEqualTo(OrderStatus.PAID));
        Integer marked = jdbc.sql("SELECT count(*) FROM processed_events WHERE event_id = ?")
                .param(eventId)
                .query(Integer.class)
                .single();
        assertThat(marked).isEqualTo(1);
    }

    /** Places an order for a new customer through the service. */
    private Order newOrder() {
        return service.place(new PlaceOrderInput("customer-" + UUID.randomUUID(), "SKU-1", 1));
    }

    /** Sends a payment event the way the payment service would. */
    private void sendPayment(PaymentCompleted event) {
        // Keyed by order id, as the payment service does: events of one order stay in order.
        kafka.send(Topics.PAYMENTS_COMPLETED, String.valueOf(event.orderId()), json.writeValueAsString(event));
    }

    /** Polls the database until the order has the expected status, failing after TIMEOUT. */
    private void awaitStatus(Order order, OrderStatus expected) {
        await().atMost(TIMEOUT)
                .untilAsserted(() -> assertThat(orders.findById(order.id()).orElseThrow().status()).isEqualTo(expected));
    }

    /** Number of listener decisions currently recorded for one bounded outcome label. */
    private double paymentEventCount(String outcome) {
        return metrics.find("orders.payment.events")
                .tag("outcome", outcome)
                .counter() == null
                ? 0
                : metrics.find("orders.payment.events").tag("outcome", outcome).counter().count();
    }

    /** A record header as text, or empty when the record does not have it. */
    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? "" : new String(header.value(), StandardCharsets.UTF_8);
    }
}
