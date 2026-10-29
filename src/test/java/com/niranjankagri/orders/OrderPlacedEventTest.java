package com.niranjankagri.orders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.niranjankagri.orders.events.Topics;
import com.niranjankagri.orders.order.OrderService;
import com.niranjankagri.orders.order.PlaceOrderInput;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.graphql.test.tester.HttpGraphQlTester;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** What this service promises to other services: an OrderPlaced event for every placed order. */
@IntegrationTest
class OrderPlacedEventTest {

    @Autowired
    HttpGraphQlTester graphQl;

    @Autowired
    KafkaContainer kafka;

    @Autowired
    JsonMapper json;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    OrderService orders;

    @Autowired
    PlatformTransactionManager transactionManager;

    /** Placing an order sends OrderPlaced, keyed by the order id, with the order's data and an event id. */
    @Test
    void placingAnOrderPublishesOrderPlacedKeyedByTheOrderId() {
        String customer = "customer-" + UUID.randomUUID();
        try (KafkaProbe probe = new KafkaProbe(kafka.getBootstrapServers(), Topics.ORDERS_PLACED)) {
            String id = graphQl.document("""
                            mutation($c: String!) {
                              placeOrder(input: {customer: $c, sku: "SKU-TEA", quantity: 3}) { id }
                            }""")
                    .variable("c", customer)
                    .execute()
                    .errors().verify()
                    .path("placeOrder.id").entity(String.class).get();

            ConsumerRecord<String, String> record = probe.awaitRecord(r -> id.equals(r.key()));
            JsonNode event = json.readTree(record.value());
            assertThat(event.get("orderId").asText()).isEqualTo(id);
            assertThat(event.get("customer").asText()).isEqualTo(customer);
            assertThat(event.get("sku").asText()).isEqualTo("SKU-TEA");
            assertThat(event.get("quantity").asInt()).isEqualTo(3);
            // Consumers deduplicate by eventId, so it must be present and a UUID.
            UUID eventId = UUID.fromString(event.get("eventId").asText());
            assertThat(eventId).isNotNull();
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                Integer published = jdbc.sql("""
                                SELECT count(*) FROM outbox_events
                                WHERE event_id = ? AND published_at IS NOT NULL
                                """)
                        .param(eventId)
                        .query(Integer.class)
                        .single();
                assertThat(published).isEqualTo(1);
            });
        }
    }

    /** The order and its outgoing event are both removed when their shared transaction rolls back. */
    @Test
    void rollingBackAnOrderAlsoRollsBackItsOutboxEvent() {
        AtomicReference<Long> orderId = new AtomicReference<>();
        TransactionTemplate transaction = new TransactionTemplate(transactionManager);

        assertThatThrownBy(() -> transaction.executeWithoutResult(status -> {
                    var order = orders.place(new PlaceOrderInput("rollback-" + UUID.randomUUID(), "SKU-TEA", 1));
                    orderId.set(order.id());
                    throw new IllegalStateException("force transaction rollback");
                }))
                .hasMessage("force transaction rollback");

        Integer savedOrder = jdbc.sql("SELECT count(*) FROM orders WHERE id = ?")
                .param(orderId.get())
                .query(Integer.class)
                .single();
        Integer savedEvent = jdbc.sql("SELECT count(*) FROM outbox_events WHERE aggregate_id = ?")
                .param(orderId.get())
                .query(Integer.class)
                .single();
        assertThat(savedOrder).isZero();
        assertThat(savedEvent).isZero();
    }

    /** A refused order sends no event; proven by waiting for a later valid order's event instead of sleeping. */
    @Test
    void anInvalidOrderPublishesNothing() {
        String customer = "customer-" + UUID.randomUUID();
        try (KafkaProbe probe = new KafkaProbe(kafka.getBootstrapServers(), Topics.ORDERS_PLACED)) {
            graphQl.document("""
                            mutation($c: String!) {
                              placeOrder(input: {customer: $c, sku: "SKU-TEA", quantity: 0}) { id }
                            }""")
                    .variable("c", customer)
                    .execute()
                    .errors().expect(e -> true).verify();

            // Proving that nothing arrives needs a bound. Instead of a fixed sleep: place a valid order afterwards and
            // wait for its event. An event for the invalid order would have been sent earlier by the same producer,
            // so it would almost certainly be visible by then. Across partitions that is a strong indication, not a
            // strict guarantee; the unit of proof is "nothing arrived before a later event did".
            String marker = "marker-" + UUID.randomUUID();
            graphQl.document("""
                            mutation($c: String!) {
                              placeOrder(input: {customer: $c, sku: "SKU-TEA", quantity: 1}) { id }
                            }""")
                    .variable("c", marker)
                    .execute()
                    .errors().verify();
            probe.awaitRecord(r -> r.value().contains(marker));
            assertThat(probe.seenValues()).noneMatch(value -> value.contains(customer));
        }
    }
}
