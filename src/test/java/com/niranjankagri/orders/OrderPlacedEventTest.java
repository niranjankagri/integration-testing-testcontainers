package com.niranjankagri.orders;

import static org.assertj.core.api.Assertions.assertThat;

import com.niranjankagri.orders.events.Topics;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.graphql.test.tester.HttpGraphQlTester;
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
            assertThat(UUID.fromString(event.get("eventId").asText())).isNotNull();
        }
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
