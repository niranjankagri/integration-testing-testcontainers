package com.niranjankagri.orders.events;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionalEventListener;
import tools.jackson.databind.json.JsonMapper;

/**
 * Sends OrderPlaced to Kafka once the order's database transaction has committed. Sending inside the transaction
 * could announce an order that then rolls back; sending after it can, in a crash, lose the event (a real system would
 * use a transactional outbox, see docs/framework-design.md).
 */
@Component
public class OrderEventPublisher {

    private final KafkaTemplate<String, String> kafka;
    private final JsonMapper json;

    /** Spring injects the Kafka template and the JSON mapper. */
    public OrderEventPublisher(KafkaTemplate<String, String> kafka, JsonMapper json) {
        this.kafka = kafka;
        this.json = json;
    }

    /** Called by Spring after the placing transaction committed; sends the event as JSON to orders.placed. */
    @TransactionalEventListener
    public void on(OrderPlaced event) {
        // Keyed by order id: all events of one order go to the same partition, so they stay in order.
        kafka.send(Topics.ORDERS_PLACED, String.valueOf(event.orderId()), json.writeValueAsString(event));
    }
}
