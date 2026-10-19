package com.niranjankagri.orders.events;

import com.niranjankagri.orders.order.Order;
import com.niranjankagri.orders.order.OrderRepository;
import com.niranjankagri.orders.order.OrderStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

/**
 * Applies payment results to orders. Kafka delivers at least once, so the same event can arrive twice: each event id
 * is recorded in processed_events in the same transaction as the order change, and a known id is skipped.
 */
@Component
public class PaymentListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentListener.class);

    private final OrderRepository orders;
    private final JdbcClient jdbc;
    private final JsonMapper json;

    /** Spring injects the repository, the JDBC client for the processed-event table and the JSON mapper. */
    public PaymentListener(OrderRepository orders, JdbcClient jdbc, JsonMapper json) {
        this.orders = orders;
        this.jdbc = jdbc;
        this.json = json;
    }

    /**
     * Handles one payment event in one transaction: parse and validate it, skip it if its id was seen before, then
     * mark the order PAID or CANCELLED. Only an order that is still PLACED changes; a later event for a finished
     * order is logged and ignored.
     */
    @KafkaListener(topics = Topics.PAYMENTS_COMPLETED)
    @Transactional
    public void on(String message) {
        PaymentCompleted event = json.readValue(message, PaymentCompleted.class); // broken JSON: not retried, see KafkaConfig
        if (event.eventId() == null || event.orderId() == null || event.result() == null) {
            throw new InvalidEventException("eventId, orderId and result are required: " + message);
        }
        if (!markProcessed(event)) {
            log.info("Skipping event {}: already processed", event.eventId());
            return;
        }
        // An unknown order is retried: the event may have overtaken the order's own transaction. If it is still
        // unknown after the retries, the event goes to the dead-letter topic.
        Order order = orders.findById(event.orderId())
                .orElseThrow(() -> new UnknownOrderException(event.orderId()));
        if (order.status() != OrderStatus.PLACED) {
            log.warn("Order {} is already {}; payment event {} ignored", order.id(), order.status(), event.eventId());
            return;
        }
        OrderStatus next = event.result() == PaymentCompleted.Result.SUCCEEDED ? OrderStatus.PAID : OrderStatus.CANCELLED;
        orders.save(order.withStatus(next));
    }

    /** Records the event id; false if it was recorded before. Rolled back with the transaction if anything fails. */
    private boolean markProcessed(PaymentCompleted event) {
        return jdbc.sql("INSERT INTO processed_events (event_id) VALUES (?) ON CONFLICT DO NOTHING")
                        .param(event.eventId())
                        .update()
                == 1;
    }

    /** An event that can never be processed, however often it is retried. */
    public static class InvalidEventException extends RuntimeException {
        /** The message names the missing fields and quotes the event. */
        public InvalidEventException(String message) {
            super(message);
        }
    }

    /**
     * The event names an order this service does not know (yet). Retried, because the order's own transaction may not
     * have committed when the event arrived.
     */
    public static class UnknownOrderException extends RuntimeException {
        /** The message names the order id; the dead-letter record's exception header carries it. */
        public UnknownOrderException(Long orderId) {
            super("No order " + orderId);
        }
    }
}
