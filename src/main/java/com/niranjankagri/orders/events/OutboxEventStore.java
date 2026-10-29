package com.niranjankagri.orders.events;

import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.json.JsonMapper;

/** Persists outgoing events in PostgreSQL so they commit or roll back with the order. */
@Repository
public class OutboxEventStore {

    private final JdbcClient jdbc;
    private final JsonMapper json;

    public OutboxEventStore(JdbcClient jdbc, JsonMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    /** Adds a serialized event to the current database transaction. */
    public void append(OrderPlaced event) {
        jdbc.sql("""
                        INSERT INTO outbox_events (event_id, aggregate_id, topic, payload)
                        VALUES (:eventId, :aggregateId, :topic, :payload)
                        """)
                .param("eventId", event.eventId())
                .param("aggregateId", event.orderId())
                .param("topic", Topics.ORDERS_PLACED)
                .param("payload", json.writeValueAsString(event))
                .update();
    }

    /** Returns a bounded batch in insertion order for the relay. */
    public List<OutboxEvent> findPending(int limit) {
        return jdbc.sql("""
                        SELECT event_id, aggregate_id, topic, payload
                        FROM outbox_events
                        WHERE published_at IS NULL
                        ORDER BY created_at, event_id
                        LIMIT :limit
                        """)
                .param("limit", limit)
                .query((rs, row) -> new OutboxEvent(
                        (UUID) rs.getObject("event_id"),
                        rs.getLong("aggregate_id"),
                        rs.getString("topic"),
                        rs.getString("payload")))
                .list();
    }

    /** Marks an event only after Kafka acknowledges the publish. */
    public void markPublished(UUID eventId) {
        jdbc.sql("UPDATE outbox_events SET published_at = now() WHERE event_id = :eventId AND published_at IS NULL")
                .param("eventId", eventId)
                .update();
    }
}
