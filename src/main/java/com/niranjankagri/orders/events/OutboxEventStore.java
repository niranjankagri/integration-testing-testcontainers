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

    /** Claims available events without blocking other relay instances; only an aggregate's oldest event is eligible. */
    public List<OutboxEvent> claimPending(int limit, UUID claimToken, int leaseSeconds) {
        return jdbc.sql("""
                        WITH candidates AS (
                            SELECT event.event_id
                            FROM outbox_events event
                            WHERE event.published_at IS NULL
                              AND (event.claimed_until IS NULL OR event.claimed_until <= now())
                              AND NOT EXISTS (
                                  SELECT 1
                                  FROM outbox_events earlier
                                  WHERE earlier.aggregate_id = event.aggregate_id
                                    AND earlier.published_at IS NULL
                                    AND (earlier.created_at, earlier.event_id) < (event.created_at, event.event_id)
                              )
                            ORDER BY event.created_at, event.event_id
                            LIMIT :limit
                            FOR UPDATE OF event SKIP LOCKED
                        )
                        UPDATE outbox_events claimed
                        SET claim_token = :claimToken,
                            claimed_until = now() + (:leaseSeconds * INTERVAL '1 second')
                        FROM candidates
                        WHERE claimed.event_id = candidates.event_id
                        RETURNING claimed.event_id, claimed.aggregate_id, claimed.topic, claimed.payload
                        """)
                .param("limit", limit)
                .param("claimToken", claimToken)
                .param("leaseSeconds", leaseSeconds)
                .query((rs, row) -> new OutboxEvent(
                        (UUID) rs.getObject("event_id"),
                        rs.getLong("aggregate_id"),
                        rs.getString("topic"),
                        rs.getString("payload")))
                .list();
    }

    /** Counts committed events still waiting for the relay. */
    public double countPending() {
        return jdbc.sql("SELECT count(*) FROM outbox_events WHERE published_at IS NULL")
                .query(Long.class)
                .single();
    }

    /** Returns the age of the oldest pending event, or zero when the outbox is clear. */
    public double oldestPendingAgeSeconds() {
        return jdbc.sql("""
                        SELECT COALESCE(EXTRACT(EPOCH FROM now() - MIN(created_at)), 0)
                        FROM outbox_events
                        WHERE published_at IS NULL
                        """)
                .query(Double.class)
                .single();
    }

    /** Marks a claimed event only after Kafka acknowledges the publish. */
    public boolean markPublished(UUID eventId, UUID claimToken) {
        return jdbc.sql("""
                        UPDATE outbox_events
                        SET published_at = now(), claim_token = NULL, claimed_until = NULL
                        WHERE event_id = :eventId AND claim_token = :claimToken AND published_at IS NULL
                        """)
                .param("eventId", eventId)
                .param("claimToken", claimToken)
                .update() == 1;
    }

    /** Releases pending rows owned by a relay attempt so failures can be retried promptly. */
    public void releaseClaim(UUID claimToken) {
        jdbc.sql("""
                        UPDATE outbox_events
                        SET claim_token = NULL, claimed_until = NULL
                        WHERE claim_token = :claimToken AND published_at IS NULL
                        """)
                .param("claimToken", claimToken)
                .update();
    }
}
