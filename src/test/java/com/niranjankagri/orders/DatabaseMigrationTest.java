package com.niranjankagri.orders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.niranjankagri.orders.events.OutboxEvent;
import com.niranjankagri.orders.events.OutboxEventStore;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The Flyway migrations, run against the same PostgreSQL version as production instead of an in-memory database
 * that only resembles it: an H2 run would not prove that the PostgreSQL syntax and constraints work.
 */
@IntegrationTest
class DatabaseMigrationTest {

    @Autowired
    JdbcClient jdbc;

    @Autowired
    OutboxEventStore outbox;

    @Autowired
    PlatformTransactionManager transactionManager;

    /** Flyway applied exactly the expected migrations, all successfully. */
    @Test
    void allMigrationsAreAppliedSuccessfully() {
        List<String> applied = jdbc.sql("SELECT version FROM flyway_schema_history WHERE success ORDER BY installed_rank")
                .query(String.class)
                .list();
        assertThat(applied).containsExactly("1", "2", "3", "4");
    }

    /** The database itself refuses a quantity of 0 or 11 (check constraint). */
    @Test
    void databaseRejectsAQuantityOutsideOneToTen() {
        // The API validates this too; the constraint protects the data from every other writer (scripts, other code).
        assertThatThrownBy(() -> insertOrder(0, "PLACED")).isInstanceOf(DataIntegrityViolationException.class);
        assertThatThrownBy(() -> insertOrder(11, "PLACED")).isInstanceOf(DataIntegrityViolationException.class);
    }

    /** The database refuses a status that is not one of the known ones. */
    @Test
    void databaseRejectsAnUnknownStatus() {
        assertThatThrownBy(() -> insertOrder(1, "SHIPPED")).isInstanceOf(DataIntegrityViolationException.class);
    }

    /** A second insert of the same event id changes nothing: the base of the duplicate-event protection. */
    @Test
    void processedEventIdsAreUnique() {
        String id = "6f1c2c1e-0000-4000-8000-000000000001";
        jdbc.sql("INSERT INTO processed_events (event_id) VALUES (?::uuid) ON CONFLICT DO NOTHING").param(id).update();
        int second = jdbc.sql("INSERT INTO processed_events (event_id) VALUES (?::uuid) ON CONFLICT DO NOTHING")
                .param(id)
                .update();
        assertThat(second).isZero();
    }

    /** Competing claims stay disjoint, preserve per-order order, and reject a stale lease owner. */
    @Test
    void outboxClaimsAreExclusiveRecoverableAndOrderedPerAggregate() {
        UUID firstId = UUID.fromString("00000000-0000-4000-8000-000000000101");
        UUID secondId = UUID.fromString("00000000-0000-4000-8000-000000000102");
        UUID otherAggregateId = UUID.fromString("00000000-0000-4000-8000-000000000103");
        UUID firstClaim = UUID.randomUUID();
        UUID secondClaim = UUID.randomUUID();
        UUID reclaim = UUID.randomUUID();
        UUID nextClaim = UUID.randomUUID();
        TransactionTemplate transaction = new TransactionTemplate(transactionManager);

        transaction.executeWithoutResult(status -> {
            insertOutboxEvent(firstId, 700001, "first");
            insertOutboxEvent(secondId, 700001, "second");
            insertOutboxEvent(otherAggregateId, 700002, "other");

            List<OutboxEvent> firstBatch = outbox.claimPending(10, firstClaim, 30);
            List<OutboxEvent> secondBatch = outbox.claimPending(10, secondClaim, 30);
            assertThat(firstBatch).hasSize(2);
            assertThat(secondBatch).isEmpty();
            assertThat(firstBatch).extracting(OutboxEvent::eventId)
                    .containsExactlyInAnyOrder(firstId, otherAggregateId);

            // Make one lease stale, then reclaim it. Its former owner can no longer mark it as published.
            jdbc.sql("UPDATE outbox_events SET claimed_until = now() - INTERVAL '1 second' WHERE event_id = ?")
                    .param(firstId)
                    .update();
            List<OutboxEvent> reclaimed = outbox.claimPending(1, reclaim, 30);
            assertThat(reclaimed).extracting(OutboxEvent::eventId).containsExactly(firstId);
            assertThat(outbox.markPublished(firstId, firstClaim)).isFalse();
            assertThat(outbox.markPublished(firstId, reclaim)).isTrue();

            // Once the head event is published, the next event for that order becomes claimable.
            List<OutboxEvent> next = outbox.claimPending(1, nextClaim, 30);
            assertThat(next).extracting(OutboxEvent::eventId).containsExactly(secondId);

            outbox.releaseClaim(firstClaim);
            outbox.releaseClaim(nextClaim);
            jdbc.sql("DELETE FROM outbox_events WHERE event_id IN (?, ?, ?)")
                    .params(firstId, secondId, otherAggregateId)
                    .update();
        });
    }

    /** Retention removes only old published rows and leaves old pending events available for delivery. */
    @Test
    void outboxRetentionDeletesOnlyPublishedRowsPastTheConfiguredAge() {
        UUID expiredPublished = UUID.fromString("00000000-0000-4000-8000-000000000104");
        UUID recentPublished = UUID.fromString("00000000-0000-4000-8000-000000000105");
        UUID oldPending = UUID.fromString("00000000-0000-4000-8000-000000000106");
        TransactionTemplate transaction = new TransactionTemplate(transactionManager);

        transaction.executeWithoutResult(status -> {
            insertOutboxEvent(expiredPublished, 700004, "expired-published");
            insertOutboxEvent(recentPublished, 700005, "recent-published");
            insertOutboxEvent(oldPending, 700006, "old-pending");
            jdbc.sql("UPDATE outbox_events SET published_at = now() - INTERVAL '31 days' WHERE event_id = ?")
                    .param(expiredPublished)
                    .update();
            jdbc.sql("UPDATE outbox_events SET published_at = now() - INTERVAL '10 days' WHERE event_id = ?")
                    .param(recentPublished)
                    .update();
            jdbc.sql("UPDATE outbox_events SET created_at = now() - INTERVAL '60 days' WHERE event_id = ?")
                    .param(oldPending)
                    .update();

            int deleted = outbox.deletePublishedBefore(30, 10);
            assertThat(deleted).isEqualTo(1);
            assertThat(outboxEventCount(expiredPublished)).isZero();
            assertThat(outboxEventCount(recentPublished)).isEqualTo(1);
            assertThat(outboxEventCount(oldPending)).isEqualTo(1);

            jdbc.sql("DELETE FROM outbox_events WHERE event_id IN (?, ?, ?)")
                    .params(expiredPublished, recentPublished, oldPending)
                    .update();
        });
    }

    /** Inserts an order with plain SQL, past the application's own validation, to reach the database constraints. */
    private void insertOrder(int quantity, String status) {
        jdbc.sql("INSERT INTO orders (customer, sku, quantity, status, created_at) VALUES ('db-test', 'SKU-1', ?, ?, now())")
                .params(quantity, status)
                .update();
    }

    /** Inserts a committed-shape outbox fixture inside the current test transaction. */
    private void insertOutboxEvent(UUID eventId, long aggregateId, String payload) {
        jdbc.sql("""
                        INSERT INTO outbox_events (event_id, aggregate_id, topic, payload, created_at)
                        VALUES (?, ?, 'orders.placed', ?, now())
                        """)
                .params(eventId, aggregateId, payload)
                .update();
    }

    /** Counts a single outbox fixture without depending on any application-level repository behavior. */
    private int outboxEventCount(UUID eventId) {
        return jdbc.sql("SELECT count(*) FROM outbox_events WHERE event_id = ?")
                .param(eventId)
                .query(Integer.class)
                .single();
    }
}
