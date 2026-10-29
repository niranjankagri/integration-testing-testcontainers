package com.niranjankagri.orders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * The Flyway migrations, run against the same PostgreSQL version as production instead of an in-memory database
 * that only resembles it: an H2 run would not prove that the PostgreSQL syntax and constraints work.
 */
@IntegrationTest
class DatabaseMigrationTest {

    @Autowired
    JdbcClient jdbc;

    /** Flyway applied exactly the expected migrations, all successfully. */
    @Test
    void allMigrationsAreAppliedSuccessfully() {
        List<String> applied = jdbc.sql("SELECT version FROM flyway_schema_history WHERE success ORDER BY installed_rank")
                .query(String.class)
                .list();
        assertThat(applied).containsExactly("1", "2");
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

    /** Inserts an order with plain SQL, past the application's own validation, to reach the database constraints. */
    private void insertOrder(int quantity, String status) {
        jdbc.sql("INSERT INTO orders (customer, sku, quantity, status, created_at) VALUES ('db-test', 'SKU-1', ?, ?, now())")
                .params(quantity, status)
                .update();
    }
}
