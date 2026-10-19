package com.niranjankagri.orders.order;

import java.time.Instant;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

/** An order, as stored in the "orders" table. Immutable: changes create a copy. */
@Table("orders")
public record Order(@Id Long id, String customer, String sku, int quantity, OrderStatus status, Instant createdAt) {

    /** A new order in status PLACED; the database assigns the id when it is saved. */
    public static Order place(String customer, String sku, int quantity, Instant now) {
        return new Order(null, customer, sku, quantity, OrderStatus.PLACED, now);
    }

    /** Used by Spring Data to set the id the database generated. */
    public Order withId(Long newId) {
        return new Order(newId, customer, sku, quantity, status, createdAt);
    }

    /** A copy in the new status. */
    public Order withStatus(OrderStatus newStatus) {
        return new Order(id, customer, sku, quantity, newStatus, createdAt);
    }
}
