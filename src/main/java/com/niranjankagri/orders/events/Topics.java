package com.niranjankagri.orders.events;

/** Kafka topic names. */
public final class Topics {

    /** Published by this service when an order is placed. Key: the order id. */
    public static final String ORDERS_PLACED = "orders.placed";
    /** Published by the payment service; consumed here. */
    public static final String PAYMENTS_COMPLETED = "payments.completed";
    /** Payment events that could not be processed, for a person to look at. */
    public static final String PAYMENTS_COMPLETED_DLT = "payments.completed.DLT";

    /** Constants only: no instances. */
    private Topics() {}
}
