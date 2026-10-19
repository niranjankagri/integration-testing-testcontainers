package com.niranjankagri.orders.events;

import java.util.UUID;

/** Event the payment service publishes to payments.completed. */
public record PaymentCompleted(UUID eventId, Long orderId, Result result) {

    /** Whether the payment went through; FAILED cancels the order. */
    public enum Result {
        SUCCEEDED,
        FAILED
    }
}
