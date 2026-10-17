package com.niranjankagri.orders.events;

import java.util.UUID;

/** Event the payment service publishes to payments.completed. */
public record PaymentCompleted(UUID eventId, Long orderId, Result result) {

    public enum Result {
        SUCCEEDED,
        FAILED
    }
}
