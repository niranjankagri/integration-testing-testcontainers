package com.niranjankagri.orders.order;

/** PLACED when ordered; PAID or CANCELLED when the payment service reports the result. */
public enum OrderStatus {
    PLACED,
    PAID,
    CANCELLED
}
