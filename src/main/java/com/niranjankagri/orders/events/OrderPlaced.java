package com.niranjankagri.orders.events;

import java.util.UUID;

/** Event published to orders.placed. eventId lets consumers recognise a redelivery. */
public record OrderPlaced(UUID eventId, Long orderId, String customer, String sku, int quantity) {}
