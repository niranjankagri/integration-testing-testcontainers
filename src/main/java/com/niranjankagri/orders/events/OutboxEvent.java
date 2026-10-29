package com.niranjankagri.orders.events;

import java.util.UUID;

/** One event persisted with its order and waiting for broker acknowledgement. */
public record OutboxEvent(UUID eventId, long aggregateId, String topic, String payload) {}
