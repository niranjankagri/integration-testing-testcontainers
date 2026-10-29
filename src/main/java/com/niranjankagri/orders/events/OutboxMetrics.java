package com.niranjankagri.orders.events;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/** Metrics for pending outbox work and relay outcomes. */
@Component
public class OutboxMetrics {

    private final OutboxEventStore outbox;
    private final Counter published;
    private final Counter publishFailures;
    private final Counter cleaned;

    public OutboxMetrics(OutboxEventStore outbox, MeterRegistry registry) {
        this.outbox = outbox;
        this.published = Counter.builder("orders.outbox.published")
                .description("Outbox events acknowledged by Kafka and marked published")
                .register(registry);
        this.publishFailures = Counter.builder("orders.outbox.publish.failures")
                .description("Outbox relay attempts that failed before marking an event published")
                .register(registry);
        this.cleaned = Counter.builder("orders.outbox.cleaned")
                .description("Published outbox rows removed by retention cleanup")
                .register(registry);
        registry.gauge("orders.outbox.pending", outbox, OutboxEventStore::countPending);
        registry.gauge("orders.outbox.oldest.pending.seconds", outbox, OutboxEventStore::oldestPendingAgeSeconds);
    }

    public void published() {
        published.increment();
    }

    public void publishFailed() {
        publishFailures.increment();
    }

    public void cleaned(int count) {
        cleaned.increment(count);
    }
}
