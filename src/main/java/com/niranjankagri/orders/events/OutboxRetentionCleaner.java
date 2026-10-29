package com.niranjankagri.orders.events;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Removes old published outbox rows in bounded, concurrency-safe batches. */
@Component
public class OutboxRetentionCleaner {

    private static final Logger log = LoggerFactory.getLogger(OutboxRetentionCleaner.class);
    private static final int BATCH_SIZE = 500;

    private final OutboxEventStore outbox;
    private final OutboxMetrics metrics;
    private final int retentionDays;

    public OutboxRetentionCleaner(
            OutboxEventStore outbox,
            OutboxMetrics metrics,
            @Value("${orders.outbox.retention-days:30}") int retentionDays) {
        this.outbox = outbox;
        this.metrics = metrics;
        this.retentionDays = retentionDays;
    }

    /** Leaves pending events untouched and records how many published rows were removed. */
    @Scheduled(fixedDelayString = "${orders.outbox.cleanup-interval-ms:60000}")
    void removeExpiredPublishedEvents() {
        try {
            int deleted = outbox.deletePublishedBefore(retentionDays, BATCH_SIZE);
            if (deleted > 0) {
                metrics.cleaned(deleted);
                log.info("Removed {} published outbox events older than {} days", deleted, retentionDays);
            }
        } catch (Exception failure) {
            log.warn("Could not clean expired published outbox events; the next run will retry", failure);
        }
    }
}
