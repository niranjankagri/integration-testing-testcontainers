package com.niranjankagri.orders.events;

import java.util.concurrent.TimeUnit;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Publishes committed outbox rows and marks them only after Kafka acknowledges each send. */
@Component
public class OrderOutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OrderOutboxRelay.class);
    private static final int BATCH_SIZE = 50;
    private static final long SEND_TIMEOUT_SECONDS = 10;
    private static final int CLAIM_LEASE_SECONDS = 30;

    private final OutboxEventStore outbox;
    private final KafkaTemplate<String, String> kafka;
    private final OutboxMetrics metrics;

    public OrderOutboxRelay(OutboxEventStore outbox, KafkaTemplate<String, String> kafka, OutboxMetrics metrics) {
        this.outbox = outbox;
        this.kafka = kafka;
        this.metrics = metrics;
    }

    /** Claims one aggregate event at a time and stops on failure to preserve per-order event order. */
    @Scheduled(fixedDelayString = "${orders.outbox.poll-interval-ms:500}")
    void publishPending() {
        for (int sent = 0; sent < BATCH_SIZE; sent++) {
            UUID claimToken = UUID.randomUUID();
            var claimed = outbox.claimPending(1, claimToken, CLAIM_LEASE_SECONDS);
            if (claimed.isEmpty()) {
                return;
            }
            OutboxEvent event = claimed.getFirst();
            try {
                kafka.send(event.topic(), Long.toString(event.aggregateId()), event.payload())
                        .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (!outbox.markPublished(event.eventId(), claimToken)) {
                    log.warn("Outbox event {} was acknowledged after its lease was lost", event.eventId());
                    return;
                }
                metrics.published();
            } catch (InterruptedException interrupted) {
                releaseClaim(claimToken);
                Thread.currentThread().interrupt();
                return;
            } catch (Exception failure) {
                // Leave the row pending. A later poll retries with the same event id, so consumers can deduplicate
                // the rare case where Kafka accepted the message but the database update did not commit.
                log.warn("Outbox event {} was not marked published; it will be retried", event.eventId(), failure);
                metrics.publishFailed();
                releaseClaim(claimToken);
                return;
            }
        }
    }

    /** Best-effort release; an un-released claim becomes available when its lease expires. */
    private void releaseClaim(UUID claimToken) {
        try {
            outbox.releaseClaim(claimToken);
        } catch (Exception failure) {
            log.warn("Could not release outbox claim {}; its lease will expire", claimToken, failure);
        }
    }
}
