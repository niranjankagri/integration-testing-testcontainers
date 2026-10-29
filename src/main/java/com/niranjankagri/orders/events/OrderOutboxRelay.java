package com.niranjankagri.orders.events;

import java.util.concurrent.TimeUnit;
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

    private final OutboxEventStore outbox;
    private final KafkaTemplate<String, String> kafka;
    private final OutboxMetrics metrics;

    public OrderOutboxRelay(OutboxEventStore outbox, KafkaTemplate<String, String> kafka, OutboxMetrics metrics) {
        this.outbox = outbox;
        this.kafka = kafka;
        this.metrics = metrics;
    }

    /** Stops at the first failure so later order events do not overtake the event being retried. */
    @Scheduled(fixedDelayString = "${orders.outbox.poll-interval-ms:500}")
    void publishPending() {
        for (OutboxEvent event : outbox.findPending(BATCH_SIZE)) {
            try {
                kafka.send(event.topic(), Long.toString(event.aggregateId()), event.payload())
                        .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                outbox.markPublished(event.eventId());
                metrics.published();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception failure) {
                // Leave the row pending. A later poll retries with the same event id, so consumers can deduplicate
                // the rare case where Kafka accepted the message but the database update did not commit.
                log.warn("Outbox event {} was not marked published; it will be retried", event.eventId(), failure);
                metrics.publishFailed();
                return;
            }
        }
    }
}
