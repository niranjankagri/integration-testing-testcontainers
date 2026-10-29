package com.niranjankagri.orders.events;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.util.backoff.FixedBackOff;
import tools.jackson.core.JacksonException;

/**
 * The topics this service uses and how a failing payment event is handled: retried, or sent to the dead-letter topic.
 */
@Configuration
public class KafkaConfig {

    /** Number of times a failing event is tried again before it goes to the dead-letter topic. */
    static final int RETRIES = 2;

    @Bean
    NewTopic ordersPlaced() {
        return TopicBuilder.name(Topics.ORDERS_PLACED).partitions(3).build();
    }

    @Bean
    NewTopic paymentsCompleted() {
        return TopicBuilder.name(Topics.PAYMENTS_COMPLETED).partitions(3).build();
    }

    /** Same partition count as the source topic: the recoverer keeps each record's partition. */
    @Bean
    NewTopic paymentsCompletedDlt() {
        return TopicBuilder.name(Topics.PAYMENTS_COMPLETED_DLT).partitions(3).build();
    }

    /**
     * A failing record is retried a few times, then published to the dead-letter topic and skipped, so one bad
     * message cannot block the partition ("poison pill"). Broken JSON and invalid events go there at once: retrying
     * cannot fix them.
     */
    @Bean
    CommonErrorHandler errorHandler(KafkaOperations<Object, Object> kafka, MeterRegistry metrics) {
        // The dead-letter topic is named explicitly: the framework's default name changed between Spring Kafka
        // versions (".DLT" before 4.0, "-dlt" since), and a silent rename would send failures where nobody looks.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafka, (record, ex) -> new TopicPartition(Topics.PAYMENTS_COMPLETED_DLT, record.partition()));
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(500, RETRIES));
        handler.addNotRetryableExceptions(JacksonException.class, PaymentListener.InvalidEventException.class);
        handler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception exception, int deliveryAttempt) {
                count(metrics, "delivery_failed");
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> record, Exception exception) {
                count(metrics, "dead_lettered");
            }

            @Override
            public void recoveryFailed(ConsumerRecord<?, ?> record, Exception original, Exception failure) {
                count(metrics, "recovery_failed");
            }
        });
        return handler;
    }

    /** Uses bounded labels so event data cannot create unbounded Prometheus time series. */
    private static void count(MeterRegistry metrics, String outcome) {
        metrics.counter("orders.payment.events", "outcome", outcome).increment();
    }
}
