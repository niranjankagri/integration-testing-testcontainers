package com.niranjankagri.orders.events;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
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
    CommonErrorHandler errorHandler(KafkaOperations<Object, Object> kafka) {
        // The dead-letter topic is named explicitly: the framework's default name changed between Spring Kafka
        // versions (".DLT" before 4.0, "-dlt" since), and a silent rename would send failures where nobody looks.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafka, (record, ex) -> new TopicPartition(Topics.PAYMENTS_COMPLETED_DLT, record.partition()));
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(500, RETRIES));
        handler.addNotRetryableExceptions(JacksonException.class, PaymentListener.InvalidEventException.class);
        return handler;
    }
}
