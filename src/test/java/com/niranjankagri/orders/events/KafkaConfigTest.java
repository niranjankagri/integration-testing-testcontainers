package com.niranjankagri.orders.events;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DefaultErrorHandler;
import tools.jackson.core.JacksonException;

/**
 * Which failures are retried. The container tests show that a bad event ends up in the dead-letter topic, but they
 * pass whether or not it was retried first (it only arrives a second later). A mutation check found that gap; this
 * unit test closes it, without starting any container.
 */
class KafkaConfigTest {

    @SuppressWarnings("unchecked")
    private final DefaultErrorHandler handler = (DefaultErrorHandler) new KafkaConfig()
            .errorHandler(mock(KafkaOperations.class), mock(MeterRegistry.class));

    // removeClassification returns the registered classification: false = "not retryable", null = not registered.

    /** Broken JSON is classified as not retryable. */
    @Test
    void brokenJsonIsNotRetried() {
        assertThat(handler.removeClassification(JacksonException.class)).isFalse();
    }

    /** An invalid event is classified as not retryable. */
    @Test
    void anInvalidEventIsNotRetried() {
        assertThat(handler.removeClassification(PaymentListener.InvalidEventException.class)).isFalse();
    }

    /** An unknown order is not classified, so the default applies: retry. */
    @Test
    void anUnknownOrderIsRetried() {
        // Not registered: the default applies, which is "retry", because the order may simply not be committed yet.
        assertThat(handler.removeClassification(PaymentListener.UnknownOrderException.class)).isNull();
    }
}
