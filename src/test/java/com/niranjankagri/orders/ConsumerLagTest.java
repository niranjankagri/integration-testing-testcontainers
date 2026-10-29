package com.niranjankagri.orders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.niranjankagri.orders.events.ConsumerLagMetrics;
import com.niranjankagri.orders.events.PaymentCompleted;
import com.niranjankagri.orders.events.PaymentCompleted.Result;
import com.niranjankagri.orders.events.Topics;
import com.niranjankagri.orders.order.Order;
import com.niranjankagri.orders.order.OrderRepository;
import com.niranjankagri.orders.order.OrderService;
import com.niranjankagri.orders.order.OrderStatus;
import com.niranjankagri.orders.order.PlaceOrderInput;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import tools.jackson.databind.json.JsonMapper;

/**
 * The consumer-lag gauge counts what the payment consumer group has not committed: it rises while the listener is
 * stopped and falls back to zero once the backlog is processed. The client's own lag metric would not show a stopped
 * listener, because it is only measured while the consumer fetches.
 */
@IntegrationTest
class ConsumerLagTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    @Autowired
    ConsumerLagMetrics lag;

    @Autowired
    KafkaListenerEndpointRegistry listeners;

    @Autowired
    KafkaTemplate<String, String> kafka;

    @Autowired
    OrderService service;

    @Autowired
    OrderRepository orders;

    @Autowired
    JsonMapper json;

    /** Pausing stands in for a stuck consumer: three payments wait, the gauge says 3, then 0 after resuming. */
    @Test
    void lagCountsUncommittedPaymentsWhileTheListenerIsStoppedAndClearsAfterwards() throws Exception {
        await().atMost(TIMEOUT).until(() -> lag.paymentLag() == 0); // nothing left over from other tests
        List<Order> placed = IntStream.range(0, 3)
                .mapToObj(i -> service.place(new PlaceOrderInput("lag-" + UUID.randomUUID(), "SKU-1", 1)))
                .toList();

        MessageListenerContainer container = listeners.getListenerContainers().iterator().next();
        container.pause();
        try {
            await().atMost(TIMEOUT).until(container::isContainerPaused);
            for (Order order : placed) {
                PaymentCompleted payment = new PaymentCompleted(UUID.randomUUID(), order.id(), Result.SUCCEEDED);
                kafka.send(Topics.PAYMENTS_COMPLETED, String.valueOf(order.id()), json.writeValueAsString(payment))
                        .get();
            }

            await().atMost(TIMEOUT).until(() -> lag.paymentLag() == 3);
            assertThat(placed).allSatisfy(order ->
                    assertThat(orders.findById(order.id()).orElseThrow().status()).isEqualTo(OrderStatus.PLACED));
        } finally {
            container.resume();
        }

        await().atMost(TIMEOUT).until(() -> lag.paymentLag() == 0);
        assertThat(placed).allSatisfy(order ->
                assertThat(orders.findById(order.id()).orElseThrow().status()).isEqualTo(OrderStatus.PAID));
    }
}
