package com.niranjankagri.orders.order;

import com.niranjankagri.orders.events.OrderPlaced;
import java.time.Clock;
import java.util.UUID;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {

    private final OrderRepository orders;
    private final ApplicationEventPublisher events;
    private final Clock clock;

    public OrderService(OrderRepository orders, ApplicationEventPublisher events, Clock clock) {
        this.orders = orders;
        this.events = events;
        this.clock = clock;
    }

    /**
     * Stores the order and announces it. The Kafka message is sent only after the transaction commits
     * (OrderEventPublisher), so no event is ever published for an order that was rolled back.
     */
    @Transactional
    public Order place(PlaceOrderInput input) {
        Order saved = orders.save(Order.place(input.customer(), input.sku(), input.quantity(), clock.instant()));
        events.publishEvent(new OrderPlaced(UUID.randomUUID(), saved.id(), saved.customer(), saved.sku(), saved.quantity()));
        return saved;
    }
}
