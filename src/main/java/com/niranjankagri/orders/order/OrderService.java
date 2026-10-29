package com.niranjankagri.orders.order;

import com.niranjankagri.orders.events.OrderPlaced;
import com.niranjankagri.orders.events.OutboxEventStore;
import java.time.Clock;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Places orders and records their outgoing events in the same transaction. */
@Service
public class OrderService {

    private final OrderRepository orders;
    private final OutboxEventStore outbox;
    private final Clock clock;

    public OrderService(OrderRepository orders, OutboxEventStore outbox, Clock clock) {
        this.orders = orders;
        this.outbox = outbox;
        this.clock = clock;
    }

    /**
     * Stores the order and its event atomically. The relay only sees committed rows, and a rollback removes both.
     */
    @Transactional
    public Order place(PlaceOrderInput input) {
        Order saved = orders.save(Order.place(input.customer(), input.sku(), input.quantity(), clock.instant()));
        outbox.append(new OrderPlaced(
                UUID.randomUUID(), saved.id(), saved.customer(), saved.sku(), saved.quantity()));
        return saved;
    }
}
